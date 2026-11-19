import sys
import json
import math
from collections.abc import Iterator, Mapping
from functools import cache
from typing import Any

from django.conf import settings

from drf_spectacular.utils import OpenApiResponse
from rest_framework import serializers, status
from rest_framework.exceptions import APIException, ParseError, UnsupportedMediaType
from rest_framework.parsers import BaseParser, JSONParser

from posthog.api.fields import LengthFirstListField
from posthog.api.mixins import ResponseDeclaration
from posthog.enums import LabeledStrEnum

from products.messaging.backend.facade.transactional_email import (
    IDEMPOTENCY_KEY_PATTERN,
    BatchRecipient,
    DeliveryUnavailable,
    EmailBatchSent,
    EmailNotQueued,
    InvalidRecipient,
    QueuedEmail,
)
from products.messaging.backend.presentation.views.code_send_fields import (
    CODE_SEND_EXTRA_FIELDS,
    validate_pipeline_address,
)
from products.messaging.backend.presentation.views.message_templates import (
    DISTINCT_ID_HELP_TEXT,
    SendEmailSerializer,
    SendWarningSerializer,
    TransactionalEmailRejected,
)
from products.messaging.backend.presentation.views.refusals import ExplainedRefusal

BATCH_MAX_RECIPIENTS = 100
BATCH_BODY_MAX_BYTES = 5 * 1024 * 1024
BATCH_BODY_MAX_DEPTH = 100
SHOWN_FIELD_NAME_MAX_LENGTH = 64
_NO_CHILD = object()
BATCH_REFUSED_FIELDS = ("scheduled_at", *CODE_SEND_EXTRA_FIELDS)
FIELDS_THAT_FAIL_ONE_RECIPIENT = frozenset({"to", "distinct_id"})
RECIPIENT_FIELDS_OUTSIDE_RECIPIENTS = {"to": "to", "distinct_id": "distinct_id", "identifiers": "distinct_id"}
RECIPIENT_FIELDS = frozenset({"to", "distinct_id", "message_data", "idempotency_key"})
UNCONFIRMED_EMAIL = (
    "The email pipeline did not confirm this email, so it may still go out. Send it again with the same "
    "`idempotency_key` within 24 hours of the first request that used it, so it goes out once."
)


class UnreadBody(dict[str, Any]):
    def __init__(self, refusal: APIException) -> None:
        super().__init__()
        self.refusal = refusal


class BatchJSONParser(BaseParser):
    media_type = "*/*"

    def parse(self, stream: Any, media_type: str | None = None, parser_context: Mapping[str, Any] | None = None) -> Any:
        if (media_type or "").split(";")[0].strip().lower() != JSONParser.media_type:
            return UnreadBody(UnsupportedMediaType(media_type or ""))
        context = parser_context or {}
        body = context["request"].body if "request" in context else stream.read()
        if len(body) > BATCH_BODY_MAX_BYTES:
            return UnreadBody(batch_body_too_large())
        try:
            value = json.loads(
                body,
                object_pairs_hook=_members,
                parse_constant=_refused_constant,
                parse_float=_finite,
                parse_int=_finite_int,
            )
        except ValueError as e:
            return UnreadBody(ParseError(f"JSON parse error - {e}"))
        except RecursionError:
            return UnreadBody(_too_deep())
        if _nests_too_deeply(value):
            return UnreadBody(_too_deep())
        try:
            json.dumps(value, ensure_ascii=False).encode()
        except UnicodeEncodeError:
            return UnreadBody(
                ParseError(
                    "The body holds text that isn't valid Unicode, such as a lone surrogate. Fix the text, then send "
                    "the batch again."
                )
            )
        return value


def _too_deep() -> ParseError:
    return ParseError(f"JSON parse error - the body nests deeper than {BATCH_BODY_MAX_DEPTH} levels.")


def _nests_too_deeply(value: Any) -> bool:
    if not isinstance(value, dict | list):
        return False
    open_containers = [_children(value)]
    while open_containers:
        child = next(open_containers[-1], _NO_CHILD)
        if child is _NO_CHILD:
            open_containers.pop()
        elif isinstance(child, dict | list):
            if len(open_containers) >= BATCH_BODY_MAX_DEPTH:
                return True
            open_containers.append(_children(child))
    return False


def _children(container: dict[str, Any] | list[Any]) -> Iterator[Any]:
    return iter(container.values() if isinstance(container, dict) else container)


def _members(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    members = dict(pairs)
    if len(members) != len(pairs):
        raise ValueError("an object names the same member twice")
    return members


def _refused_constant(text: str) -> float:
    raise ValueError(f"{text} isn't a JSON value")


def _finite(text: str) -> float:
    if not math.isfinite(value := float(text)):
        raise ValueError("a number is out of range")
    return value


def _finite_int(text: str) -> int:
    if abs(value := int(text)) > sys.float_info.max:
        raise ValueError("a number is out of range")
    return value


class TextOnlyField(serializers.CharField):
    def to_internal_value(self, data: Any) -> str:
        if not isinstance(data, str):
            self.fail("invalid")
        return super().to_internal_value(data)


class EmailBatchRecipientSerializer(serializers.Serializer):
    to = serializers.EmailField(
        validators=[validate_pipeline_address],
        help_text="Email address of this recipient. An address PostHog can't send to fails only this recipient's "
        "result, with `invalid_recipient`.",
    )
    distinct_id = serializers.CharField(
        required=False, allow_null=True, allow_blank=True, max_length=400, help_text=DISTINCT_ID_HELP_TEXT
    )
    message_data = serializers.DictField(
        child=serializers.JSONField(allow_null=True),
        required=False,
        default=dict,
        help_text="Data for this recipient, merged over the batch's `message_data`: a top-level key set here "
        "replaces the same key there. Only a batch with `template_id` takes it.",
    )
    idempotency_key = TextOnlyField(
        trim_whitespace=False,
        help_text="A unique key for this recipient's email, 1 to 255 visible ASCII characters, different for each "
        "recipient of the batch. Retrying with the same key and the same email within 24 hours sends nothing new and "
        "returns the first send's `message_id`, also when the first send was a single send.",
    )

    def validate_idempotency_key(self, value: str) -> str:
        if not IDEMPOTENCY_KEY_PATTERN.fullmatch(value):
            raise serializers.ValidationError("Use 1 to 255 visible ASCII characters, with no spaces.")
        return value


class EmailBatchRecipientsField(LengthFirstListField):
    def __init__(self, **kwargs: Any) -> None:
        super().__init__(
            child=EmailBatchRecipientSerializer(), allow_empty=False, max_length=BATCH_MAX_RECIPIENTS, **kwargs
        )

    def run_child_validation(self, data: list[Any]) -> list[BatchRecipient]:
        recipients: list[BatchRecipient] = []
        errors: dict[str, dict[str, Any]] = {}
        positions: dict[str, int] = {}
        for index, item in enumerate(data):
            serializer = EmailBatchRecipientSerializer(data=item)
            if serializer.is_valid():
                fields = serializer.validated_data
                recipient = BatchRecipient(
                    to=fields["to"],
                    idempotency_key=fields["idempotency_key"],
                    distinct_id=fields.get("distinct_id") or None,
                    message_data=fields["message_data"],
                )
            elif set(serializer.errors) <= FIELDS_THAT_FAIL_ONE_RECIPIENT:
                field, messages = next(iter(serializer.errors.items()))
                recipient = BatchRecipient(
                    to=str(item.get("to", "")),
                    idempotency_key=str(item["idempotency_key"]),
                    refusal=InvalidRecipient(f"`{field}`: {messages[0]}"),
                )
            else:
                errors[str(index)] = {
                    name: value
                    for name, value in serializer.errors.items()
                    if name not in FIELDS_THAT_FAIL_ONE_RECIPIENT
                }
                continue
            first = positions.setdefault(recipient.idempotency_key, index)
            if first != index:
                errors[str(index)] = {
                    "idempotency_key": [
                        f"Recipient {first} has the same `idempotency_key`. Give each recipient its own key."
                    ]
                }
            recipients.append(recipient)
        if errors:
            raise serializers.ValidationError(errors)
        return recipients


class SendEmailBatchSerializer(SendEmailSerializer):
    recipients = EmailBatchRecipientsField(
        help_text=f"Who gets the email: 1 to {BATCH_MAX_RECIPIENTS} recipients, each sent its own email with its "
        "own `message_id`."
    )

    def get_fields(self) -> dict[str, serializers.Field]:
        fields = super().get_fields()
        for name in ("to", "distinct_id", *BATCH_REFUSED_FIELDS):
            fields.pop(name, None)
        return fields


class EmailBatchResultStatus(LabeledStrEnum):
    QUEUED = "queued", "Queued"


class EmailBatchErrorSerializer(serializers.Serializer):
    code = serializers.CharField(
        help_text="Why this recipient's email wasn't queued: `invalid_recipient`, `invalid_request`, "
        "`content_too_large`, `idempotency_key_reused`, `sandbox_recipient_not_member`, or `delivery_unavailable` "
        "when the email pipeline didn't confirm it, so it may still go out."
    )
    detail = serializers.CharField(help_text="What happened and what to do next, in words a person can read.")
    status = serializers.IntegerField(help_text="The HTTP status a single send of this email would answer.")
    retryable = serializers.BooleanField(
        help_text="True when sending it again with the same `idempotency_key` can succeed. Only "
        "`delivery_unavailable` is retryable."
    )


class EmailBatchResultSerializer(serializers.Serializer):
    message_id = serializers.CharField(
        help_text="ID of this recipient's email, the same for every send with its `idempotency_key`. Read the "
        "email's status with it, also when the result is an error."
    )
    status = serializers.ChoiceField(
        choices=EmailBatchResultStatus.choices,
        required=False,
        help_text="`queued` when PostHog accepted the email. The email can still be skipped later, for example "
        "when the address is on the suppression list, so read its status to see whether it went out. Absent with "
        "`error`.",
    )
    replayed = serializers.BooleanField(
        required=False,
        help_text="True when an earlier request with the same `idempotency_key` already queued this email, so no "
        "new email was sent. False doesn't prove this request sent it: an earlier request may still be on its way. "
        "Absent with `error`.",
    )
    error = EmailBatchErrorSerializer(required=False, help_text="Why this email wasn't queued. Absent when it was.")


class SendEmailBatchResponseSerializer(serializers.Serializer):
    results = EmailBatchResultSerializer(many=True, help_text="One result per recipient, in the order of the request.")
    warnings = SendWarningSerializer(
        many=True,
        required=False,
        help_text="Things the caller should act on although emails were queued. Left out when there are none.",
    )

    def to_representation(self, instance: Any) -> dict[str, Any]:
        data = super().to_representation(instance)
        if not data.get("warnings"):
            data.pop("warnings", None)
        return data


EMAIL_BATCH_RESPONSES: dict[int, ResponseDeclaration] = {
    200: OpenApiResponse(
        response=SendEmailBatchResponseSerializer,
        description="The batch was read. Each result says whether that recipient's email was queued.",
    ),
    400: OpenApiResponse(
        description="The shared email is invalid or can't be sent, its workflow isn't active, a recipient has no valid `idempotency_key` or "
        "shares one with another recipient, a field a batch doesn't take is present, there are more than "
        f"{BATCH_MAX_RECIPIENTS} recipients, or the shared content or `message_data` is over 256 KiB"
    ),
    403: OpenApiResponse(description="The sandbox sender isn't available for this organization, so nothing was sent"),
    404: OpenApiResponse(description="Template not found, or batch sends aren't available for this project"),
    409: OpenApiResponse(
        description="Batch sends aren't available for this project yet, so nothing was sent. Send each email on its "
        "own instead"
    ),
    413: OpenApiResponse(
        description="The request body is over 5 MiB, or the batch is larger than the email pipeline takes at once"
    ),
    415: OpenApiResponse(description="The request body isn't JSON"),
    422: OpenApiResponse(description="Email sending is suspended for this project, or paused for this email"),
    429: OpenApiResponse(
        description="The batch would go over the send rate limit, or the project is over its email quota, so "
        "nothing was sent"
    ),
    503: OpenApiResponse(
        description="The email pipeline did not confirm the batch. Some of its emails may go out. Read each "
        "recipient's status by its `idempotency_key`"
    ),
}


def refused_batch_field(data: Any, *, takes_template_key: bool) -> ExplainedRefusal | None:
    if not isinstance(data, Mapping):
        return None
    if field := next((field for field in BATCH_REFUSED_FIELDS if field in data), None):
        return _field_refusal(field)
    if field := next((field for field in RECIPIENT_FIELDS_OUTSIDE_RECIPIENTS if field in data), None):
        return ExplainedRefusal(
            f"A batch send names each recipient in `recipients`, so it doesn't take `{field}` outside them.",
            code="field_not_accepted",
            attr=field,
            why="Each recipient of a batch gets its own email, addressed by its own entry in `recipients`.",
            fix=f"Move `{RECIPIENT_FIELDS_OUTSIDE_RECIPIENTS[field]}` into each entry of `recipients`.",
        )
    if (unknown := next((field for field in data if field not in _batch_fields()), None)) is not None:
        return _unknown_field_refusal(unknown)
    recipients = data.get("recipients")
    if not isinstance(recipients, list) or len(recipients) > BATCH_MAX_RECIPIENTS:
        return None
    for index, recipient in enumerate(recipients):
        if not isinstance(recipient, Mapping):
            continue
        if field := next((field for field in BATCH_REFUSED_FIELDS if field in recipient), None):
            return _field_refusal(f"recipients__{index}__{field}")
        if (unknown := next((name for name in recipient if name not in RECIPIENT_FIELDS), None)) is not None:
            field = _shown(unknown)
            return ExplainedRefusal(
                f"A batch recipient doesn't take `{field}`, so PostHog didn't send the batch.",
                code="field_not_accepted",
                attr=f"recipients__{index}__{field}",
                why="A recipient takes only `to`, `distinct_id`, `message_data` and `idempotency_key`.",
                fix=f"Remove `{field}` from the recipient, or send this email on its own.",
            )
        if takes_template_key and "message_data" in recipient:
            return ExplainedRefusal(
                "A batch with `template_key` sends the same content to every recipient, so a recipient can't carry "
                "`message_data`.",
                code="field_not_accepted",
                attr=f"recipients__{index}__message_data",
                why="PostHog sends content your code rendered as is, with no data to fill in.",
                fix="Send personalized content from a template with `template_id`, or send each email on its own.",
            )
    return None


def _field_refusal(attr: str) -> ExplainedRefusal:
    field = attr.rsplit("__", 1)[-1]
    return ExplainedRefusal(
        f"A batch send doesn't take `{field}`, so PostHog didn't send it.",
        code="field_not_accepted",
        attr=attr,
        why="Batch sends don't support scheduling, copies or attachments.",
        fix=f"Remove `{field}`, or send this email on its own.",
    )


def batch_body_too_large() -> ExplainedRefusal:
    ceiling = settings.DATA_UPLOAD_MAX_MEMORY_SIZE
    limit = _size(BATCH_BODY_MAX_BYTES if ceiling is None else min(BATCH_BODY_MAX_BYTES, ceiling))
    return ExplainedRefusal(
        f"The request body is over {limit}, so PostHog didn't send the batch.",
        code="content_too_large",
        status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
        why=f"A batch send takes at most {limit} of request body.",
        fix="Split the recipients into smaller batches.",
    )


def batch_too_large_to_send() -> ExplainedRefusal:
    return ExplainedRefusal(
        "The batch is larger than PostHog can send at once, so PostHog didn't send it.",
        code="content_too_large",
        status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
        why="The email pipeline takes a limited amount of data in one batch.",
        fix="Split the recipients into smaller batches.",
    )


def batch_not_admitted() -> ExplainedRefusal:
    return ExplainedRefusal(
        "The email pipeline was at capacity and turned this batch away. Send the batch again with the same "
        "`idempotency_key` for each recipient within 24 hours of the first request that used it, so each email goes "
        "out once even if an earlier attempt queued it.",
        code="delivery_unavailable",
        status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
        why="PostHog's email pipeline was handling as many batches as it can.",
        fix="Send the batch again in a few seconds with the same `idempotency_key` for each recipient.",
    )


def unconfirmed_batch() -> ExplainedRefusal:
    return ExplainedRefusal(
        "The email pipeline did not confirm the batch, so some of its emails may still go out. To retry, send the "
        "batch again with the same `idempotency_key` for each recipient within 24 hours of the first request that "
        "used it, so each email goes out once.",
        code="delivery_unavailable",
        status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
        why="PostHog's email pipeline didn't answer for each recipient of the batch.",
        fix="Send the batch again with the same `idempotency_key` for each recipient within 24 hours of its first "
        "request.",
    )


def _size(size_bytes: int) -> str:
    if size_bytes % (1024 * 1024) == 0:
        return f"{size_bytes // (1024 * 1024)} MiB"
    if size_bytes % 1024 == 0:
        return f"{size_bytes // 1024} KiB"
    return f"{size_bytes:,} bytes"


def email_batch_response(sent: EmailBatchSent) -> dict[str, Any]:
    return SendEmailBatchResponseSerializer(
        {"results": [_result(result) for result in sent.results], "warnings": list(sent.warnings)}
    ).data


def email_batch_outcome(result: QueuedEmail | EmailNotQueued) -> dict[str, Any]:
    if isinstance(result, QueuedEmail):
        outcome: dict[str, Any] = {"outcome": "replayed" if result.replayed else "queued"}
        if result.warnings:
            outcome["warning_codes"] = [warning.code for warning in result.warnings]
        return outcome
    if isinstance(result.error, DeliveryUnavailable):
        return {"outcome": "unavailable"}
    return {"outcome": "rejected", "error_code": result.error.code}


def _result(result: QueuedEmail | EmailNotQueued) -> dict[str, Any]:
    if isinstance(result, QueuedEmail):
        return {"message_id": result.message_id, "status": EmailBatchResultStatus.QUEUED, "replayed": result.replayed}
    if isinstance(result.error, DeliveryUnavailable):
        error = {
            "code": "delivery_unavailable",
            "detail": UNCONFIRMED_EMAIL,
            "status": status.HTTP_503_SERVICE_UNAVAILABLE,
            "retryable": True,
        }
    else:
        code = result.error.code
        status_code = TransactionalEmailRejected.STATUS_BY_CODE.get(code, status.HTTP_400_BAD_REQUEST)
        error = {"code": code, "detail": str(result.error), "status": status_code, "retryable": False}
    return {"message_id": result.message_id, "error": error}


def _shown(name: str) -> str:
    return name if len(name) <= SHOWN_FIELD_NAME_MAX_LENGTH else f"{name[:SHOWN_FIELD_NAME_MAX_LENGTH]}…"


@cache
def _batch_fields() -> frozenset[str]:
    return frozenset(SendEmailBatchSerializer().fields.keys())


def _unknown_field_refusal(unknown: str) -> ExplainedRefusal:
    field = _shown(unknown)
    return ExplainedRefusal(
        f"A batch send doesn't take `{field}`, so PostHog didn't send the batch.",
        code="field_not_accepted",
        attr=field,
        why="A batch takes the fields of the shared email and `recipients`, and a misspelled field would be dropped.",
        fix=f"Remove `{field}`, or fix its spelling.",
    )
