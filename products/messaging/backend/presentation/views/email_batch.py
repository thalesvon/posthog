import sys
import json
import math
from collections.abc import Mapping
from functools import cache
from typing import Any

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
    InvalidRecipient,
    QueuedEmail,
    SendNotAttempted,
    SendNotQueued,
    TransactionalEmailError,
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
BATCH_REFUSED_FIELDS = ("scheduled_at", *CODE_SEND_EXTRA_FIELDS)
FIELDS_THAT_FAIL_ONE_RECIPIENT = frozenset({"to", "distinct_id"})
RECIPIENT_FIELDS_OUTSIDE_RECIPIENTS = {"to": "to", "distinct_id": "distinct_id", "identifiers": "distinct_id"}
RECIPIENT_FIELDS = frozenset({"to", "distinct_id", "message_data", "idempotency_key"})
RETRYABLE_CODES = frozenset({"idempotency_key_in_flight", "quota_limited", "delivery_unavailable", "not_attempted"})
UNCONFIRMED_EMAIL = (
    "The email pipeline did not confirm this email, so it may still go out. To retry, send it again within 24 hours "
    "with the same `idempotency_key`, so it goes out once."
)
NOT_ATTEMPTED_EMAIL = (
    "PostHog stopped before it sent this email, so nothing was queued. Send it again with the same `idempotency_key`."
)
NOT_QUEUED_EMAIL = (
    "The email pipeline failed before it queued this email, so it won't go out. Send it again with the same "
    "`idempotency_key`."
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
    pending = [(value, 1)]
    while pending:
        item, depth = pending.pop()
        if not isinstance(item, dict | list):
            continue
        if depth > BATCH_BODY_MAX_DEPTH:
            return True
        pending.extend((child, depth + 1) for child in (item.values() if isinstance(item, dict) else item))
    return False


def _members(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    members = dict(pairs)
    if len(members) != len(pairs):
        raise ValueError("an object names the same member twice")
    return members


def _refused_constant(text: str) -> float:
    raise ValueError(f"{text} isn't a JSON value")


def _finite(text: str) -> float:
    if not math.isfinite(value := float(text)):
        raise ValueError(f"{text} is out of range")
    return value


def _finite_int(text: str) -> int:
    if abs(value := int(text)) > sys.float_info.max:
        raise ValueError(f"{text} is out of range")
    return value


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
    idempotency_key = serializers.CharField(
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
        help_text="Why this recipient's email wasn't queued, the code a single send would answer, such as "
        "`recipient_suppressed`. `not_attempted` means PostHog stopped before this email, so nothing was queued."
    )
    detail = serializers.CharField(help_text="What happened and what to do next, in words a person can read.")
    status = serializers.IntegerField(help_text="The HTTP status a single send of this email would answer.")
    retryable = serializers.BooleanField(
        help_text="True when sending it again with the same `idempotency_key` can succeed."
    )


class EmailBatchResultSerializer(serializers.Serializer):
    message_id = serializers.CharField(required=False, help_text="ID of the queued email. Absent with `error`.")
    status = serializers.ChoiceField(
        choices=EmailBatchResultStatus.choices, required=False, help_text="`queued` when the email was queued."
    )
    replayed = serializers.BooleanField(
        required=False,
        help_text="True when an earlier request with the same `idempotency_key` already queued this email. No new "
        "email was sent, and `message_id` is the earlier send's ID.",
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
        description="The shared email is invalid or can't be sent, a recipient has no valid `idempotency_key` or "
        "shares one with another recipient, a field a batch doesn't take is present, there are more than "
        f"{BATCH_MAX_RECIPIENTS} recipients, or the shared content or `message_data` is over 256 KiB"
    ),
    404: OpenApiResponse(description="Template not found, or batch sends aren't available for this project"),
    413: OpenApiResponse(description="The request body is over 5 MiB"),
    415: OpenApiResponse(description="The request body isn't JSON"),
    429: OpenApiResponse(description="The batch would go over the send rate limit, so nothing was sent"),
    503: OpenApiResponse(description="The email pipeline did not confirm the batch. Some of its emails may go out"),
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
    return ExplainedRefusal(
        f"The request body is over {BATCH_BODY_MAX_BYTES // (1024 * 1024)} MiB, so PostHog didn't send the batch.",
        code="content_too_large",
        status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
        why=f"A batch send takes at most {BATCH_BODY_MAX_BYTES // (1024 * 1024)} MiB of request body.",
        fix="Split the recipients into smaller batches.",
    )


def unconfirmed_batch() -> ExplainedRefusal:
    return ExplainedRefusal(
        "The email pipeline did not confirm the batch, so some of its emails may still go out. To retry, send the "
        "batch again within 24 hours with the same `idempotency_key` for each recipient, so each email goes out once.",
        code="delivery_unavailable",
        status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
        why="PostHog's email pipeline didn't answer for each recipient of the batch.",
        fix="Send the batch again within 24 hours with the same `idempotency_key` for each recipient.",
    )


def email_batch_response(sent: EmailBatchSent) -> dict[str, Any]:
    return SendEmailBatchResponseSerializer(
        {"results": [_result(result) for result in sent.results], "warnings": list(sent.warnings)}
    ).data


def email_batch_outcome(result: QueuedEmail | TransactionalEmailError | DeliveryUnavailable) -> dict[str, Any]:
    if isinstance(result, QueuedEmail):
        outcome: dict[str, Any] = {"outcome": "replayed" if result.replayed else "queued"}
        if result.warnings:
            outcome["warning_codes"] = [warning.code for warning in result.warnings]
        return outcome
    if isinstance(result, SendNotAttempted):
        return {"outcome": "not_attempted"}
    if isinstance(result, SendNotQueued):
        return {"outcome": "not_queued"}
    if isinstance(result, DeliveryUnavailable):
        return {"outcome": "unavailable"}
    return {"outcome": "rejected", "error_code": result.code}


def _result(result: QueuedEmail | TransactionalEmailError | DeliveryUnavailable) -> dict[str, Any]:
    if isinstance(result, QueuedEmail):
        return {"message_id": result.message_id, "status": EmailBatchResultStatus.QUEUED, "replayed": result.replayed}
    status_code: int
    if isinstance(result, SendNotAttempted):
        code, detail, status_code = "not_attempted", NOT_ATTEMPTED_EMAIL, status.HTTP_503_SERVICE_UNAVAILABLE
    elif isinstance(result, SendNotQueued):
        code, detail, status_code = "delivery_unavailable", NOT_QUEUED_EMAIL, status.HTTP_503_SERVICE_UNAVAILABLE
    elif isinstance(result, DeliveryUnavailable):
        code, detail, status_code = "delivery_unavailable", UNCONFIRMED_EMAIL, status.HTTP_503_SERVICE_UNAVAILABLE
    else:
        code, detail = result.code, str(result)
        status_code = TransactionalEmailRejected.STATUS_BY_CODE.get(code, status.HTTP_400_BAD_REQUEST)
    return {"error": {"code": code, "detail": detail, "status": status_code, "retryable": code in RETRYABLE_CODES}}


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
