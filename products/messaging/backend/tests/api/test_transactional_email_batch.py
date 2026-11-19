import re
import json
from typing import Any
from uuid import uuid4

from posthog.test.base import APIBaseTest
from unittest.mock import MagicMock, patch

from django.core.cache import cache
from django.test import override_settings

import requests
from parameterized import parameterized
from requests.structures import CaseInsensitiveDict
from rest_framework import status
from rest_framework.test import APIClient

from posthog import rate_limit
from posthog.models import ProjectSecretAPIKey
from posthog.models.instance_setting import override_instance_config
from posthog.models.integration import Integration
from posthog.models.personal_api_key import hash_key_value
from posthog.models.utils import generate_random_token_secret
from posthog.plugins.plugin_server_api import WORKFLOWS_TRANSACTIONAL_EMAIL_JWT_PURPOSE

from products.messaging.backend.models.message_template import MessageTemplate
from products.messaging.backend.models.transactional_email_key import TransactionalEmailKey
from products.messaging.backend.presentation.views.email_batch import UNCONFIRMED_EMAIL
from products.messaging.backend.presentation.views.message_templates import (
    EMAIL_BATCH_FLAG,
    EMAIL_FROM_CODE_FLAG,
    TRANSACTIONAL_EMAIL_API_FLAG,
    TransactionalEmailBurstThrottle,
    TransactionalEmailSustainedThrottle,
    TransactionalEmailTeamBurstThrottle,
    TransactionalEmailTeamSustainedThrottle,
)
from products.messaging.backend.services.transactional_email import (
    BATCH_SANDBOX_SENDER_WARNING,
    TRANSACTIONAL_EMAIL_KEY_LIMIT,
    make_transactional,
)
from products.messaging.backend.services.transactional_email_status import keyed_message_id
from products.messaging.backend.tests.fake_cdp_api import _response
from products.workflows.backend.facade.api import set_default_email_sender_if_unset
from products.workflows.backend.facade.models import HogFlow
from products.workflows.backend.facade.team_extension import TeamWorkflowsConfig

BATCH_URL = "/api/messaging/send/email_batch/"
SINGLE_URL = "/api/messaging/send/email/"
MESSAGE_ID = "0196a8f0-0000-7000-8000-000000000001"
SECOND_MESSAGE_ID = "0196a8f0-0000-7000-8000-000000000002"
ACCEPTED = {
    "status": 202,
    "body": {
        "invocation_id": MESSAGE_ID,
        "status": "queued",
        "scheduled_at": None,
        "replayed": False,
        "attachment_count": 0,
        "attachment_bytes": 0,
    },
}
RECIPIENT = {"to": "a@example.com", "idempotency_key": "key-a"}

WHOLE_CALL_REFUSALS = [
    ("batch_unavailable", 409, "batch_unavailable", 409, True),
    ("quota_limited", 429, "quota_limited", 429, True),
    ("sending_suspended", 422, "sending_suspended", 422, True),
    ("sending_paused", 422, "sending_paused", 422, True),
    ("sandbox_sender_unavailable", 403, "sandbox_sender_unavailable", 403, True),
    ("sandbox_fields_unsupported", 400, "invalid_request", 400, False),
    ("not_sendable", 400, "transactional_email_paused", 400, True),
    ("invalid_request", 400, "invalid_request", 400, True),
    ("Expected string, received number", 400, "invalid_request", 400, True),
    ("too_large", 413, "content_too_large", 413, False),
]
RECIPIENT_REFUSALS = [
    ("idempotency_key_reused", 422, "idempotency_key_reused", 422, True),
    ("sandbox_recipient_not_member", 422, "sandbox_recipient_not_member", 422, True),
    ("invalid_request", 400, "invalid_request", 400, True),
    ("a 400 without a known code", 400, "invalid_recipient", 400, True),
    ("message_too_large", 413, "content_too_large", 400, True),
    ("recipient_unreachable", 422, "invalid_recipient", 400, True),
    ("intake_unavailable", 503, "delivery_unavailable", 503, True),
    ("recipient_suppressed", 422, "delivery_unavailable", 503, True),
    ("idempotency_key_in_flight", 409, "delivery_unavailable", 503, True),
    ("not_attempted", 503, "delivery_unavailable", 503, True),
    ("idempotency_key_reused", 422, "idempotency_key_reused", 422, False),
    ("intake_unavailable", 503, "delivery_unavailable", 503, False),
]


BATCH_WIRE = {
    "template": (
        "/transactional_email/batch",
        {"variables", "recipients"},
        {"to", "distinct_id", "idempotency_key", "variables"},
    ),
    "code": (
        "/transactional_email/send/batch",
        {"variables", "send", "recipients"},
        {"to", "distinct_id", "idempotency_key"},
    ),
}


def _slot_request(forwarded: dict[str, Any], recipient: dict[str, Any]) -> dict[str, Any]:
    slot = {**recipient, "variables": {**forwarded.get("variables", {}), **recipient.get("variables", {})}}
    return {**slot, "send": forwarded["send"]} if "send" in forwarded else slot


def _batch_answer(*results: dict[str, Any]) -> requests.Response:
    return _response(200, {"results": list(results)})


def _forwarded(call: Any) -> dict[str, Any]:
    return call.kwargs["json"] if call.kwargs.get("json") is not None else json.loads(call.kwargs["data"])


@patch("products.messaging.backend.presentation.views.message_templates.posthoganalytics.capture")
@patch(
    "products.messaging.backend.presentation.views.message_templates.posthoganalytics.feature_enabled",
    return_value=True,
)
@patch("posthog.plugins.plugin_server_api.internal_requests.post")
class TestTransactionalEmailBatch(APIBaseTest):
    def setUp(self):
        super().setUp()
        cache.clear()
        self.sender = Integration.objects.create(
            team=self.team, kind="email", integration_id="hello@example.com", config={"verified": True}
        )
        set_default_email_sender_if_unset(self.team.id, self.sender.id)
        self.template = MessageTemplate.objects.create(
            team=self.team,
            name="Password reset",
            content={"email": {"subject": "Reset your password", "html": "<a href='{{ variables.link }}'>Reset</a>"}},
            type="email",
        )
        self.hog_flow_id = make_transactional(
            self.team.id, self.template.id, user_id=self.user.id, from_integration_id=None
        )
        raw_token = generate_random_token_secret()
        ProjectSecretAPIKey.objects.create(
            team=self.team,
            label="batch sends",
            secure_value=hash_key_value(raw_token),
            scopes=["transactional_email:write"],
            mask_value=f"{raw_token[:4]}...{raw_token[-4:]}",
        )
        self.psak = APIClient()
        self.psak.credentials(HTTP_AUTHORIZATION=f"Bearer {raw_token}")

    def tearDown(self):
        cache.clear()
        super().tearDown()

    def _email(self, form: str) -> dict[str, Any]:
        if form == "code":
            return {"template_key": "welcome", "subject": "Welcome", "html": "<p>Hi</p>"}
        return {"template_id": str(self.template.id), "message_data": {"link": "https://example.com/a", "n": 1}}

    def _recipients(self, count: int) -> list[dict[str, Any]]:
        return [{"to": f"user{index}@example.com", "idempotency_key": f"key-{index}"} for index in range(count)]

    def _batch(self, form: str = "template", recipients: list[dict[str, Any]] | None = None, /, **fields: Any) -> Any:
        body = {**self._email(form), "recipients": recipients or self._recipients(2), **fields}
        return self.psak.post(BATCH_URL, json.dumps(body), content_type="application/json")

    def _single(self, form: str = "template", **fields: Any) -> Any:
        body = {**self._email(form), "to": "user0@example.com", **fields}
        return self.psak.post(SINGLE_URL, body, format="json", headers={"Idempotency-Key": "key-0"})

    def _archive(self, hog_flow_id: Any) -> None:
        HogFlow.objects.filter(team=self.team, id=hog_flow_id).update(status="archived")

    @parameterized.expand([("template",), ("code",)])
    def test_each_recipient_is_forwarded_as_a_single_send_to_it_would_be(self, mock_post, _flag, capture, form):
        mock_post.return_value = MagicMock(status_code=202, json=lambda: {"invocation_id": "single"})
        merged_data = {"message_data": {"link": "https://example.com/a", "n": 2}} if form == "template" else {}
        assert self._single(form, distinct_id="user-0", **merged_data).status_code == status.HTTP_202_ACCEPTED
        single = _forwarded(mock_post.call_args)
        mock_post.return_value = _batch_answer(
            ACCEPTED, {"status": 202, "body": {"invocation_id": SECOND_MESSAGE_ID, "replayed": False}}
        )
        first_recipient: dict[str, Any] = {
            "to": "user0@example.com",
            "distinct_id": "user-0",
            "idempotency_key": "key-0",
        }
        if form == "template":
            first_recipient["message_data"] = {"n": 2}

        response = self._batch(form, [first_recipient, {"to": "user1@example.com", "idempotency_key": "key-1"}])

        assert response.status_code == status.HTTP_200_OK, response.json()
        assert response.json() == {
            "results": [
                {"message_id": MESSAGE_ID, "status": "queued", "replayed": False},
                {"message_id": SECOND_MESSAGE_ID, "status": "queued", "replayed": False},
            ]
        }
        url, forwarded = mock_post.call_args.args[0], _forwarded(mock_post.call_args)
        route, shared_fields, recipient_fields = BATCH_WIRE[form]
        assert url.endswith(route)
        assert set(forwarded) <= shared_fields
        assert all(set(recipient) <= recipient_fields for recipient in forwarded["recipients"])
        first, second = (_slot_request(forwarded, recipient) for recipient in forwarded["recipients"])
        assert first == single
        assert second == {
            **single,
            "to": "user1@example.com",
            "distinct_id": "user1@example.com",
            "idempotency_key": "key-1",
            "variables": forwarded.get("variables", {}),
        }
        single_event, *batch_events = [call.kwargs["properties"] for call in capture.call_args_list]
        assert {"batch_size", "batch_id"}.isdisjoint(single_event)
        assert [(event["batch_size"], event["outcome"], event["has_idempotency_key"]) for event in batch_events] == [
            (2, "queued", True)
        ] * 2
        assert batch_events[0]["batch_id"] == batch_events[1]["batch_id"]

    def test_each_forward_carries_a_token_for_its_own_route(self, mock_post, _flag, _capture):
        mock_post.return_value = MagicMock(status_code=202, json=lambda: {"invocation_id": "single"})
        self._single("template")
        self._single("code")
        mock_post.return_value = _batch_answer(ACCEPTED, ACCEPTED)
        self._batch("template")
        self._batch("code")

        forwards = []
        for call in mock_post.call_args_list:
            path = call.args[0].split("/api/projects/")[1]
            match = re.fullmatch(r"(\d+)/hog_flows/([^/]+)/transactional_email(.*)", path)
            assert match is not None
            team_id, hog_flow_id, route = match.groups()
            headers = CaseInsensitiveDict(call.kwargs["headers"])
            scheme, token = headers["Authorization"].split(" ", 1)
            assert scheme == "Bearer"
            claims = WORKFLOWS_TRANSACTIONAL_EMAIL_JWT_PURPOSE.verify(token)
            assert (claims["team_id"], claims["hog_flow_id"]) == (int(team_id), hog_flow_id)
            assert headers.get("Content-Encoding", "identity") == "identity"
            forwards.append((route, claims.get("kind")))
        assert forwards == [
            ("", None),
            ("/send", "code_send"),
            ("/batch", "template_batch"),
            ("/send/batch", "code_send_batch"),
        ]

    def test_a_recipient_the_pipeline_cannot_address_fails_only_its_own_result(self, mock_post, _flag, _capture):
        mock_post.return_value = _batch_answer(ACCEPTED)
        recipients: list[dict[str, Any]] = [
            {"to": "{{person.email}}@example.com", "idempotency_key": "key-0"},
            {"to": "user1@example.com", "idempotency_key": "key-1"},
            {"to": "not an address", "idempotency_key": "key-2"},
        ]

        response = self._batch("template", recipients)

        assert response.status_code == status.HTTP_200_OK, response.json()
        refused, queued, malformed = response.json()["results"]
        assert (refused["error"]["code"], refused["error"]["status"], refused["error"]["retryable"]) == (
            "invalid_recipient",
            400,
            False,
        )
        assert "{ or }" in refused["error"]["detail"]
        assert queued == {"message_id": MESSAGE_ID, "status": "queued", "replayed": False}
        assert malformed["error"]["code"] == "invalid_recipient"
        assert [refused["message_id"], malformed["message_id"]] == [
            str(keyed_message_id(self.team.id, "key-0")),
            str(keyed_message_id(self.team.id, "key-2")),
        ]
        assert [recipient["to"] for recipient in _forwarded(mock_post.call_args)["recipients"]] == ["user1@example.com"]

    @parameterized.expand(
        [
            ("duplicate_key", "template", {"recipients": [RECIPIENT] * 2}, 400, "recipients__1__idempotency_key"),
            (
                "missing_key",
                "template",
                {"recipients": [{"to": "a@example.com"}]},
                400,
                "recipients__0__idempotency_key",
            ),
            (
                "key_with_spaces",
                "template",
                {"recipients": [{**RECIPIENT, "idempotency_key": "a b"}]},
                400,
                "recipients__0__idempotency_key",
            ),
            (
                "too_many_recipients",
                "template",
                {"recipients": [{"to": "a@example.com", "idempotency_key": f"key-{i}"} for i in range(101)]},
                400,
                "recipients",
            ),
            (
                "too_many_recipients_before_their_fields",
                "template",
                {"recipients": [{"to": "a@example.com"} for _ in range(101)]},
                400,
                "recipients",
            ),
            (
                "too_many_recipients_with_a_refused_field",
                "code",
                {"recipients": [{**RECIPIENT, "cc": []}] + [RECIPIENT] * 100},
                400,
                "recipients",
            ),
            ("shared_scheduled_at", "code", {"scheduled_at": "2030-01-01T00:00:00Z"}, 400, "scheduled_at"),
            ("shared_to", "template", {"to": "a@example.com"}, 400, "to"),
            ("shared_distinct_id", "code", {"distinct_id": "person-1"}, 400, "distinct_id"),
            (
                "shared_unknown_field",
                "template",
                {"mesage_data": {"link": "https://example.com/a"}},
                400,
                "mesage_data",
            ),
            ("shared_unknown_field_with_a_long_name", "code", {"x" * 10_000: 1}, 400, "x" * 64 + "…"),
            ("shared_field_name_that_is_not_valid_unicode", "code", {"\ud800": 1}, 400, None),
            ("shared_html_with_a_lone_surrogate", "code", {"html": "<p>\ud800</p>"}, 400, None),
            ("sender_name_with_a_lone_surrogate", "code", {"from": "\ud800 <hello@example.com>"}, 400, None),
            (
                "recipient_data_with_a_lone_surrogate",
                "template",
                {"recipients": [{**RECIPIENT, "message_data": {"name": "\ud800"}}]},
                400,
                None,
            ),
            ("shared_legacy_identifiers", "template", {"identifiers": {"distinct_id": "person-1"}}, 400, "identifiers"),
            (
                "unverified_sender_and_no_recipient_it_can_address",
                "code",
                {
                    "from": "Example <hello@unverified.example>",
                    "recipients": [{"to": "not an address", "idempotency_key": "k"}],
                },
                400,
                "from",
            ),
            (
                "recipient_unknown_field",
                "template",
                {"recipients": [{**RECIPIENT, "identifiers": {"distinct_id": "person-1"}}]},
                400,
                "recipients__0__identifiers",
            ),
            (
                "recipient_unknown_field_after_an_empty_one",
                "template",
                {"recipients": [{**RECIPIENT, "": 1, "mesage_data": {"n": 1}}]},
                400,
                "recipients__0__",
            ),
            (
                "recipient_unknown_field_with_a_long_name",
                "template",
                {"recipients": [{**RECIPIENT, "x" * 10_000: 1}]},
                400,
                "recipients__0__" + "x" * 64 + "…",
            ),
            (
                "recipient_field_name_that_is_not_valid_unicode",
                "template",
                {"recipients": [{**RECIPIENT, "\ud800": 1}]},
                400,
                None,
            ),
            (
                "key_of_a_refused_address_repeated",
                "template",
                {
                    "recipients": [
                        {"to": "not an address", "idempotency_key": "7"},
                        {**RECIPIENT, "idempotency_key": "7"},
                    ]
                },
                400,
                "recipients__1__idempotency_key",
            ),
            (
                "key_that_is_a_number",
                "template",
                {"recipients": [{"to": "not an address", "idempotency_key": 7}]},
                400,
                "recipients__0__idempotency_key",
            ),
            ("shared_data_with_a_lone_surrogate", "template", {"message_data": {"name": "\ud800"}}, 400, None),
            ("shared_attachments", "template", {"attachments": []}, 400, "attachments"),
            ("recipient_cc", "code", {"recipients": [{**RECIPIENT, "cc": []}]}, 400, "recipients__0__cc"),
            (
                "recipient_message_data_with_template_key",
                "code",
                {"recipients": [{**RECIPIENT, "message_data": {"n": 1}}]},
                400,
                "recipients__0__message_data",
            ),
            ("shared_data_over_256_kib", "template", {"message_data": {"blob": "x" * 256 * 1024}}, 400, None),
            ("shared_content_over_256_kib", "code", {"html": "<p>" + "x" * 256 * 1024 + "</p>"}, 400, None),
            (
                "body_over_5_mib_with_a_refused_field",
                "template",
                {"scheduled_at": "2030-01-01T00:00:00Z", "message_data": {"blob": "x" * 5 * 1024 * 1024}},
                413,
                None,
            ),
        ]
    )
    def test_refuses_a_batch_it_cannot_send_as_a_whole(
        self, mock_post, _flag, _capture, _name, form, fields, expected_status, attr
    ):
        response = self._batch(form, **fields)

        assert response.status_code == expected_status, response.json()
        assert response.json()["attr"] == attr
        mock_post.assert_not_called()

    @parameterized.expand(
        [
            ("over_the_batch_cap", 5 * 1024 * 1024, 20 * 1024 * 1024, "5 MiB"),
            ("over_the_django_upload_ceiling", 2 * 1024 * 1024, 1024 * 1024, "1 MiB"),
            ("over_an_uneven_django_upload_ceiling", 2 * 1024 * 1024, 1024 * 1024 + 1, "1,048,577 bytes"),
        ]
    )
    def test_refuses_an_oversized_batch_and_reports_no_attachment_counts(
        self, mock_post, _flag, capture, _name, blob_bytes, upload_ceiling, named_limit
    ):
        with override_settings(DATA_UPLOAD_MAX_MEMORY_SIZE=upload_ceiling):
            response = self._batch("template", message_data={"blob": "x" * blob_bytes})

        assert response.status_code == status.HTTP_413_REQUEST_ENTITY_TOO_LARGE, response.json()
        assert response.json()["code"] == "content_too_large"
        assert f"over {named_limit}," in response.json()["detail"]
        refused = capture.call_args.kwargs["properties"]
        assert (refused["outcome"], refused["attachment_count"], refused["attachment_bytes"]) == (
            "rejected",
            None,
            None,
        )
        mock_post.assert_not_called()

    @parameterized.expand(
        [
            ("the_most_recipients_a_batch_takes", 100, "application/json"),
            ("a_json_media_type_in_any_case", 2, "Application/JSON; charset=utf-8"),
        ]
    )
    def test_queues_every_recipient_of_a_json_batch(self, mock_post, _flag, _capture, _name, count, content_type):
        mock_post.return_value = _batch_answer(*[ACCEPTED] * count)
        body = {**self._email("template"), "recipients": self._recipients(count)}

        response = self.psak.post(BATCH_URL, json.dumps(body), content_type=content_type)

        assert response.status_code == status.HTTP_200_OK, response.json()
        assert [result["status"] for result in response.json()["results"]] == ["queued"] * count

    def _form_encoded_batch(self) -> Any:
        return self.psak.post(
            BATCH_URL, {"template_id": str(self.template.id), "recipients[0]to": "a@example.com"}, format="multipart"
        )

    @parameterized.expand([("form_encoded", None), ("wildcard_subtype", "application/*"), ("wildcard", "*/*")])
    def test_refuses_a_batch_whose_body_is_not_named_json(self, mock_post, _flag, _capture, _name, content_type):
        if content_type is None:
            response = self._form_encoded_batch()
        else:
            body = {**self._email("template"), "recipients": self._recipients(2)}
            response = self.psak.post(BATCH_URL, json.dumps(body), content_type=content_type)

        assert response.status_code == status.HTTP_415_UNSUPPORTED_MEDIA_TYPE, response.json()
        mock_post.assert_not_called()

    @parameterized.expand(
        [
            ("constant_that_is_not_json", "NaN", 400, "parse_error"),
            ("number_beyond_a_float", "1e400", 400, "parse_error"),
            ("integer_beyond_a_float", "1" + "0" * 4000, 400, "parse_error"),
            ("member_named_twice", '"Welcome", "subject": "Welcome again"', 400, "parse_error"),
            ("nesting_past_the_limit", "[" * 101 + "]" * 101, 400, "parse_error"),
            ("nesting_too_deep_to_encode", "[" * 50_000 + "]" * 50_000, 400, "parse_error"),
            ("nesting_too_deep_to_decode", "[" * 100_000 + "]" * 100_000, 400, "parse_error"),
            ("cut_off_over_the_batch_cap", '"' + "x" * 6 * 1024 * 1024, 413, "content_too_large"),
        ]
    )
    def test_refuses_a_batch_body_that_is_not_strict_json(
        self, mock_post, _flag, _capture, _name, subject, expected_status, expected_code
    ):
        body = json.dumps({**self._email("code"), "subject": "SUBJECT", "recipients": self._recipients(1)})

        response = self.psak.post(BATCH_URL, body.replace('"SUBJECT"', subject), content_type="application/json")

        assert (response.status_code, response.json()["code"]) == (expected_status, expected_code)
        assert len(response.content) < 1024
        mock_post.assert_not_called()

    def test_reads_a_json_batch_whatever_charset_it_names(self, mock_post, _flag, _capture):
        mock_post.return_value = _batch_answer(ACCEPTED)
        body = json.dumps({**self._email("template"), "recipients": self._recipients(1)})

        response = self.psak.post(BATCH_URL, body, content_type="application/json; charset=base64_codec")

        assert response.status_code == status.HTTP_200_OK, response.json()

    def test_a_duplicate_key_names_both_recipients(self, mock_post, _flag, _capture):
        recipients = [{**RECIPIENT, "to": "a@example.com"}, {**RECIPIENT, "to": "b@example.com"}]

        response = self._batch("template", recipients)

        assert response.status_code == status.HTTP_400_BAD_REQUEST, response.json()
        assert response.json()["attr"] == "recipients__1__idempotency_key"
        assert "Recipient 0" in response.json()["detail"]

    @parameterized.expand([("template",), ("code",)])
    def test_a_disabled_workflow_still_forwards_and_the_pipeline_refuses_the_whole_batch(
        self, mock_post, _flag, _capture, form
    ):
        if form == "code":
            mock_post.return_value = MagicMock(status_code=202, json=lambda: {"invocation_id": "single"})
            assert self._single("code").status_code == status.HTTP_202_ACCEPTED
            hog_flow_id = TransactionalEmailKey.objects.for_team(self.team.id, canonical=True).get().hog_flow_id
        else:
            hog_flow_id = self.hog_flow_id
        self._archive(hog_flow_id)
        mock_post.return_value = _response(400, {"error": "not_sendable"})

        response = self._batch(form, self._recipients(2))

        assert (response.status_code, response.json()["code"]) == (400, "transactional_email_paused")
        assert f"/hog_flows/{hog_flow_id}/" in mock_post.call_args.args[0]

    @parameterized.expand(WHOLE_CALL_REFUSALS)
    def test_a_whole_call_refusal_answers_as_a_single_send_does(
        self, mock_post, _flag, capture, pipeline_code, pipeline_status, public_code, public_status, as_single_send
    ):
        mock_post.return_value = _response(pipeline_status, {"error": pipeline_code})
        single = self._single("code")
        capture.reset_mock()

        response = self._batch("code")

        assert (response.status_code, response.json()["code"]) == (public_status, public_code)
        assert "results" not in response.json()
        assert (response.json() == single.json()) == as_single_send
        if pipeline_code == "sandbox_fields_unsupported":
            assert "sandbox sender" in response.json()["detail"]
        assert [
            (call.kwargs["properties"]["outcome"], call.kwargs["properties"]["error_code"])
            for call in capture.call_args_list
        ] == [("rejected", public_code)]

    @parameterized.expand(RECIPIENT_REFUSALS)
    def test_each_recipient_refusal_maps_to_its_public_code_and_keeps_its_message_id(
        self, mock_post, _flag, _capture, pipeline_code, pipeline_status, public_code, public_status, answers_id
    ):
        refusal: dict[str, Any] = {"error": pipeline_code}
        if answers_id:
            refusal["invocation_id"] = SECOND_MESSAGE_ID
        mock_post.return_value = _batch_answer(ACCEPTED, {"status": pipeline_status, "body": refusal})

        response = self._batch("template", self._recipients(2))

        assert response.status_code == status.HTTP_200_OK, response.json()
        queued, refused = response.json()["results"]
        assert queued == {"message_id": MESSAGE_ID, "status": "queued", "replayed": False}
        assert refused["message_id"] == (
            SECOND_MESSAGE_ID if answers_id else str(keyed_message_id(self.team.id, "key-1"))
        )
        assert {name: refused["error"][name] for name in ("code", "status", "retryable")} == {
            "code": public_code,
            "status": public_status,
            "retryable": public_code == "delivery_unavailable",
        }
        if public_code == "delivery_unavailable":
            assert refused["error"]["detail"] == UNCONFIRMED_EMAIL

    @parameterized.expand(
        [
            ("pod_without_batch_routes", {"return_value": _response(401, {"error": "Unauthorized"})}),
            ("pipeline_error", {"return_value": _response(500, {})}),
            ("a_server_error_with_a_code", {"return_value": _response(500, {"error": "internal_error"})}),
            ("a_missed_deadline", {"return_value": _response(503, {"error": "deadline_exceeded"})}),
            ("an_unknown_refusal", {"return_value": _response(503, {"error": "some_new_refusal"})}),
            ("timeout", {"side_effect": requests.ReadTimeout()}),
            ("an_answer_missing", {"return_value": _batch_answer(ACCEPTED)}),
            ("an_answer_without_a_status", {"return_value": _batch_answer(ACCEPTED, {"body": {}})}),
            (
                "an_answer_with_an_empty_invocation_id",
                {
                    "return_value": _batch_answer(
                        ACCEPTED, {"status": 202, "body": {"invocation_id": "", "replayed": False}}
                    )
                },
            ),
            (
                "an_answer_with_an_invocation_id_that_is_not_a_uuid",
                {
                    "return_value": _batch_answer(
                        ACCEPTED, {"status": 202, "body": {"invocation_id": "not-a-uuid", "replayed": False}}
                    )
                },
            ),
            (
                "an_answer_without_a_replay_marker",
                {
                    "return_value": _batch_answer(
                        ACCEPTED, {"status": 202, "body": {"invocation_id": SECOND_MESSAGE_ID}}
                    )
                },
            ),
            (
                "an_answer_with_an_invocation_id_that_is_not_valid_unicode",
                {
                    "return_value": _batch_answer(
                        ACCEPTED, {"status": 202, "body": {"invocation_id": "\ud800", "replayed": False}}
                    )
                },
            ),
            (
                "an_answer_with_a_replay_marker_that_is_not_a_bool",
                {
                    "return_value": _batch_answer(
                        ACCEPTED, {"status": 202, "body": {"invocation_id": SECOND_MESSAGE_ID, "replayed": "no"}}
                    )
                },
            ),
            (
                "an_answer_that_is_queued_and_refused",
                {
                    "return_value": _batch_answer(
                        ACCEPTED,
                        {
                            "status": 202,
                            "body": {
                                "invocation_id": SECOND_MESSAGE_ID,
                                "replayed": False,
                                "error": "recipient_suppressed",
                            },
                        },
                    )
                },
            ),
            (
                "a_refusal_with_an_invocation_id_that_is_not_a_uuid",
                {
                    "return_value": _batch_answer(
                        ACCEPTED, {"status": 422, "body": {"error": "idempotency_key_reused", "invocation_id": "x"}}
                    )
                },
            ),
            ("a_whole_call_refusal_as_a_server_error", {"return_value": _response(503, {"error": "quota_limited"})}),
            (
                "a_recipient_refusal_for_the_whole_call",
                {"return_value": _response(422, {"error": "idempotency_key_reused"})},
            ),
            ("a_missing_team", {"return_value": _response(404, {"error": "Team not found"})}),
            (
                "an_answer_with_a_success_status_and_a_refusal",
                {"return_value": _batch_answer(ACCEPTED, {"status": 200, "body": {"error": "recipient_suppressed"}})},
            ),
            (
                "a_body_that_is_not_json",
                {"return_value": MagicMock(status_code=200, json=MagicMock(side_effect=ValueError))},
            ),
        ]
    )
    def test_a_batch_the_pipeline_does_not_answer_in_full_is_unavailable(
        self, mock_post, _flag, capture, _name, answer
    ):
        mock_post.configure_mock(**answer)

        response = self._batch("code")

        assert response.status_code == status.HTTP_503_SERVICE_UNAVAILABLE, response.json()
        assert response.json()["code"] == "delivery_unavailable"
        assert "may still go out" in response.json()["detail"]
        assert [call.kwargs["properties"]["outcome"] for call in capture.call_args_list] == ["unavailable"]

    def test_a_batch_the_pipeline_turns_away_says_this_request_queued_nothing(self, mock_post, _flag, capture):
        mock_post.return_value = _response(503, {"error": "over_capacity"})

        response = self._batch("code")

        assert response.status_code == status.HTTP_503_SERVICE_UNAVAILABLE, response.json()
        assert response.json()["code"] == "delivery_unavailable"
        assert "turned this batch away" in response.json()["detail"]
        assert "may still go out" not in response.json()["detail"]
        assert [call.kwargs["properties"]["outcome"] for call in capture.call_args_list] == ["not_admitted"]

    @patch("posthog.rate_limit.team_is_allowed_to_bypass_throttle", return_value=False)
    @patch("posthog.rate_limit.is_rate_limit_enabled", return_value=True)
    def test_the_rate_limit_is_reserved_before_the_key_is_created(self, _enabled, _bypass, mock_post, _flag, _capture):
        mock_post.return_value = _batch_answer(*[ACCEPTED] * 5)

        with patch.object(TransactionalEmailTeamBurstThrottle, "rate", "4/minute"):
            response = self._batch("code", self._recipients(5))

        assert response.status_code == status.HTTP_429_TOO_MANY_REQUESTS
        assert not TransactionalEmailKey.objects.for_team(self.team.id, canonical=True).exists()
        mock_post.assert_not_called()

    @parameterized.expand(
        [
            ("key_burst", TransactionalEmailBurstThrottle),
            ("key_sustained", TransactionalEmailSustainedThrottle),
            ("team_burst", TransactionalEmailTeamBurstThrottle),
            ("team_sustained", TransactionalEmailTeamSustainedThrottle),
        ]
    )
    def test_a_batch_spends_one_send_per_recipient_and_a_refused_one_spends_one(
        self, mock_post, _flag, _capture, _name, throttle
    ):
        with (
            patch("posthog.rate_limit.team_is_allowed_to_bypass_throttle", return_value=False),
            patch("posthog.rate_limit.is_rate_limit_enabled", return_value=True),
            patch.object(throttle, "rate", "5/minute"),
        ):
            mock_post.return_value = _batch_answer(*[ACCEPTED] * 3)
            spent = self._batch("template", self._recipients(3))
            refused = self._batch("template", self._recipients(3))
            mock_post.return_value = MagicMock(status_code=202, json=lambda: {"invocation_id": "single"})
            last_single = self._single("template")
            over_budget = self._single("template")

        assert spent.status_code == status.HTTP_200_OK, spent.json()
        assert refused.status_code == status.HTTP_429_TOO_MANY_REQUESTS
        assert int(refused.headers["Retry-After"]) > 0
        assert last_single.status_code == status.HTTP_202_ACCEPTED, last_single.json()
        assert over_budget.status_code == status.HTTP_429_TOO_MANY_REQUESTS

    @parameterized.expand(
        [
            ("from", {"from": "Example <hello@example.org>"}),
            ("reply_to", {"reply_to": ["help@example.org"]}),
            ("both", {"from": "Example <hello@example.org>", "reply_to": ["help@example.org"]}),
        ]
    )
    def test_a_sandbox_batch_that_names_a_sender_answers_as_a_single_send_does(
        self, mock_post, _flag, _capture, _name, sender
    ):
        Integration.objects.filter(id=self.sender.id).delete()
        TeamWorkflowsConfig.objects.filter(team_id=self.team.id).update(default_email_integration_id=None)
        sandbox_send = {"invocation_id": MESSAGE_ID, "replayed": False, "sender_kind": "email_sandbox"}
        mock_post.return_value = MagicMock(status_code=202, json=lambda: sandbox_send)
        single = self._single("code", **sender)
        single_send = _forwarded(mock_post.call_args)["send"] if mock_post.called else None
        mock_post.reset_mock()
        mock_post.return_value = _batch_answer(*[{"status": 202, "body": sandbox_send}] * 2)

        batch = self._batch("code", **sender)

        if single.status_code == status.HTTP_202_ACCEPTED:
            assert batch.status_code == status.HTTP_200_OK, batch.json()
            assert batch.json()["warnings"] == [
                {**warning, "message": BATCH_SANDBOX_SENDER_WARNING} for warning in single.json()["warnings"]
            ]
            (batch_call,) = mock_post.call_args_list
            assert _forwarded(batch_call)["send"] == single_send
        else:
            assert (batch.status_code, batch.json()) == (single.status_code, single.json())

    @parameterized.expand(
        [
            ("every_recipient_from_the_sandbox_sender", "template", "email_sandbox", "email_sandbox", True),
            ("only_a_replay_from_the_sandbox_sender", "code", None, "email_sandbox", True),
            ("every_recipient_from_its_own_sender", "template", None, None, False),
        ]
    )
    def test_warns_once_when_any_queued_recipient_went_out_from_the_sandbox_sender(
        self, mock_post, _flag, _capture, _name, form, queued_sender_kind, replayed_sender_kind, warns
    ):
        mock_post.return_value = _batch_answer(
            {"status": 202, "body": {**ACCEPTED["body"], "sender_kind": queued_sender_kind}},
            {
                "status": 202,
                "body": {"invocation_id": SECOND_MESSAGE_ID, "replayed": True, "sender_kind": replayed_sender_kind},
            },
            {"status": 422, "body": {"error": "sandbox_recipient_not_member"}},
        )

        response = self._batch(form, self._recipients(3))

        assert response.status_code == status.HTTP_200_OK, response.json()
        warnings = response.json().get("warnings", [])
        assert [(warning["code"], warning["message"]) for warning in warnings] == (
            [("sandbox_sender", BATCH_SANDBOX_SENDER_WARNING)] if warns else []
        )

    @parameterized.expand([("verified_sender", True), ("sandbox_sender", False)])
    def test_a_code_batch_with_no_recipient_it_can_address_creates_nothing(
        self, mock_post, _flag, _capture, _name, verified_sender
    ):
        if not verified_sender:
            Integration.objects.filter(id=self.sender.id).delete()
            TeamWorkflowsConfig.objects.filter(team_id=self.team.id).update(default_email_integration_id=None)
        workflows = HogFlow.objects.filter(team=self.team).count()
        integrations = Integration.objects.filter(team=self.team).count()

        response = self._batch("code", [{"to": "not an address", "idempotency_key": "key-0"}])

        assert response.status_code == status.HTTP_200_OK, response.json()
        assert [result["error"]["code"] for result in response.json()["results"]] == ["invalid_recipient"]
        assert not TransactionalEmailKey.objects.for_team(self.team.id, canonical=True).exists()
        assert HogFlow.objects.filter(team=self.team).count() == workflows
        assert Integration.objects.filter(team=self.team).count() == integrations
        mock_post.assert_not_called()

    @parameterized.expand(
        [
            ("sensitive_template_marked_not_sensitive", True, False, False, "sensitive_template"),
            ("new_key_past_the_key_limit", False, False, True, "too_many_keys"),
            ("known_key_at_the_key_limit", False, True, True, "invalid_recipient"),
        ]
    )
    def test_a_code_batch_with_no_recipient_it_can_address_answers_as_its_shared_email_does(
        self, mock_post, _flag, _capture, _name, sensitive_template, key_known, at_the_key_limit, code
    ):
        MessageTemplate.objects.filter(id=self.template.id).update(
            key="welcome" if sensitive_template else None, sensitive=sensitive_template
        )
        if key_known:
            mock_post.return_value = MagicMock(status_code=202, json=lambda: {"invocation_id": "single"})
            assert self._single("code").status_code == status.HTTP_202_ACCEPTED
            mock_post.reset_mock()
        if at_the_key_limit:
            room = (
                TRANSACTIONAL_EMAIL_KEY_LIMIT
                - TransactionalEmailKey.objects.for_team(self.team.id, canonical=True).count()
            )
            TransactionalEmailKey.objects.for_team(self.team.id, canonical=True).bulk_create(
                TransactionalEmailKey(team_id=self.team.id, key=f"seeded-{index}", hog_flow_id=uuid4())
                for index in range(room)
            )
        keys = TransactionalEmailKey.objects.for_team(self.team.id, canonical=True).count()

        response = self._batch("code", [{"to": "not an address", "idempotency_key": "key-0"}], sensitive=False)

        answer = response.json()
        codes = [result["error"]["code"] for result in answer["results"]] if "results" in answer else [answer["code"]]
        assert codes == [code], answer
        assert TransactionalEmailKey.objects.for_team(self.team.id, canonical=True).count() == keys
        mock_post.assert_not_called()

    @parameterized.expand(
        [("batch_flag_off", EMAIL_BATCH_FLAG, "template"), ("code_flag_off", EMAIL_FROM_CODE_FLAG, "code")]
    )
    def test_a_batch_while_its_flag_is_off_spends_no_send(self, mock_post, flag, _capture, _name, flag_off, form):
        flag.side_effect = lambda key, *_args, **_kwargs: key != flag_off
        mock_post.return_value = MagicMock(status_code=202, json=lambda: {"invocation_id": "single"})
        rate_limit.is_rate_limit_enabled.cache_clear()
        rate_limit.get_team_allow_list.cache_clear()
        self.addCleanup(rate_limit.is_rate_limit_enabled.cache_clear)
        self.addCleanup(rate_limit.get_team_allow_list.cache_clear)

        with (
            override_instance_config("RATE_LIMIT_ENABLED", True),
            patch.object(TransactionalEmailTeamBurstThrottle, "rate", "1/minute"),
        ):
            hidden = self._batch(form)
            single = self._single("template")
            hidden_past_the_budget = self._batch(form)

        assert hidden.status_code == status.HTTP_404_NOT_FOUND
        assert single.status_code == status.HTTP_202_ACCEPTED, single.json()
        assert hidden_past_the_budget.status_code == status.HTTP_404_NOT_FOUND

    @parameterized.expand(
        [
            ("batch_flag_off", "template", {TRANSACTIONAL_EMAIL_API_FLAG, EMAIL_FROM_CODE_FLAG}),
            ("batch_flag_off_form_encoded", "form", {TRANSACTIONAL_EMAIL_API_FLAG, EMAIL_FROM_CODE_FLAG}),
            ("batch_flag_off_malformed_json", "malformed", {TRANSACTIONAL_EMAIL_API_FLAG, EMAIL_FROM_CODE_FLAG}),
            ("batch_flag_off_over_the_upload_ceiling", "huge", {TRANSACTIONAL_EMAIL_API_FLAG, EMAIL_FROM_CODE_FLAG}),
            ("template_flag_off", "template", {EMAIL_BATCH_FLAG, EMAIL_FROM_CODE_FLAG}),
            ("code_flag_off", "code", {EMAIL_BATCH_FLAG, TRANSACTIONAL_EMAIL_API_FLAG}),
        ]
    )
    def test_is_hidden_while_a_flag_it_needs_is_off(self, mock_post, flag, _capture, _name, form, enabled_flags):
        flag.side_effect = lambda key, *_args, **_kwargs: key in enabled_flags

        with override_settings(DATA_UPLOAD_MAX_MEMORY_SIZE=1024 * 1024):
            if form == "form":
                response = self._form_encoded_batch()
            elif form == "malformed":
                response = self.psak.post(BATCH_URL, '{"recipients": [', content_type="application/json")
            elif form == "huge":
                response = self._batch("template", message_data={"blob": "x" * 2 * 1024 * 1024})
            else:
                response = self._batch(form)

        assert response.status_code == status.HTTP_404_NOT_FOUND
        mock_post.assert_not_called()
