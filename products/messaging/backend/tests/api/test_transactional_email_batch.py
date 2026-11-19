import re
import json
from typing import Any

from posthog.test.base import APIBaseTest
from unittest.mock import MagicMock, patch

from django.core.cache import cache
from django.test import override_settings

import requests
from parameterized import parameterized
from requests.structures import CaseInsensitiveDict
from rest_framework import status
from rest_framework.test import APIClient

from posthog.models import ProjectSecretAPIKey
from posthog.models.integration import Integration
from posthog.models.personal_api_key import hash_key_value
from posthog.models.utils import generate_random_token_secret
from posthog.plugins.plugin_server_api import WORKFLOWS_TRANSACTIONAL_EMAIL_JWT_PURPOSE

from products.messaging.backend.models.message_template import MessageTemplate
from products.messaging.backend.models.transactional_email_key import TransactionalEmailKey
from products.messaging.backend.presentation.views.message_templates import (
    EMAIL_BATCH_FLAG,
    EMAIL_FROM_CODE_FLAG,
    TRANSACTIONAL_EMAIL_API_FLAG,
    TransactionalEmailBurstThrottle,
    TransactionalEmailSustainedThrottle,
    TransactionalEmailTeamBurstThrottle,
    TransactionalEmailTeamSustainedThrottle,
)
from products.messaging.backend.services.transactional_email import PIPELINE_REFUSALS, make_transactional
from products.messaging.backend.tests.fake_cdp_api import _response
from products.workflows.backend.facade.api import set_default_email_sender_if_unset
from products.workflows.backend.facade.models import HogFlow
from products.workflows.backend.facade.team_extension import TeamWorkflowsConfig

BATCH_URL = "/api/messaging/send/email_batch/"
SINGLE_URL = "/api/messaging/send/email/"
ACCEPTED = {"status": 202, "body": {"invocation_id": "invocation-1"}}
RECIPIENT = {"to": "a@example.com", "idempotency_key": "key-a"}

PIPELINE_STATUSES = {
    "quota_limited": 429,
    "idempotency_key_in_flight": 409,
    "idempotency_key_reused": 422,
    "sending_suspended": 422,
    "sending_paused": 422,
    "recipient_suppressed": 422,
    "recipient_unreachable": 422,
    "sandbox_recipient_not_member": 422,
    "sandbox_sender_unavailable": 403,
}
PIPELINE_ANSWERS = [
    *((code, PIPELINE_STATUSES.get(code, 400)) for code in PIPELINE_REFUSALS),
    ("a 400 without a known code", 400),
    ("message_too_large", 413),
    ("intake_unavailable", 503),
    ("Workflow not found", 404),
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
        mock_post.return_value = _batch_answer(ACCEPTED, {"status": 202, "body": {"invocation_id": "i-2"}})
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
                {"message_id": "invocation-1", "status": "queued", "replayed": False},
                {"message_id": "i-2", "status": "queued", "replayed": False},
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
            {"to": "user3@example.com", "idempotency_key": "key-3", "message_data": {"name": "\ud800"}},
        ]

        response = self._batch("template", recipients)

        assert response.status_code == status.HTTP_200_OK, response.json()
        refused, queued, malformed, unencodable = response.json()["results"]
        assert (refused["error"]["code"], refused["error"]["status"], refused["error"]["retryable"]) == (
            "invalid_recipient",
            400,
            False,
        )
        assert "{ or }" in refused["error"]["detail"]
        assert queued == {"message_id": "invocation-1", "status": "queued", "replayed": False}
        assert malformed["error"]["code"] == "invalid_recipient"
        assert unencodable["error"]["code"] == "invalid_recipient"
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
                "recipient_unknown_field",
                "template",
                {"recipients": [{**RECIPIENT, "identifiers": {"distinct_id": "person-1"}}]},
                400,
                "recipients__0__identifiers",
            ),
            (
                "key_of_a_refused_address_repeated_as_text",
                "template",
                {"recipients": [{"to": "not an address", "idempotency_key": 7}, {**RECIPIENT, "idempotency_key": "7"}]},
                400,
                "recipients__1__idempotency_key",
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
            ("over_the_batch_cap", 5 * 1024 * 1024, 20 * 1024 * 1024),
            ("over_the_django_upload_ceiling", 2 * 1024 * 1024, 1024 * 1024),
        ]
    )
    def test_refuses_an_oversized_batch_and_reports_no_attachment_counts(
        self, mock_post, _flag, capture, _name, blob_bytes, upload_ceiling
    ):
        with override_settings(DATA_UPLOAD_MAX_MEMORY_SIZE=upload_ceiling):
            response = self._batch("template", message_data={"blob": "x" * blob_bytes})

        assert response.status_code == status.HTTP_413_REQUEST_ENTITY_TOO_LARGE, response.json()
        assert response.json()["code"] == "content_too_large"
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

    def test_a_duplicate_key_names_both_recipients(self, mock_post, _flag, _capture):
        recipients = [{**RECIPIENT, "to": "a@example.com"}, {**RECIPIENT, "to": "b@example.com"}]

        response = self._batch("template", recipients)

        assert response.status_code == status.HTTP_400_BAD_REQUEST, response.json()
        assert response.json()["attr"] == "recipients__1__idempotency_key"
        assert "Recipient 0" in response.json()["detail"]

    @parameterized.expand([("template",), ("code",)])
    def test_a_disabled_workflow_still_forwards_and_each_answer_maps_to_its_recipient(
        self, mock_post, _flag, _capture, form
    ):
        if form == "code":
            mock_post.return_value = MagicMock(status_code=202, json=lambda: {"invocation_id": "single"})
            assert self._single("code").status_code == status.HTTP_202_ACCEPTED
            hog_flow_id = TransactionalEmailKey.objects.for_team(self.team.id, canonical=True).get().hog_flow_id
        else:
            hog_flow_id = self.hog_flow_id
        self._archive(hog_flow_id)
        mock_post.return_value = _batch_answer(
            {"status": 202, "body": {"invocation_id": "first-send", "replayed": True}},
            {"status": 400, "body": {"error": "not_sendable"}},
            {"status": 503, "body": {"error": "not_attempted"}},
        )

        response = self._batch(form, self._recipients(3))

        assert response.status_code == status.HTTP_200_OK, response.json()
        replayed, refused, unstarted = response.json()["results"]
        assert replayed == {"message_id": "first-send", "status": "queued", "replayed": True}
        assert {name: refused["error"][name] for name in ("code", "status", "retryable")} == {
            "code": "transactional_email_paused",
            "status": 400,
            "retryable": False,
        }
        assert {name: unstarted["error"][name] for name in ("code", "status", "retryable")} == {
            "code": "not_attempted",
            "status": 503,
            "retryable": True,
        }
        assert f"/hog_flows/{hog_flow_id}/" in mock_post.call_args.args[0]

    @parameterized.expand(PIPELINE_ANSWERS)
    def test_each_recipient_gets_the_answer_a_single_send_gets_for_the_same_refusal(
        self, mock_post, _flag, _capture, pipeline_code, pipeline_status
    ):
        mock_post.return_value = _response(pipeline_status, {"error": pipeline_code})
        single = self._single("template")
        mock_post.return_value = _batch_answer({"status": pipeline_status, "body": {"error": pipeline_code}})

        (result,) = self._batch("template", self._recipients(1)).json()["results"]

        assert (result["error"]["code"], result["error"]["status"]) == (single.json()["code"], single.status_code)
        assert result["error"]["retryable"] == (single.status_code in (409, 429, 503))

    @parameterized.expand(
        [
            ("pod_without_batch_routes", {"return_value": _response(401, {"error": "Unauthorized"})}),
            ("pipeline_error", {"return_value": _response(500, {})}),
            ("admission_refused", {"return_value": _response(503, {"error": "batch_capacity"})}),
            ("timeout", {"side_effect": requests.ReadTimeout()}),
            ("an_answer_missing", {"return_value": _batch_answer(ACCEPTED)}),
            ("an_answer_without_a_status", {"return_value": _batch_answer(ACCEPTED, {"body": {}})}),
            (
                "an_answer_with_an_empty_invocation_id",
                {"return_value": _batch_answer(ACCEPTED, {"status": 202, "body": {"invocation_id": ""}})},
            ),
            (
                "an_answer_with_a_replay_marker_that_is_not_a_bool",
                {
                    "return_value": _batch_answer(
                        ACCEPTED, {"status": 202, "body": {"invocation_id": "i-2", "replayed": "no"}}
                    )
                },
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
        sandbox_send = {"invocation_id": "sandbox-1", "sender_kind": "email_sandbox"}
        mock_post.return_value = MagicMock(status_code=202, json=lambda: sandbox_send)
        single = self._single("code", **sender)
        single_send = _forwarded(mock_post.call_args)["send"] if mock_post.called else None
        mock_post.return_value = _batch_answer(*[{"status": 202, "body": sandbox_send}] * 2)

        batch = self._batch("code", **sender)

        if single.status_code == status.HTTP_202_ACCEPTED:
            assert batch.status_code == status.HTTP_200_OK, batch.json()
            assert batch.json()["warnings"] == single.json()["warnings"]
            assert _forwarded(mock_post.call_args)["send"] == single_send
        else:
            assert (batch.status_code, batch.json()) == (single.status_code, single.json())

    def test_a_code_batch_with_no_recipient_it_can_address_creates_nothing(self, mock_post, _flag, _capture):
        workflows = HogFlow.objects.filter(team=self.team).count()

        response = self._batch("code", [{"to": "not an address", "idempotency_key": "key-0"}])

        assert response.status_code == status.HTTP_200_OK, response.json()
        assert [result["error"]["code"] for result in response.json()["results"]] == ["invalid_recipient"]
        assert not TransactionalEmailKey.objects.for_team(self.team.id, canonical=True).exists()
        assert HogFlow.objects.filter(team=self.team).count() == workflows
        mock_post.assert_not_called()

    @patch("posthog.rate_limit.team_is_allowed_to_bypass_throttle", return_value=False)
    @patch("posthog.rate_limit.is_rate_limit_enabled", return_value=True)
    def test_a_batch_while_its_flag_is_off_spends_no_send(self, _enabled, _bypass, mock_post, flag, _capture):
        flag.side_effect = lambda key, *_args, **_kwargs: key != EMAIL_BATCH_FLAG
        mock_post.return_value = MagicMock(status_code=202, json=lambda: {"invocation_id": "single"})

        with patch.object(TransactionalEmailTeamBurstThrottle, "rate", "1/minute"):
            hidden = self._batch("template")
            single = self._single("template")
            hidden_past_the_budget = self._batch("template")

        assert hidden.status_code == status.HTTP_404_NOT_FOUND
        assert single.status_code == status.HTTP_202_ACCEPTED, single.json()
        assert hidden_past_the_budget.status_code == status.HTTP_404_NOT_FOUND

    @parameterized.expand(
        [
            ("batch_flag_off", "template", {TRANSACTIONAL_EMAIL_API_FLAG, EMAIL_FROM_CODE_FLAG}),
            ("batch_flag_off_form_encoded", "form", {TRANSACTIONAL_EMAIL_API_FLAG, EMAIL_FROM_CODE_FLAG}),
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
            elif form == "huge":
                response = self._batch("template", message_data={"blob": "x" * 2 * 1024 * 1024})
            else:
                response = self._batch(form)

        assert response.status_code == status.HTTP_404_NOT_FOUND
        mock_post.assert_not_called()
