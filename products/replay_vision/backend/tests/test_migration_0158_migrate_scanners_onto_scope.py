from typing import Any

from posthog.test.base import TestMigrations

_MODEL = "gemini-3.8-flash"


class TestMigrateScannersOntoScope(TestMigrations):
    app = "replay_vision"
    migrate_from = "0157_drop_replayscanner_sampling_mode_column"
    migrate_to = "0158_migrate_scanners_onto_scope"

    def setUpBeforeMigration(self, apps: Any) -> None:
        ReplayScanner = apps.get_model("replay_vision", "ReplayScanner")

        created = 0

        def scanner(**fields):
            nonlocal created
            created += 1
            # Named apart because a team may hold one configured scanner per name.
            defaults = {
                "team_id": self.team.id,
                "name": f"s{created}",
                "model": _MODEL,
                "scanner_config": {"prompt": "p"},
            }
            return ReplayScanner.objects.create(**{**defaults, **fields}).id

        self.native = scanner(
            scanner_type="experiment",
            scanner_config={"prompt": "p", "length": "medium", "experiment_id": 11, "variants": ["control", "test"]},
        )
        self.native_proportional = scanner(
            scanner_type="experiment",
            scanner_config={"prompt": "p", "experiment_id": 12, "variants": ["control"], "balance_variants": False},
        )
        self.native_every_variant = scanner(
            scanner_type="experiment",
            scanner_config={"prompt": "p", "experiment_id": 13, "variants": None},
        )
        self.legacy_single = scanner(
            scanner_type="summarizer",
            experiment_targeting={"experiment_id": 21, "variant": "test"},
        )
        self.legacy_several = scanner(
            scanner_type="classifier",
            scanner_config={"prompt": "p", "tags": ["a"]},
            experiment_targeting={"experiment_id": 22, "variants": ["control", "test"]},
        )
        self.unscoped = scanner(scanner_type="monitor")

    def test_a_native_scanner_becomes_a_summarizer_carrying_its_experiment_in_scope(self) -> None:
        ReplayScanner = self.apps.get_model("replay_vision", "ReplayScanner")  # type: ignore[union-attr]
        row = ReplayScanner.objects.get(id=self.native)

        assert row.scanner_type == "summarizer"
        assert row.scope["experiment_id"] == 11
        assert row.scope["variants"] == ["control", "test"]
        assert row.scope["variants_source"] == "all_at_creation"
        assert row.scanner_config == {"prompt": "p", "length": "medium"}

    def test_a_scanner_that_samples_proportionally_keeps_doing_so(self) -> None:
        ReplayScanner = self.apps.get_model("replay_vision", "ReplayScanner")  # type: ignore[union-attr]
        row = ReplayScanner.objects.get(id=self.native_proportional)

        assert row.scope["balance"] is False
        assert "balance_variants" not in row.scanner_config

    def test_a_default_balance_stays_out_of_the_migrated_scope(self) -> None:
        ReplayScanner = self.apps.get_model("replay_vision", "ReplayScanner")  # type: ignore[union-attr]
        row = ReplayScanner.objects.get(id=self.native)

        assert "balance" not in row.scope
        assert "start_on_launch" not in row.scope

    def test_watching_every_variant_stays_unnarrowed(self) -> None:
        ReplayScanner = self.apps.get_model("replay_vision", "ReplayScanner")  # type: ignore[union-attr]
        row = ReplayScanner.objects.get(id=self.native_every_variant)

        assert "variants" not in row.scope
        assert row.scope["variants_source"] == "all_at_creation"

    def test_a_legacy_singular_variant_becomes_a_one_item_list(self) -> None:
        ReplayScanner = self.apps.get_model("replay_vision", "ReplayScanner")  # type: ignore[union-attr]
        row = ReplayScanner.objects.get(id=self.legacy_single)

        assert row.scope["experiment_id"] == 21
        assert row.scope["variants"] == ["test"]
        assert row.scanner_type == "summarizer"

    def test_a_legacy_column_scanner_keeps_its_answer_format(self) -> None:
        ReplayScanner = self.apps.get_model("replay_vision", "ReplayScanner")  # type: ignore[union-attr]
        row = ReplayScanner.objects.get(id=self.legacy_several)

        assert row.scanner_type == "classifier"
        assert row.scope["variants"] == ["control", "test"]
        assert row.scanner_config == {"prompt": "p", "tags": ["a"]}

    def test_a_scanner_watching_no_experiment_is_left_alone(self) -> None:
        ReplayScanner = self.apps.get_model("replay_vision", "ReplayScanner")  # type: ignore[union-attr]
        row = ReplayScanner.objects.get(id=self.unscoped)

        assert row.scope is None
        assert row.scanner_type == "monitor"

    def test_every_migrated_row_is_stamped(self) -> None:
        ReplayScanner = self.apps.get_model("replay_vision", "ReplayScanner")  # type: ignore[union-attr]
        migrated = [self.native, self.native_proportional, self.native_every_variant, self.legacy_single]

        for scanner_id in migrated:
            assert ReplayScanner.objects.get(id=scanner_id).scope["migrated_at"]


class TestRestoreScannersFromScope(TestMigrations):
    """The backward step, which is the rollback path if the forward one has to be undone."""

    app = "replay_vision"
    migrate_from = "0158_migrate_scanners_onto_scope"
    migrate_to = "0157_drop_replayscanner_sampling_mode_column"

    def setUpBeforeMigration(self, apps: Any) -> None:
        ReplayScanner = apps.get_model("replay_vision", "ReplayScanner")
        stamp = "2026-11-18T00:00:00+00:00"

        def scanner(**fields):
            defaults = {"team_id": self.team.id, "model": _MODEL}
            return ReplayScanner.objects.create(**{**defaults, **fields}).id

        self.from_native = scanner(
            name="n1",
            scanner_type="summarizer",
            scanner_config={"prompt": "p", "length": "medium"},
            scope={
                "kind": "experiment",
                "experiment_id": 11,
                "variants": ["control", "test"],
                "variants_source": "all_at_creation",
                "migrated_at": stamp,
            },
        )
        self.from_native_proportional = scanner(
            name="n2",
            scanner_type="summarizer",
            scanner_config={"prompt": "p"},
            scope={
                "kind": "experiment",
                "experiment_id": 12,
                "variants": ["control"],
                "balance": False,
                "migrated_at": stamp,
            },
        )
        self.from_legacy_single = scanner(
            name="n3",
            scanner_type="classifier",
            scanner_config={"prompt": "p", "tags": ["a"]},
            scope={"kind": "experiment", "experiment_id": 21, "variants": ["test"], "migrated_at": stamp},
        )
        self.unmigrated = scanner(
            name="n4",
            scanner_type="summarizer",
            scanner_config={"prompt": "p"},
            experiment_targeting={"experiment_id": 31},
            scope={"kind": "experiment", "experiment_id": 31},
        )

    def test_a_native_scanner_gets_its_type_and_config_back(self) -> None:
        ReplayScanner = self.apps.get_model("replay_vision", "ReplayScanner")  # type: ignore[union-attr]
        row = ReplayScanner.objects.get(id=self.from_native)

        assert row.scanner_type == "experiment"
        assert row.scanner_config["experiment_id"] == 11
        assert row.scanner_config["variants"] == ["control", "test"]
        assert row.scanner_config["prompt"] == "p"
        assert row.scope is None

    def test_proportional_sampling_survives_the_round_trip(self) -> None:
        ReplayScanner = self.apps.get_model("replay_vision", "ReplayScanner")  # type: ignore[union-attr]
        row = ReplayScanner.objects.get(id=self.from_native_proportional)

        assert row.scanner_config["balance_variants"] is False

    def test_a_column_targeted_scanner_gets_its_column_back(self) -> None:
        ReplayScanner = self.apps.get_model("replay_vision", "ReplayScanner")  # type: ignore[union-attr]
        row = ReplayScanner.objects.get(id=self.from_legacy_single)

        assert row.scanner_type == "classifier"
        assert row.experiment_targeting["experiment_id"] == 21
        assert row.experiment_targeting["variant"] == "test"
        assert row.scope is None

    def test_a_scope_this_migration_did_not_write_is_left_alone(self) -> None:
        ReplayScanner = self.apps.get_model("replay_vision", "ReplayScanner")  # type: ignore[union-attr]
        row = ReplayScanner.objects.get(id=self.unmigrated)

        assert row.scope == {"kind": "experiment", "experiment_id": 31}
        assert row.scanner_type == "summarizer"
