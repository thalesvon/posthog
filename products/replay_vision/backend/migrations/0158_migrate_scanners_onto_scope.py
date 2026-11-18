from django.db import migrations, transaction
from django.utils import timezone

_BATCH_SIZE = 500

# Copied rather than imported: a migration must keep reading the shape the rows held when it was
# written, and the live model's choices move with the product. `experiment` is the type this
# migration retires; `summarizer` is what a migrated row becomes, because the two share an answer
# format and the scope now carries the experiment the type used to imply.
_EXPERIMENT = "experiment"
_SUMMARIZER = "summarizer"

# The `scanner_config` keys that the scope replaces. Everything else a native experiment scanner
# holds (`prompt`, `length`) belongs to the answer format and stays.
_CONFIG_SCOPE_KEYS = ("experiment_id", "variants", "balance_variants", "start_on_launch")


def _scope_from_native_config(config: dict) -> dict | None:
    """The scope a native experiment scanner's `scanner_config` becomes, or None if it names none."""
    experiment_id = config.get("experiment_id")
    if experiment_id is None:
        return None
    scope: dict = {"kind": "experiment", "experiment_id": experiment_id}
    variants = config.get("variants")
    if variants:
        scope["variants"] = list(variants)
    # `variants: null` means every variant to every reader, so it is carried through rather than
    # expanded here. Expanding it would mean reading each experiment's variant list across an app
    # boundary, and the rule-linked path raises on a rule it cannot read, which would stop the
    # whole deploy on one row. A later backfill fills explicit lists once it can fail per row.
    scope["variants_source"] = "all_at_creation"
    # Both are omitted at their default, because the mirror in `ReplayScanner.save()` decides
    # whether it owns a row's scope by comparing the stored blob against a fresh derivation of it.
    # Writing a defaulted key here would make the mirror read a migrated row as one set by hand.
    if config.get("balance_variants") is False:
        scope["balance"] = False
    if config.get("start_on_launch") is True:
        scope["start_on_launch"] = True
    return scope


def _scope_from_targeting(targeting: dict) -> dict | None:
    """The scope a legacy `experiment_targeting` column becomes, or None if it names no experiment."""
    experiment_id = targeting.get("experiment_id")
    if experiment_id is None:
        return None
    scope: dict = {"kind": "experiment", "experiment_id": experiment_id}
    # The legacy column holds a singular `variant`; the scope shape has only the plural, so a
    # single watched variant becomes a one-item list. This is the same fold `stored_scope` does.
    variants = targeting.get("variants") or ([targeting["variant"]] if targeting.get("variant") else None)
    if variants:
        scope["variants"] = list(variants)
    # A column-targeted scanner was never offered a variant picker, so whatever it watches is what
    # the experiment had. `balance` is left at its default: these rows carry no balance setting,
    # and the default matches what the sampler already does for them.
    scope["variants_source"] = "all_at_creation"
    return scope


def migrate_scanners_onto_scope(apps, schema_editor):
    ReplayScanner = apps.get_model("replay_vision", "ReplayScanner")
    now = timezone.now()
    last_pk = None
    while True:
        rows = ReplayScanner.objects.order_by("pk")
        if last_pk is not None:
            rows = rows.filter(pk__gt=last_pk)
        batch = list(rows.only("id", "scanner_type", "scanner_config", "experiment_targeting", "scope")[:_BATCH_SIZE])
        if not batch:
            return
        last_pk = batch[-1].pk
        changed = []
        for scanner in batch:
            # Idempotent: a row that already carries a migrated scope is left exactly as it is, so a
            # retried or resumed run neither rewrites it nor moves `migrated_at`. Checked before any
            # derivation, so a re-run reads each migrated row once and computes nothing for it.
            if isinstance(scanner.scope, dict) and scanner.scope.get("migrated_at"):
                continue
            config = scanner.scanner_config if isinstance(scanner.scanner_config, dict) else {}
            targeting = scanner.experiment_targeting if isinstance(scanner.experiment_targeting, dict) else {}
            if scanner.scanner_type == _EXPERIMENT:
                scope = _scope_from_native_config(config)
                if scope is None:
                    # An experiment-type row naming no experiment cannot build a scanner either, so
                    # there is nothing to carry across. Left alone for a person to look at.
                    continue
                scanner.scanner_config = {k: v for k, v in config.items() if k not in _CONFIG_SCOPE_KEYS}
                scanner.scanner_type = _SUMMARIZER
            elif targeting:
                scope = _scope_from_targeting(targeting)
                if scope is None:
                    continue
            else:
                continue
            scanner.scope = {**scope, "migrated_at": now.isoformat()}
            changed.append(scanner)
        # Each batch commits alone, so a scanner edit or a sweep waits on at most one batch's locks.
        with transaction.atomic():
            ReplayScanner.objects.bulk_update(changed, ["scanner_type", "scanner_config", "scope"])


def restore_scanners_from_scope(apps, schema_editor):
    """Rebuild the older stores from `scope` for the rows this migration moved.

    Keyed on `scope.migrated_at`, so a scope written by hand or by the mirror is left alone.
    """
    ReplayScanner = apps.get_model("replay_vision", "ReplayScanner")
    last_pk = None
    while True:
        rows = ReplayScanner.objects.order_by("pk")
        if last_pk is not None:
            rows = rows.filter(pk__gt=last_pk)
        batch = list(rows.only("id", "scanner_type", "scanner_config", "experiment_targeting", "scope")[:_BATCH_SIZE])
        if not batch:
            return
        last_pk = batch[-1].pk
        changed = []
        for scanner in batch:
            scope = scanner.scope
            if not isinstance(scope, dict) or not scope.get("migrated_at"):
                continue
            config = scanner.scanner_config if isinstance(scanner.scanner_config, dict) else {}
            # A row whose `scanner_config` still holds the answer format but no experiment is one
            # this migration moved off the native type; everything else came from the column.
            if "prompt" in config and scanner.scanner_type == _SUMMARIZER:
                restored = dict(config)
                restored["experiment_id"] = scope["experiment_id"]
                restored["variants"] = scope.get("variants")
                if scope.get("balance") is False:
                    restored["balance_variants"] = False
                if scope.get("start_on_launch") is True:
                    restored["start_on_launch"] = True
                scanner.scanner_config = restored
                scanner.scanner_type = _EXPERIMENT
            else:
                variants = scope.get("variants")
                scanner.experiment_targeting = {
                    "experiment_id": scope["experiment_id"],
                    "variant": variants[0] if variants and len(variants) == 1 else None,
                    "variants": list(variants) if variants and len(variants) > 1 else None,
                }
            scanner.scope = None
            changed.append(scanner)
        with transaction.atomic():
            ReplayScanner.objects.bulk_update(
                changed, ["scanner_type", "scanner_config", "experiment_targeting", "scope"]
            )


class Migration(migrations.Migration):
    atomic = False

    dependencies = [
        ("replay_vision", "0157_drop_replayscanner_sampling_mode_column"),
    ]

    operations = [
        migrations.RunPython(migrate_scanners_onto_scope, restore_scanners_from_scope, elidable=False),
    ]
