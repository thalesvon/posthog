"""Which events table a batch export reads.

This module must stay importable by both the API layer and the Temporal worker, so it
should not import from `products.batch_exports.backend.temporal` or any DRF code.
"""

import contextlib
import collections.abc

from django.conf import settings

from posthog.schema import HogQLQueryModifiers

from posthog.models.event.new_events_schema import events_route_scope


def batch_exports_read_new_events_table(export_opted_in: bool) -> bool:
    """Whether a batch export reads events_json instead of the legacy events table.

    The switches that move the rest of PostHog (CLICKHOUSE_HOGQL_USE_NEW_EVENTS_SCHEMA,
    CLICKHOUSE_HOGQL_USE_NEW_EVENTS_SCHEMA_TEAMS, the new-events-table-rollout flag and a project's opt-in) do not
    move a batch export, because the rows an export writes to a customer's destination must change only when the
    batch exports team moves it.

    `export_opted_in` is the `use_new_events_schema` key of the export's destination config. A user sets it to move
    one export early, for example a test export next to a production one. It can only move an export to events_json,
    so a switch that moves a whole project must be checked here before it.
    """
    # The test suites that run on both tables turn on the new one with this Django setting.
    return export_opted_in or (settings.TEST and settings.CLICKHOUSE_HOGQL_USE_NEW_EVENTS_SCHEMA)


@contextlib.asynccontextmanager
async def route_to_events_table_cluster(reads_new_events_table: bool) -> collections.abc.AsyncIterator[None]:
    """A ClickHouse client opened inside connects to the cluster that stores the table the export reads.

    Without it, the client follows the project's table, which a batch export does not follow. It is async so that it
    can share one `async with` statement with the client.
    """
    with events_route_scope(HogQLQueryModifiers(useNewEventsSchema=reads_new_events_table)):
        yield
