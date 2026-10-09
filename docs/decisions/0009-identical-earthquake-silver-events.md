# 0009: Identical earthquake events in Silver

- **Status:** Accepted
- **Date:** 2026-10-09

## Context

The owner requested Silver duplicate cleanup after the dev audit found 74 repeated
PHIVOLCS event keys and 79 extra observations, with identical event attributes.
Current Bronze is a replacement snapshot, while Silver contains historical events.

## Decision

Keep one Silver row per existing deterministic `id`. Combine valid current Bronze
observations with retained Silver history, then choose the latest `_ingested_at`,
`_batch_id`, `_row_hash` in descending order. Collapse only identical time,
coordinates, depth, magnitude and location attributes. Conflicting attributes for
a key block the replacement, are recorded in monitoring, and require review.
Bronze is unchanged. Retain historical events absent from current Bronze.

## Why

This removes repeat observations without inventing a new event key, losing history,
changing measures or picking arbitrarily between different events. The explicit
Silver policy enables Gold key uniqueness and prevents analytic double counting.

## Alternatives considered

- **Deduplicate Gold only:** the owner requested cleanup in Silver instead.
- **Delete events absent from current Bronze:** would discard retained history.
- **Choose a row for conflicting attributes:** would conceal an unresolved collision.

## Consequences

Silver publishes an atomic validated snapshot. Reruns retain one row per event key;
lineage comes from the selected observation and all available Bronze remains raw.
Monitoring checks current valid Bronze key coverage and warns about historical
Silver events outside that snapshot, rather than requiring snapshot row equality.
Gold still requires unique event keys before spatial matching. This supersedes the
PHIVOLCS dictionary's earlier no-deduplication limitation only for identical events.

## Open

Restore or retain historical Bronze evidence for Silver events absent from the
current snapshot. Confirm the source timezone and review future key conflicts.
