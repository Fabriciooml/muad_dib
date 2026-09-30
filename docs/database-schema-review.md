# Database schema review — 2026-09-30

Status: design validated against V1 workflows; final session identity index waits for Cineart showtime ID inspection. No migration or database exists yet.

Wire output contract lives in [`schemas/output.v1.schema.json`](../schemas/output.v1.schema.json): session fields, preview GET response, and Kafka discovery event. PostgreSQL table schema below is separate from that JSON contract.

## Tables and invariants

| Table | Key columns | Constraints and indexes |
| --- | --- | --- |
| `watches` | `id UUID`; submitted `movie_title`; nullable `providers TEXT[]`; optional `city`, `cinema`, `room`; `enabled`; `poll_interval_minutes`; `next_poll_at TIMESTAMPTZ`; `revision BIGINT`; timestamps | Interval defaults to 15 and stays > 0; revision >= 1; providers null or nonempty; partial index on `next_poll_at WHERE enabled` |
| `watch_provider_targets` | `(watch_id, provider)`; nullable `movie_id`; `resolution_status`; last resolution attempt, last fetch attempt, last successful fetch, last fetch error | Watch FK cascades on delete; `resolved` requires movie ID; pending statuses require null movie ID; status limited to `resolved`, `pending_not_found`, `pending_source_error`, `pending_ambiguous` |
| `sessions` | `id UUID`; provider and movie/cinema IDs; city/cinema/room display names; normalized `room_key`; date; minute-precision local time; timezone; format; language; `purchase_url`; `first_seen_at` | Unique identity by provider; current fallback: `(provider, movie_id, cinema_id, room_key, session_date, session_time)` |
| `outbox` | `id UUID`; `session_id`; topic; Kafka key; immutable JSON payload; created/published timestamps; attempts; last error | Session FK; `attempts >= 0`; unique `(session_id, topic)`; partial index on `(created_at, id) WHERE published_at IS NULL` |

`providers = NULL` means every current and future registered provider. Explicit nonempty array fixes selection. No watch-to-session join table: session discovery is global, so overlapping watches do not create duplicate events.

## Transaction checks

1. Create or replace watch and its provider targets together. Replacement increments revision and makes watch due immediately.
2. After source requests, poll transaction locks due watch rows and checks revision. Only still-current, enabled watches contribute matching sessions. Insert new sessions and outbox records, update targets, and reschedule in same transaction.
3. Concurrent inserts of same session conflict on identity; only successful first insert creates outbox record.
4. Purchase URL or display metadata update changes existing session row. Discovery payload stays as originally observed.
5. Publisher marks outbox row published only after Kafka acknowledgement. Retry may redeliver same event ID.
6. Preview GETs may read sessions for newness comparison; they write no rows.

## Identity gate before migration

Inspect saved Cineart payload for durable showtime ID, including behavior when purchase URL changes. If ID is stable, add nullable `source_session_id` and partial unique `(provider, source_session_id)` index for sessions with IDs; keep partial composite unique index for sessions without IDs. Keep one identity mode per provider to avoid duplicate discovery when an ID appears later. If no stable ID exists, use composite index above. Room casing/outer whitespace changes normalize to same `room_key`; genuine room rename can still look new under composite fallback.
