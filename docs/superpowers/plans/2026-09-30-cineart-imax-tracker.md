# Configurable Cinema Session Tracker Implementation Plan

> **For implementers:** Execute tasks in order. For each behavior, write one failing test, run it and confirm the expected failure, add minimum code, run focused test, then full suite. Follow red → green; no production behavior before its test fails.

**Goal:** Poll configured movie/provider watches at their own intervals (default 15 minutes), persist newly listed sessions in PostgreSQL, and publish discoveries to Kafka. First watch: Cineart Duna - Parte Três (`23469`), Cineart Boulevard, Sala 06 IMAX.

**Architecture:** One `Provider` interface resolves movie titles to source IDs and returns normalized sessions; Cineart is V1 adapter. Shared `SessionInspector` handles provider selection, resolution, fetch, and matching for scheduler and preview GETs without writes. Scheduler reads due watches, asks inspector for results, persists new sessions plus outbox rows atomically, and updates targets/schedules. Separate publisher sends outbox events to Kafka. FastAPI exposes internal watch CRUD, previews, health, and Prometheus metrics. Portainer Docker Standalone deploys one prebuilt tracker image against existing production PostgreSQL and Kafka.

**Tech Stack:** Python 3.11+, `python-components==0.4.0`, `fastapi-component==0.3.0`, `kafka-component`, `prometheus-component`, `postgres-component` at verified Git commit pins in `pyproject.toml`/`uv.lock`, `SQLAlchemy` async/Core, `Alembic`, `httpx`, `uvicorn`, `pytest`, `pytest-asyncio`, `PyYAML`, `testcontainers`. `asyncpg` arrives through `postgres-component`.

**Spec:** `docs/superpowers/specs/2026-09-30-cineart-imax-tracker-design.md`; output contract: `schemas/output.v1.schema.json`.

## Global constraints

- Provider names and source IDs are strings; V1 registry contains `cineart` only. Every request accepting provider selection uses optional `providers`: omitted means all registered providers, including future adapters for persisted watches; explicit nonempty list is fixed. Unknown/empty lists return 422.
- Use `PostgresComponent` for database engine/session lifecycle and readiness; store owns domain queries. App owns Alembic revisions; component never migrates schema at startup. `DATABASE_URL` uses `postgresql+asyncpg://`.
- Clients submit `movie_title`, never `movie_id`; each selected adapter resolves its own source ID. Watch persists title, nullable provider selection, and per-provider targets. Watch creation succeeds with all providers pending when film is absent or sources fail; pending targets retry on later polls. Ambiguous title blocks watch creation/replacement.
- Cineart URL is `https://cineart.com.br/filme/{movie_id}`. Session source fields include city, cinema ID/name, room, date, time, format, language, and purchase URL.
- Initial watch is created through CRUD after deployment, not hardcoded in application. First successful poll after creation emits sessions not already stored globally. Overlapping watches do not emit duplicates.
- Topic is `cinema.sessions.discovered.v1`; each event includes `provider`, provider-scoped IDs, stable `event_id`, and `schema_version: 1`. Delivery is at least once.
- One Portainer replica, one Uvicorn worker. Scheduler checks due watches at startup and every 30 seconds; each watch defaults to a 15-minute interval. No overlapping polls. CRUD changes take effect within 30 seconds without redeployment.
- Watch CRUD requires `X-Admin-Token` matching `ADMIN_API_KEY`. Default Portainer host binding is `127.0.0.1:8000`; no public ingress or browser frontend in V1.
- Automated tests use saved Cineart HTML fixture. Live source request is manual smoke check only.

## Before coding and deployment

- Start TDD Tasks 1–2 now. Task 2 must capture a real Cineart HTML fixture and decide whether a durable source showtime ID exists before Task 3 defines session uniqueness. Do not assert a fixed live session count.
- Resolve and lock all five component package versions in the project environment early; local sibling repositories do not substitute for checking installable release artifacts. Validate `schemas/output.v1.schema.json` with a JSON Schema 2020-12 validator in output tests. Before Task 6, freeze watch CRUD response and error bodies in generated OpenAPI and contract tests; current formal schema covers session previews and Kafka events only.
- Before Portainer deployment, confirm production PostgreSQL role can run Alembic DDL, tracker role can read/write its tables, Kafka topic `cinema.sessions.discovered.v1` exists with producer write permission, and Stack supports migration completion dependency. Document verified values and results without committing secrets.

## Files and responsibilities

- `src/cinema_tracker/domain.py`: `Watch`, `Session`, shared `SessionFilter`, provider `Protocol`, and matching.
- `src/cinema_tracker/cineart.py`: Cineart catalog title resolution, session fetch, and parsing into generic `Session` values.
- `src/cinema_tracker/inspect.py`: shared provider selection, target resolution, live fetch, matching, and per-provider results; no database or Kafka writes.
- `src/cinema_tracker/storage.py`, `alembic/env.py`, `alembic/versions/001_init.py`, `alembic.ini`: watch/session/outbox persistence and app-owned migration.
- `src/cinema_tracker/poll.py`, `src/cinema_tracker/publish.py`: scheduling and outbox delivery.
- `src/cinema_tracker/app.py`, `src/cinema_tracker/settings.py`: component composition, protected CRUD and session previews, health, metrics.
- `Dockerfile`, `portainer-stack.yaml`, `compose.local.yaml`, `README.md`: deployment and operations.

## Review focus

- Empty or malformed Cineart response must not erase stored sessions or create false discoveries (Tasks 2, 5).
- Two watches for same provider/movie must cause one fetch; overlapping matches must create one event. One multi-provider watch must poll each selected provider independently (Task 5).
- Watches with different intervals must be independently due; restart must preserve `next_poll_at` (Tasks 3, 5).
- Same movie ID across providers must remain distinct; seconds-only time changes and purchase URL updates must not create discoveries. Latest purchase URL must persist (Task 3).
- Concurrent inserts must create one session/outbox pair; DB transaction failure must create neither (Task 3).
- Kafka acknowledgement followed by failure before marking published may replay same event ID, never lose it (Task 4).
- Preview routes must share watch matching, query all selected providers, expose per-provider statuses, leave every table and Kafka unchanged, and compare new sessions against global persisted identity (Tasks 1, 3, 6).

---

### Task 1: Generic domain and watch matching

**Files:** Create `pyproject.toml`, `.python-version`, `src/cinema_tracker/__init__.py`, `src/cinema_tracker/domain.py`, `tests/test_domain.py`.

**Interfaces:** `Watch(id: UUID, movie_title: str, providers: tuple[str, ...] | None, targets: tuple[ProviderTarget, ...], city: str | None, cinema: str | None, room: str | None, enabled: bool, poll_interval_minutes: int, next_poll_at: datetime, revision: int)`; `Session(provider: str, movie_id: str, city: str, cinema_id: str, cinema: str, room: str, date: date, time: time, timezone: str, format: str, language: str, purchase_url: str)`; `SessionKey(provider, movie_id, cinema_id, room_key, date, minute)`; `ProviderTarget(provider: str, movie_id: str | None, resolution_status: str, last_resolution_at: datetime | None, last_fetch_attempt_at: datetime | None, last_successful_fetch_at: datetime | None, last_fetch_error: str | None)`; `SessionFilter(provider: str, movie_id: str, city: str | None, cinema: str | None, room: str | None)` with `matches(session: Session) -> bool`; `MovieMatch(title: str, movie_id: str, source_url: str)`; `Provider(Protocol)` with `async resolve_movie(title: str) -> MovieMatch` and `async fetch_sessions(movie_id: str) -> list[Session]`. `Watch.matches` checks `enabled` and delegates to a `SessionFilter` built from the matching resolved provider target.

- [ ] Create package metadata with pinned five component versions, Python `>=3.11`, runtime/test dependencies, `src` layout, and `uv run pytest` entry. Initialize Git before first implementation commit if `cinema_tracker` remains standalone repository.
- [ ] RED: one watch with an explicit Cineart target, city ` Belo Horizonte `, cinema ` Cineart Boulevard `, room `Sala 06 IMAX` matches same provider/movie/session with city/cinema/room case and outer whitespace differences. `uv run pytest tests/test_domain.py -q` must fail because domain interface is missing.
- [ ] GREEN: immutable dataclasses and `Provider` protocol; shared `SessionFilter.matches` compares provider/movie exactly and optional city/cinema/room names with `strip().casefold()`. `Watch.matches` reuses filter behavior for each resolved provider target. Run focused test and full `uv run pytest -q`.
- [ ] RED/GREEN: omitted location fields match all; wrong provider/movie/city/cinema/room fails; disabled watch never matches. Pure `select_providers(requested, registry_names)` expands `None` to current registry and restricts explicit tuple. Preview filter produces same matches as an enabled watch with identical fields. Commit `feat: define cinema watch domain`.

### Task 2: Cineart provider adapter and shared inspector

**Files:** Create `src/cinema_tracker/cineart.py`, `src/cinema_tracker/inspect.py`, `tests/fixtures/duna_23469.html`, `tests/test_cineart.py`, `tests/test_inspect.py`.

**Interface:** `CineartProvider(client: httpx.AsyncClient)` implements `Provider.resolve_movie(title: str)` and `Provider.fetch_sessions(movie_id: str)`; `parse_cineart_catalog(page: str) -> list[MovieMatch]`; `parse_cineart_sessions(page: str, movie_id: str) -> list[Session]`. Adapter never applies watch filters.

- [ ] Save minimal representative HTML fixture containing escaped `filme-prog :cinemas` JSON: Cineart Boulevard ID `25`, room `Sala 06 IMAX`, six Duna times across Dec 15–16, and at least one other cinema/room/city. Preserve observed `HORARIO` seconds variation and `URL_COMPRA`.
- [ ] RED/GREEN: catalog fixtures for `/em-cartaz`, `/estreias`, and `/em-breve` include movie cards with titles and `/filme/{id}` links. Exact normalized `Duna - Parte Três` resolves to `23469` from `/em-breve`. Case, accents, and outer whitespace normalize; unrelated or fuzzy titles never auto-match. Missing title raises not-found; repeated listings with the same ID dedupe; duplicate exact title with distinct IDs raises ambiguous with candidates; HTTP/parse failure raises source error. Test with `httpx.MockTransport` and saved HTML, no live network.
- [ ] RED: parsing fixture yields all sessions across locations, including six Boulevard IMAX sessions; first matching value has provider `cineart`, movie ID `23469`, cinema ID `25`, `time(14, 0)`, timezone `America/Sao_Paulo`. Run focused test; expect missing adapter failure.
- [ ] GREEN: extract attribute, `html.unescape`, `json.loads`; normalize time to minute and source IDs to strings. Include city/cinema/room/format/language/link. Sort results for deterministic tests. Run focused and full suite.
- [ ] RED/GREEN: explicit unavailable-page message returns `[]`; missing attribute without that message and malformed JSON raise `CineartParseError`; `fetch_sessions` uses 10-second HTTP timeout and raises on non-2xx. Test transport with `httpx.MockTransport`, no live network.
- [ ] RED/GREEN: inspect Cineart payload for a durable showtime ID. Use it as session identity only if stable across purchase URL updates; otherwise normalize room key with `strip().casefold()` and use the documented composite identity. Test URL and casing changes produce no discovery.
- [ ] RED/GREEN: `SessionInspector.resolve_targets()` expands provider selection and returns resolved/pending outcomes without fetching sessions. `inspect()` groups `(provider, movie_id)` across watches, fetches each once, applies shared filters, and reports independent provider outcomes. Fake providers verify dynamic-all, partial failures, cached IDs, and preview matching. Inspector has no DB or Kafka write methods. Commit `feat: adapt Cineart and inspect sessions`.

### Task 3: PostgreSQL watches, sessions, and outbox

**Files:** Create `alembic.ini`, `alembic/env.py`, `alembic/versions/001_init.py`, `src/cinema_tracker/storage.py`, `tests/conftest.py`, `tests/test_storage.py`; package Alembic files in image.

**Interfaces:** `PostgresStore(database: PostgresComponent)` owns `create_watch`, `list_watches`, `get_watch`, `replace_watch`, `delete_watch`, `list_due_watches(now: datetime) -> list[Watch]`, `upsert_provider_target(watch_id, target)`, `apply_poll_results(results, expected_revisions, completed_at)`, `insert_discoveries(sessions, observed_at) -> int`, `existing_session_keys(keys: Sequence[SessionKey]) -> set[SessionKey]`, `pending_events(limit)`, `mark_published(event_id)`, `record_publish_failure(event_id, error)`, `pending_count`, `ping`. It is a repository, not a lifecycle component. Use `async with database.session() as session` for each transaction. `OutboxEvent` has UUID `id`, `topic`, byte `key`, and JSON `payload`.

- [ ] RED: Docker-backed PostgreSQL test creates a watch with resolved and pending targets, reads it, replaces its filter/selection, disables it, and deletes it; list-enabled excludes disabled/deleted watches. Run `uv run pytest tests/test_storage.py -q`; expect missing store/migration failure.
- [ ] RED/GREEN: with `PostgresComponent` started against test PostgreSQL, `database.session()` commits a session plus outbox row together and rolls both back on injected failure; failed startup against unavailable PostgreSQL raises. Verify app-owned Alembic upgrade can run before component startup.
- [ ] GREEN: app-owned Alembic revision creates `watches` (UUID ID, submitted movie title, nullable provider-name array for dynamic-all versus explicit selection (`NULL` or nonempty; database check), nullable city/cinema/room name filters, enabled, `poll_interval_minutes INTEGER NOT NULL DEFAULT 15 CHECK (poll_interval_minutes > 0)`, `next_poll_at TIMESTAMPTZ NOT NULL`, `revision BIGINT NOT NULL DEFAULT 1 CHECK (revision >= 1)`, timestamps; partial due index on `next_poll_at WHERE enabled`), `watch_provider_targets` (primary key `(watch_id, provider)`, cascading watch FK, nullable resolved movie ID, checked resolution status consistent with ID presence, last resolution attempt, last fetch attempt, last successful fetch, last fetch error), `sessions` (UUID primary key, provider-scoped movie/cinema IDs, city, display room name plus normalized room key, date, minute-precision time, format/language/purchase URL, first-seen), and `outbox` (UUID ID, session FK, topic, key, immutable JSONB payload, created/published timestamps, attempts, last error; partial pending index on `(created_at, id) WHERE published_at IS NULL`). `alembic/env.py` uses `PostgresComponent(url=DATABASE_URL).migration_wiring()` with `create_migration_engine()` and Alembic async `run_sync`. Implement watch operations with parameterized SQLAlchemy Core. Run `alembic upgrade head` twice against test database and focused/full suite.
- [ ] RED/GREEN: create without interval stores `15` and `next_poll_at` at creation time; provider selection `NULL` persists dynamic all, while explicit list persists fixed names; unresolved target status survives restart; custom positive interval persists; zero/negative interval is rejected; replace resets `next_poll_at` to replacement time. `list_due_watches(now)` returns only enabled rows with `next_poll_at <= now`; replacement/disable increments `revision`; successful poll result applies only at the inspected revision and sets `next_poll_at` to `completed_at + its own interval`. Test with real PostgreSQL.
- [ ] RED/GREEN: `upsert_provider_target` adds target for adapter introduced after dynamic-all watch creation, updates pending target to resolved ID on later title match, and never creates target for adapter outside explicit selection. Replacing title clears stale resolved IDs; deleting watch cascades target rows but preserves discovered sessions.
- [ ] RED: insert one normalized session, expect one row with its exact `purchase_url` and one outbox row containing same URL; same session repeated or purchase URL changed yields zero new events; same movie ID from another provider yields separate session/event. Run focused test; expect failure before insert implementation.
- [ ] GREEN: add `sessions.purchase_url TEXT NOT NULL` to Alembic revision. Use unique composite session key `(provider, movie_id, cinema_id, room_key, session_date, session_time)`; Task 2 confirmed Cineart payload has no durable showtime ID. Add unique outbox `(session_id, topic)`. Within one `database.session()` transaction, `INSERT ... ON CONFLICT DO NOTHING RETURNING id`; create outbox only for new ID. On conflict, update `purchase_url` and other mutable metadata while leaving existing outbox payload untouched. Event topic `cinema.sessions.discovered.v1`; payload matches spec and UTC observation time. Run focused/full suite.
- [ ] RED/GREEN: when Cineart changes `URL_COMPRA` for same session, read stored row and assert new `purchase_url`; assert event count unchanged and original discovery event still contains original URL.
- [ ] RED/GREEN: read-only `existing_session_keys` returns only matching persisted identities for a batch of live candidates, including rows inserted by another watch; no table changes occur. Compare minute-normalized identity, not purchase URL.
- [ ] RED/GREEN: concurrent same-session inserts produce one row/event; concurrent watch replacement/disable prevents stale poll results and schedule overwrite; induced outbox failure rolls back insert; `mark_published` removes pending record; publish failure preserves it and increments attempts. Commit `feat: persist watches and session outbox`.

### Task 4: Kafka outbox publisher

**Files:** Create `src/cinema_tracker/publish.py`, `tests/test_publish.py`.

**Interface:** `OutboxPublisher(Component)` takes store, `KafkaProducerComponent`, and `Metrics`; `async publish_once(limit: int = 100) -> int`; `async start()/shutdown()` manage 5-second retry loop.

- [ ] RED: pending row sends topic `cinema.sessions.discovered.v1`, stable session key, JSON payload conforming to `schemas/output.v1.schema.json`, then marks published. Run `uv run pytest tests/test_publish.py -q`; expect missing publisher failure.
- [ ] GREEN: call `producer.send(topic, payload, key=key)` and await acknowledgement before mark. Process oldest pending first. Run focused/full suite.
- [ ] RED/GREEN: producer failure leaves event pending with attempt count; retry sends same `event_id`; failure after send but before mark can replay same event. Integration test with Testcontainers Kafka consumes and validates one event. Commit `feat: publish discoveries from outbox`.

### Task 5: Scheduler over configurable watches

**Files:** Create `src/cinema_tracker/poll.py`, `tests/test_poll.py`.

**Interface:** `SessionPoller(Component)` takes store, `dict[str, Provider]`, metrics, and `check_interval_seconds=30`; `async poll_once(now: datetime) -> int` fetches due watches only; `async start()/shutdown()` check immediately, then every 30 seconds without overlap.

- [ ] RED: two due watches for Cineart movie `23469` with different cinemas cause one provider fetch; filter selects union; overlap creates one discovery. A third watch for same movie with future `next_poll_at` does not participate. Run `uv run pytest tests/test_poll.py -q`; expect missing poller failure.
- [ ] RED: one due watch with `providers=None` and two fake adapters resolves/fetches both; one explicit `providers=("cineart",)` watch fetches only Cineart. Add third adapter after watch creation; dynamic-all watch picks it up on next due poll. One provider without movie remains pending and is retried later; resolved provider continues delivering. Run focused test; expect failure.
- [ ] GREEN: load due watches, call shared inspector for provider expansion, pending title resolution, grouped fetch, matching, and per-provider results. Persist target outcomes, new sessions/outbox rows, and reschedule each watch after all its provider attempts finish. Lock due watch rows in one transaction, compare stored revisions with inspected revisions, then recompute the session union from matching enabled watches, insert discoveries/outbox rows, update their target states, and reschedule them before commit. Concurrent PUT/disable/delete cannot publish stale matches. Existing resolved IDs do not require catalog lookup on each poll. Run focused/full suite.
- [ ] RED/GREEN: 5-minute and 15-minute watches become due independently; adding/disabling/deleting/replacing watch affects next 30-second check; two different movies fetch separately; one provider fetch failure records target error and does not suppress another; database write failure leaves affected watch due for next check; invalid/empty provider response never deletes stored sessions. Restart against same database honors provider targets and persisted `next_poll_at`. Fake short check interval proves startup check, no overlap, and clean shutdown. Commit `feat: schedule per-watch polling`.

### Task 6: FastAPI CRUD, read-only previews, lifecycle, and metrics

**Files:** Create `src/cinema_tracker/settings.py`, `src/cinema_tracker/app.py`, `tests/test_app.py`.

**Interface:** `build_app(settings: Settings, *, providers: dict[str, Provider] | None = None) -> FastAPI`; `create_application() -> FastAPI` loads env for Uvicorn `--factory`. CRUD: `GET /watches`, `GET /watches/{id}`, `POST /watches` (201), `PUT /watches/{id}` (200), `DELETE /watches/{id}` (204). POST body requires `movie_title`; optional `providers: list[str]` selects exact adapters, while omission persists dynamic all. Optional `city`, `cinema`, `room`, `enabled=true`, `poll_interval_minutes=15`; provider movie IDs are read-only. PUT requires full editable watch definition including title and interval; unchanged title/selection retains resolved targets. Responses include selection (`null` for all), per-provider target statuses/IDs, interval, and `next_poll_at`. Watch creation/replacement persists pending targets even when no provider resolves yet or source fetch fails. Any ambiguous match returns 409 without mutation. Successful POST returns 201 and PUT returns 200 with per-provider statuses. Unknown/empty provider list, blank required strings or supplied filters, or nonpositive/noninteger interval returns 422. All CRUD routes require `X-Admin-Token`; missing/wrong token returns 401.

**Preview interface:** `GET /sessions/preview` and `GET /sessions/preview/new` require `movie_title` and accept repeatable optional `providers` query parameters (`?providers=cineart&providers=cinemark`); omission uses all currently registered adapters. Optional `city`, `cinema`, `room`; all require `X-Admin-Token`. Both resolve title in each selected provider, fetch available live sessions, and apply shared `SessionFilter`; `/new` removes identities returned by `store.existing_session_keys`. Response includes requested selection, per-provider statuses/IDs, `partial`, applied filters, UTC `checked_at`, total `count`, and sorted provider-tagged `sessions` with `purchase_url`. Return `Cache-Control: no-store`. No watch/session/outbox writes, rescheduling, or Kafka sends. Successful providers return even when another has no movie, ambiguity, or failure; if none succeeds, all-not-found uses 404, ambiguity 409, source failure 503. Invalid input uses 422. Newness is a point-in-time comparison against global persisted sessions, not a reservation.

- [ ] RED: FastAPI lifespan test POSTs `{"providers":["cineart"],"movie_title":"Duna - Parte Três","cinema":"Cineart Boulevard","room":"Sala 06 IMAX"}` without interval, reads back Cineart target `movie_id: "23469"`, `poll_interval_minutes: 15`, and due `next_poll_at`, replaces with custom interval, then deletes. Omitted `providers` stores `null` selection; explicit list stores names. Unauthorized returns 401; unknown/empty providers, blank title/cinema, and zero interval return 422; all-pending returns 201, ambiguity 409. Run `uv run pytest tests/test_app.py -q`; expect missing app/routes failure.
- [ ] RED/GREEN: inject two fake providers. Omitted `providers` attempts both; explicit list attempts only requested names; duplicate names attempt once. If one resolves and second is not found or temporarily fails, POST returns 201 with one resolved and one pending target; if none resolves, POST still returns 201 with all targets pending; ambiguous candidate in any selected provider returns 409 without creating a watch. GET watch exposes stored selection and each target status.
- [ ] RED: with zero watches, GET `/sessions/preview` with `movie_title` and omitted providers returns all six Cineart Boulevard IMAX sessions and purchase URLs; `/sessions/preview/new` returns same six. Explicit repeated provider query limits adapters; two fake providers contribute provider-tagged sessions and statuses. One missing/failed provider yields 200 with `partial: true` when another succeeds. Repeat calls and assert unchanged watch/session/outbox counts, schedule state, and Kafka send count. Seed two session rows (including one from a different watch), then `/new` returns four while all preview still returns six; same minute with changed purchase URL stays known. Omit city/cinema/room to broaden results. Unauthorized returns 401; all-not-found/ambiguous/source-failure-only and bad filters follow 404/409/503/422. Confirm `Cache-Control: no-store` and validate preview responses against `schemas/output.v1.schema.json`. Run focused test; expect missing routes.
- [ ] GREEN: both preview routes call shared inspector with no cached targets; `/new` also calls `store.existing_session_keys`. Serialize per-provider status without writes. Run focused/full suite.
- [ ] GREEN: `Settings` requires `DATABASE_URL`, `KAFKA_BOOTSTRAP_SERVERS`, `ADMIN_API_KEY` and accepts `PORT=8000`; compare admin token with `hmac.compare_digest`. Build provider registry with `CineartProvider` only; `python-components.System` owns `PostgresComponent`, producer, metrics, poller, and publisher. Inject `PostgresStore(database)` into poller, publisher, and CRUD; declare component dependencies so database starts before both background loops and shuts down after them. Use `fastapi_component.create_app(component_routes=False)` and explicit metrics/health/CRUD routers. Importing `app.py` alone must not load env. Run focused/full suite.
- [ ] RED/GREEN: `/health/live` returns 200; `/health/ready` reflects `PostgresComponent.get_status()` plus fresh DB ping and producer state; `/metrics` exposes `cinema_poll_total`, `cinema_sessions_discovered_total`, `cinema_outbox_pending`, last-success timestamp, and publish outcomes. `Metrics(namespace="cinema")`; instrument app once and refresh pending gauge after writes/sends. Lifespan starts dependencies before loops and stops loops before connections. Commit `feat: expose watch administration and metrics`.

### Task 7: Container and Portainer contract

**Files:** Create `Dockerfile`, `.dockerignore`, `portainer-stack.yaml`, `compose.local.yaml`, `README.md`, `tests/test_container_contract.py`.

- [ ] RED: YAML contract test expects only `migrate` and `tracker` services in production Stack; same `${TRACKER_IMAGE}` for both; tracker depends on successful migration; `ADMIN_API_KEY`, `DATABASE_URL`, and `KAFKA_BOOTSTRAP_SERVERS` come from Portainer variables; Docker healthcheck uses `/health/live`; one Uvicorn worker. Run focused test; expect missing files.
- [ ] GREEN: image based on `python:3.11-slim`, installs package and Alembic files, runs unprivileged, exposes port 8000. Stack uses prebuilt `${TRACKER_IMAGE:?set TRACKER_IMAGE}`; migration command `alembic upgrade head`; tracker command uses `uvicorn cinema_tracker.app:create_application --factory --workers 1`; default port binding `${TRACKER_BIND_ADDR:-127.0.0.1}:${TRACKER_PORT:-8000}:8000`, restart policy, and healthcheck. No production PostgreSQL/Kafka services or literal credentials in Stack. Validate `docker compose -f portainer-stack.yaml config --quiet` with dummy env values.
- [ ] Create `compose.local.yaml` with pinned `postgres:17-alpine`, `confluentinc/cp-kafka:7.5.0`, and persistent PostgreSQL volume. Document local startup, `docker build -t cinema-tracker:local .`, image tag/push, Portainer Git Stack deployment, admin key, initial watch POST with optional `providers` and `poll_interval_minutes` (default 15), both preview GET examples with repeatable `providers`, one-replica rule, and Kafka at-least-once delivery. Validate local Compose config and Docker build. Commit `build: add Portainer standalone deployment`.

### Task 8: End-to-end acceptance

**Files:** Create `tests/test_end_to_end.py`; modify `pyproject.toml` for test markers/lint as needed.

- [ ] RED: Docker-backed test starts PostgreSQL and Kafka; app starts with zero watches; authenticated POST creates initial Duna watch with default 15-minute interval; next scheduler check saves/emits six matching sessions. Second poll/restart emits zero. Add a new matching time and get one event; add another watch for same movie with 5-minute interval and confirm one fetch when both are due and no old-event replay. Run focused test; expect failure until full wiring works.
- [ ] GREEN: fix integration defects only; run focused and full suite.
- [ ] Final gates: `uv run pytest -q`, `uv run ruff check .`, `uv run ruff format --check .`, `docker build -t cinema-tracker:local .`, both Compose config checks. Optional read-only live Cineart smoke check outside automated tests. Inspect outputs before success claim. Commit `test: verify configurable tracker end to end`.

## Acceptance checklist

- [ ] Admin can create, list, replace, disable, and delete watches; unauthorized requests fail. Optional `providers` applies to every request that accepts provider selection.
- [ ] Omitted watch provider selection tracks current and future registered adapters; explicit nonempty list stays fixed. Per-provider resolution/fetch status is visible; pending providers retry without blocking successful ones.
- [ ] Both protected preview GETs use live source and watch filters; all preview returns full result, new preview excludes globally stored identities; neither mutates database, schedule, or Kafka.
- [ ] Each watch stores positive polling interval, default 15 minutes, and persisted next due time; custom intervals survive restart.
- [ ] Cineart fixture watch discovers six fixture sessions; repeats and overlaps emit zero duplicates. Live count is not fixed.
- [ ] New provider adapter can implement `resolve_movie(title)` and `fetch_sessions(movie_id)` without changing scheduler, storage, or publisher.
- [ ] Session table has required `purchase_url`; URL refresh updates row without duplicate discovery event.
- [ ] PostgreSQL transaction and outbox preserve events across failures; stable event ID allows downstream deduplication.
- [ ] Portainer Stack runs migration then one tracker against external production PostgreSQL/Kafka.
- [ ] Every behavior implemented after observed failing test; final unit/integration/container gates pass.

## Execution note

`/home/fab/codes/cinema_tracker` currently has no Git metadata. Initialize repository before implementation commits if this directory is the project root. This document and linked spec are planning artifacts; no product code exists yet.

## Future implementation TODO (outside V1)

- [ ] Add fuzzy candidate search and explicit selection for movie titles, cinema names, and room names. Follow matching rules and TDD cases in spec's “Future TODO: fuzzy name matching” section. Preserve exact-match V1 behavior until selected identities are safe to use during polling.
