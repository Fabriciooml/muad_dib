# Cinema session tracker

Monitors configured movie titles across cinema providers. V1 provider: Cineart. Each watch defaults to a 15-minute polling interval. Newly observed sessions go to PostgreSQL and Kafka topic `cinema.sessions.discovered.v1` through a transactional outbox.

## Local development

Requirements: Python 3.11+, `uv`, Docker with Compose.

```bash
docker compose -f compose.local.yaml up -d
export DATABASE_URL='postgresql+asyncpg://tracker:tracker@localhost:5432/tracker'
export KAFKA_BOOTSTRAP_SERVERS='localhost:9092'
export ADMIN_API_KEY='replace-with-a-long-local-key'
uv sync --frozen
uv run alembic upgrade head
docker compose -f compose.local.yaml exec kafka kafka-topics --bootstrap-server localhost:9092 --create --if-not-exists --topic cinema.sessions.discovered.v1 --partitions 1 --replication-factor 1
uv run uvicorn cinema_tracker.app:create_application --factory --host 127.0.0.1 --port 8000 --workers 1
```

Create initial watch (provider ID resolved from title):

```bash
curl -X POST http://127.0.0.1:8000/watches \
  -H "X-Admin-Token: $ADMIN_API_KEY" -H 'Content-Type: application/json' \
  -d '{"movie_title":"Duna - Parte Três","providers":["cineart"],"cinema":"Cineart Boulevard","room":"Sala 06 IMAX"}'
```

Omit `providers` to include every registered provider, including adapters added later. Set `poll_interval_minutes` in request to override default `15`. A watch may remain pending until selected provider lists movie.

Read live sessions without creating watch:

```bash
curl -G http://127.0.0.1:8000/sessions/preview \
  -H "X-Admin-Token: $ADMIN_API_KEY" \
  --data-urlencode 'movie_title=Duna - Parte Três' \
  --data-urlencode 'providers=cineart' \
  --data-urlencode 'cinema=Cineart Boulevard' \
  --data-urlencode 'room=Sala 06 IMAX'
```

Use `/sessions/preview/new` for sessions absent from PostgreSQL. Both preview GETs leave watches, sessions, outbox, and Kafka unchanged. Repeat `providers` parameter to select several providers.

## Portainer Docker Standalone

Build and push image from this repository, then deploy `portainer-stack.yaml` as a Git Stack. Set `TRACKER_IMAGE`, `DATABASE_URL`, `KAFKA_BOOTSTRAP_SERVERS`, and `ADMIN_API_KEY` as Portainer variables. Production PostgreSQL and Kafka run outside Stack. Provision Kafka topic and database migration rights before deployment. Stack runs Alembic migration, then one tracker container with one Uvicorn worker. Default host binding is `127.0.0.1:8000`; set `TRACKER_BIND_ADDR` and `TRACKER_PORT` only when needed.

```bash
docker build -t registry.example.com/cinema-tracker:0.1.0 .
docker push registry.example.com/cinema-tracker:0.1.0
```

Kafka delivery is at least once. Consumers deduplicate with stable `event_id`. Output JSON Schema: [`schemas/output.v1.schema.json`](schemas/output.v1.schema.json). GET `/health/live`, GET `/health/ready`, and GET `/metrics` support operations.
