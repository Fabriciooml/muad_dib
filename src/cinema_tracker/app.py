"""Internal tracker API and component composition."""

import hmac
import os
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from typing import Annotated
from uuid import UUID, uuid4

import httpx
from fastapi import APIRouter, Depends, Header, HTTPException, Query, Response
from fastapi_component import create_app
from kafka_component import KafkaProducerComponent
from postgres_component import PostgresComponent
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest
from prometheus_component import Metrics
from pydantic import BaseModel, Field, field_validator
from python_components import Component, System
from starlette.responses import JSONResponse

from cinema_tracker.cineart import CineartProvider
from cinema_tracker.domain import AmbiguousMovie, Provider, Watch
from cinema_tracker.inspect import InspectionRequest, SessionInspector
from cinema_tracker.poll import SessionPoller
from cinema_tracker.publish import OutboxPublisher
from cinema_tracker.storage import PostgresStore


@dataclass(frozen=True)
class Settings:
    database_url: str
    kafka_bootstrap_servers: str
    admin_api_key: str
    port: int = 8000

    @classmethod
    def from_env(cls) -> "Settings":
        return cls(
            database_url=os.environ["DATABASE_URL"],
            kafka_bootstrap_servers=os.environ["KAFKA_BOOTSTRAP_SERVERS"],
            admin_api_key=os.environ["ADMIN_API_KEY"],
            port=int(os.environ.get("PORT", "8000")),
        )


class WatchCreate(BaseModel):
    movie_title: str
    providers: list[str] | None = None
    city: str | None = None
    cinema: str | None = None
    room: str | None = None
    enabled: bool = True
    poll_interval_minutes: int = Field(default=15, gt=0)

    @field_validator("movie_title", "city", "cinema", "room")
    @classmethod
    def nonblank(cls, value: str | None) -> str | None:
        if value is not None and not value.strip():
            raise ValueError("must not be blank")
        return value

    @field_validator("providers")
    @classmethod
    def nonempty_providers(cls, value: list[str] | None) -> list[str] | None:
        if value is not None and (not value or any(not item.strip() for item in value)):
            raise ValueError("providers must be nonempty")
        return value


class WatchReplace(WatchCreate):
    providers: list[str] | None
    enabled: bool
    poll_interval_minutes: int = Field(gt=0)


class ProviderTargetResponse(BaseModel):
    provider: str
    movie_id: str | None
    resolution_status: str
    last_resolution_at: datetime | None
    last_fetch_attempt_at: datetime | None
    last_successful_fetch_at: datetime | None
    last_fetch_error: str | None


class WatchResponse(BaseModel):
    id: UUID
    movie_title: str
    providers: list[str] | None
    city: str | None
    cinema: str | None
    room: str | None
    enabled: bool
    poll_interval_minutes: int
    next_poll_at: datetime
    revision: int
    targets: list[ProviderTargetResponse]


class ProviderClient(Component):
    def __init__(self, client: httpx.AsyncClient) -> None:
        self.using([])
        self.client = client

    def start(self) -> None:
        pass

    async def shutdown(self) -> None:
        await self.client.aclose()


def _watch_response(watch: Watch) -> dict:
    return {
        "id": str(watch.id),
        "movie_title": watch.movie_title,
        "providers": list(watch.providers) if watch.providers is not None else None,
        "city": watch.city,
        "cinema": watch.cinema,
        "room": watch.room,
        "enabled": watch.enabled,
        "poll_interval_minutes": watch.poll_interval_minutes,
        "next_poll_at": watch.next_poll_at.isoformat(),
        "revision": watch.revision,
        "targets": [
            {
                "provider": target.provider,
                "movie_id": target.movie_id,
                "resolution_status": target.resolution_status,
                "last_resolution_at": target.last_resolution_at,
                "last_fetch_attempt_at": target.last_fetch_attempt_at,
                "last_successful_fetch_at": target.last_successful_fetch_at,
                "last_fetch_error": target.last_fetch_error,
            }
            for target in watch.targets
        ],
    }


def build_app(
    settings: Settings,
    *,
    providers: dict[str, Provider] | None = None,
    store: PostgresStore | None = None,
):
    if not settings.admin_api_key:
        raise ValueError("ADMIN_API_KEY must be nonempty")
    database = PostgresComponent(url=settings.database_url)
    producer = KafkaProducerComponent(bootstrap_servers=settings.kafka_bootstrap_servers)
    metrics = Metrics(namespace="cinema")
    components: dict[str, Component] = {
        "database": database,
        "producer": producer,
        "metrics": metrics,
    }
    if providers is None:
        client = httpx.AsyncClient()
        providers = {"cineart": CineartProvider(client)}
        components["provider_client"] = ProviderClient(client)
    active_store = store or PostgresStore(database)
    inspector = SessionInspector(providers)
    poller = SessionPoller(active_store, providers, metrics).using(["database"])
    publisher = OutboxPublisher(active_store, producer, metrics).using(["database", "producer"])
    components.update(poller=poller, publisher=publisher)
    system = System(components)

    async def require_admin(x_admin_token: str | None = Header(default=None)) -> None:
        if x_admin_token is None or not hmac.compare_digest(x_admin_token, settings.admin_api_key):
            raise HTTPException(status_code=401, detail="Unauthorized")

    router = APIRouter(dependencies=[Depends(require_admin)])

    @router.post("/watches", status_code=201, response_model=WatchResponse)
    async def create_watch(body: WatchCreate) -> dict:
        selected = tuple(body.providers) if body.providers is not None else None
        try:
            targets = await inspector.resolve_targets(body.movie_title, selected)
        except ValueError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc
        except AmbiguousMovie as exc:
            raise HTTPException(
                status_code=409,
                detail={"candidates": [candidate.__dict__ for candidate in exc.candidates]},
            ) from exc
        watch = Watch(
            id=uuid4(),
            movie_title=body.movie_title.strip(),
            providers=tuple(dict.fromkeys(selected)) if selected is not None else None,
            targets=targets,
            city=body.city,
            cinema=body.cinema,
            room=body.room,
            enabled=body.enabled,
            poll_interval_minutes=body.poll_interval_minutes,
            next_poll_at=datetime.now(UTC),
            revision=1,
        )
        await active_store.create_watch(watch)
        return _watch_response(watch)

    @router.get("/watches/{watch_id}", response_model=WatchResponse)
    async def get_watch(watch_id: UUID) -> dict:
        watch = await active_store.get_watch(watch_id)
        if watch is None:
            raise HTTPException(status_code=404, detail="Watch not found")
        return _watch_response(watch)

    @router.get("/watches", response_model=list[WatchResponse])
    async def list_watches() -> list[dict]:
        return [_watch_response(watch) for watch in await active_store.list_watches()]

    @router.put("/watches/{watch_id}", response_model=WatchResponse)
    async def replace_watch(watch_id: UUID, body: WatchReplace) -> dict:
        existing = await active_store.get_watch(watch_id)
        if existing is None:
            raise HTTPException(status_code=404, detail="Watch not found")
        selected = tuple(dict.fromkeys(body.providers)) if body.providers is not None else None
        cached = (
            existing.targets
            if existing.movie_title == body.movie_title.strip() and existing.providers == selected
            else ()
        )
        try:
            targets = await inspector.resolve_targets(body.movie_title, selected, cached)
        except ValueError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc
        except AmbiguousMovie as exc:
            raise HTTPException(
                status_code=409,
                detail={"candidates": [candidate.__dict__ for candidate in exc.candidates]},
            ) from exc
        updated = replace(
            existing,
            movie_title=body.movie_title.strip(),
            providers=selected,
            targets=targets,
            city=body.city,
            cinema=body.cinema,
            room=body.room,
            enabled=body.enabled,
            poll_interval_minutes=body.poll_interval_minutes,
            next_poll_at=datetime.now(UTC),
            revision=existing.revision + 1,
        )
        if not await active_store.replace_watch(updated):
            raise HTTPException(status_code=409, detail="Watch changed concurrently")
        return _watch_response(updated)

    @router.delete("/watches/{watch_id}", status_code=204)
    async def delete_watch(watch_id: UUID) -> Response:
        if not await active_store.delete_watch(watch_id):
            raise HTTPException(status_code=404, detail="Watch not found")
        return Response(status_code=204)

    async def preview(
        movie_title: str,
        providers: list[str] | None,
        city: str | None,
        cinema: str | None,
        room: str | None,
        *,
        new_only: bool,
    ) -> JSONResponse:
        if any(
            value is not None and not value.strip() for value in (movie_title, city, cinema, room)
        ):
            raise HTTPException(status_code=422, detail="Names must not be blank")
        selected = tuple(providers) if providers is not None else None
        try:
            result = (
                await inspector.inspect_many(
                    [
                        InspectionRequest(
                            movie_title=movie_title,
                            providers=selected,
                            city=city,
                            cinema=cinema,
                            room=room,
                        )
                    ]
                )
            )[0]
        except ValueError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc
        statuses = {
            "pending_not_found": "not_found",
            "pending_ambiguous": "ambiguous",
            "pending_source_error": "source_error",
        }
        provider_results = [
            {
                "provider": item.provider,
                "status": statuses.get(item.status, item.status),
                "movie_id": item.movie_id,
            }
            for item in result.providers
        ]
        successful = any(item["status"] == "success" for item in provider_results)
        if not successful:
            failures = {item["status"] for item in provider_results}
            status_code = (
                503 if "source_error" in failures else 409 if "ambiguous" in failures else 404
            )
            raise HTTPException(status_code=status_code, detail=provider_results)
        sessions = list(result.sessions)
        if new_only:
            known = await active_store.existing_session_keys([item.key for item in sessions])
            sessions = [item for item in sessions if item.key not in known]
        sessions.sort(
            key=lambda item: (item.date, item.time, item.provider, item.cinema, item.room)
        )
        payload = {
            "movie_title": movie_title.strip(),
            "providers": list(dict.fromkeys(selected)) if selected is not None else None,
            "filters": {
                "city": city.strip() if city is not None else None,
                "cinema": cinema.strip() if cinema is not None else None,
                "room": room.strip() if room is not None else None,
            },
            "partial": any(item["status"] != "success" for item in provider_results),
            "checked_at": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
            "count": len(sessions),
            "provider_results": provider_results,
            "sessions": [
                {
                    "provider": item.provider,
                    "movie_id": item.movie_id,
                    "city": item.city,
                    "cinema_id": item.cinema_id,
                    "cinema": item.cinema,
                    "room": item.room,
                    "date": item.date.isoformat(),
                    "time": item.key.minute.strftime("%H:%M"),
                    "timezone": item.timezone,
                    "format": item.format,
                    "language": item.language,
                    "purchase_url": item.purchase_url,
                }
                for item in sessions
            ],
        }
        return JSONResponse(payload, headers={"Cache-Control": "no-store"})

    @router.get("/sessions/preview")
    async def preview_all(
        movie_title: str,
        providers: Annotated[list[str] | None, Query()] = None,
        city: str | None = None,
        cinema: str | None = None,
        room: str | None = None,
    ) -> JSONResponse:
        return await preview(movie_title, providers, city, cinema, room, new_only=False)

    @router.get("/sessions/preview/new")
    async def preview_new(
        movie_title: str,
        providers: Annotated[list[str] | None, Query()] = None,
        city: str | None = None,
        cinema: str | None = None,
        room: str | None = None,
    ) -> JSONResponse:
        return await preview(movie_title, providers, city, cinema, room, new_only=True)

    health = APIRouter()

    @health.get("/health/live")
    async def live() -> dict:
        return {"status": "ok"}

    @health.get("/health/ready")
    async def ready() -> dict:
        if not database.get_status()["connected"] or not producer.get_status()["connected"]:
            raise HTTPException(status_code=503, detail="Dependencies unavailable")
        try:
            await active_store.ping()
        except Exception as exc:
            raise HTTPException(status_code=503, detail="Database unavailable") from exc
        return {"status": "ready"}

    @health.get("/metrics")
    async def serve_metrics() -> Response:
        return Response(content=generate_latest(metrics.registry), media_type=CONTENT_TYPE_LATEST)

    app = create_app(system, routers=[router, health], component_routes=False)
    metrics.instrument(app)
    return app


def create_application():
    return build_app(Settings.from_env())
