"""PostgreSQL store for watches, sessions, and discovery outbox."""

import json
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from uuid import UUID, uuid4

from postgres_component import PostgresComponent
from sqlalchemy import delete, func, insert, select, text, tuple_, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from cinema_tracker.domain import ProviderTarget, Session, SessionKey, Watch
from cinema_tracker.schema import outbox, sessions, targets, watches

TOPIC = "cinema.sessions.discovered.v1"


@dataclass(frozen=True)
class OutboxEvent:
    id: UUID
    topic: str
    key: bytes
    payload: dict


@dataclass(frozen=True)
class PollWrite:
    watch: Watch
    sessions: tuple[Session, ...]
    targets: tuple[ProviderTarget, ...]


def _event_time(value: datetime) -> str:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _event_key(key: SessionKey) -> bytes:
    return json.dumps(
        [
            key.provider,
            key.movie_id,
            key.cinema_id,
            key.room_key,
            key.date.isoformat(),
            key.minute.strftime("%H:%M"),
        ],
        ensure_ascii=False,
        separators=(",", ":"),
    ).encode("utf-8")


def _event_payload(event_id: UUID, item: Session, observed_at: datetime) -> dict:
    return {
        "event_id": str(event_id),
        "event_type": "session.discovered",
        "schema_version": 1,
        "observed_at": _event_time(observed_at),
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


class PostgresStore:
    def __init__(self, database: PostgresComponent) -> None:
        self.database = database

    async def ping(self) -> bool:
        async with self.database.session() as session:
            return (await session.execute(text("SELECT 1"))).scalar_one() == 1

    async def create_watch(self, watch: Watch) -> None:
        async with self.database.session() as session:
            await session.execute(
                insert(watches).values(
                    id=watch.id,
                    movie_title=watch.movie_title,
                    providers=list(watch.providers) if watch.providers is not None else None,
                    city=watch.city,
                    cinema=watch.cinema,
                    room=watch.room,
                    enabled=watch.enabled,
                    poll_interval_minutes=watch.poll_interval_minutes,
                    next_poll_at=watch.next_poll_at,
                    revision=watch.revision,
                )
            )
            await self._insert_targets(session, watch.id, watch.targets)

    async def get_watch(self, watch_id: UUID) -> Watch | None:
        async with self.database.session() as session:
            row = (
                (await session.execute(select(watches).where(watches.c.id == watch_id)))
                .mappings()
                .one_or_none()
            )
            if row is None:
                return None
            target_rows = (
                (await session.execute(select(targets).where(targets.c.watch_id == watch_id)))
                .mappings()
                .all()
            )
            return Watch(
                id=row["id"],
                movie_title=row["movie_title"],
                providers=tuple(row["providers"]) if row["providers"] is not None else None,
                targets=tuple(
                    ProviderTarget(
                        provider=item["provider"],
                        movie_id=item["movie_id"],
                        resolution_status=item["resolution_status"],
                        last_resolution_at=item["last_resolution_at"],
                        last_fetch_attempt_at=item["last_fetch_attempt_at"],
                        last_successful_fetch_at=item["last_successful_fetch_at"],
                        last_fetch_error=item["last_fetch_error"],
                    )
                    for item in target_rows
                ),
                city=row["city"],
                cinema=row["cinema"],
                room=row["room"],
                enabled=row["enabled"],
                poll_interval_minutes=row["poll_interval_minutes"],
                next_poll_at=row["next_poll_at"],
                revision=row["revision"],
            )

    async def list_watches(self) -> list[Watch]:
        async with self.database.session() as session:
            ids = (
                await session.execute(select(watches.c.id).order_by(watches.c.created_at))
            ).scalars()
            watch_ids = list(ids)
        return [watch for watch_id in watch_ids if (watch := await self.get_watch(watch_id))]

    async def list_due_watches(self, now: datetime) -> list[Watch]:
        async with self.database.session() as session:
            ids = (
                (
                    await session.execute(
                        select(watches.c.id)
                        .where(watches.c.enabled.is_(True), watches.c.next_poll_at <= now)
                        .order_by(watches.c.next_poll_at, watches.c.id)
                    )
                )
                .scalars()
                .all()
            )
        return [watch for watch_id in ids if (watch := await self.get_watch(watch_id))]

    async def replace_watch(self, watch: Watch) -> bool:
        async with self.database.session() as session:
            result = await session.execute(
                update(watches)
                .where(watches.c.id == watch.id, watches.c.revision == watch.revision - 1)
                .values(
                    movie_title=watch.movie_title,
                    providers=list(watch.providers) if watch.providers is not None else None,
                    city=watch.city,
                    cinema=watch.cinema,
                    room=watch.room,
                    enabled=watch.enabled,
                    poll_interval_minutes=watch.poll_interval_minutes,
                    next_poll_at=watch.next_poll_at,
                    revision=watch.revision,
                    updated_at=func.now(),
                )
            )
            if not result.rowcount:
                return False
            await session.execute(delete(targets).where(targets.c.watch_id == watch.id))
            await self._insert_targets(session, watch.id, watch.targets)
        return True

    async def delete_watch(self, watch_id: UUID) -> bool:
        async with self.database.session() as session:
            result = await session.execute(delete(watches).where(watches.c.id == watch_id))
            return bool(result.rowcount)

    async def _insert_targets(
        self, session: AsyncSession, watch_id: UUID, provider_targets: tuple[ProviderTarget, ...]
    ) -> None:
        for target in provider_targets:
            await session.execute(
                insert(targets).values(
                    watch_id=watch_id,
                    provider=target.provider,
                    movie_id=target.movie_id,
                    resolution_status=target.resolution_status,
                    last_resolution_at=target.last_resolution_at,
                    last_fetch_attempt_at=target.last_fetch_attempt_at,
                    last_successful_fetch_at=target.last_successful_fetch_at,
                    last_fetch_error=target.last_fetch_error,
                )
            )

    async def upsert_provider_target(self, watch_id: UUID, target: ProviderTarget) -> None:
        async with self.database.session() as session:
            await self._upsert_target(session, watch_id, target)

    async def _upsert_target(
        self, session: AsyncSession, watch_id: UUID, target: ProviderTarget
    ) -> None:
        values = {
            "watch_id": watch_id,
            "provider": target.provider,
            "movie_id": target.movie_id,
            "resolution_status": target.resolution_status,
            "last_resolution_at": target.last_resolution_at,
            "last_fetch_attempt_at": target.last_fetch_attempt_at,
            "last_successful_fetch_at": target.last_successful_fetch_at,
            "last_fetch_error": target.last_fetch_error,
        }
        await session.execute(
            pg_insert(targets)
            .values(**values)
            .on_conflict_do_update(
                index_elements=[targets.c.watch_id, targets.c.provider],
                set_={
                    key: value
                    for key, value in values.items()
                    if key not in ("watch_id", "provider")
                },
            )
        )

    async def apply_poll_results(self, writes: list[PollWrite], completed_at: datetime) -> int:
        if not writes:
            return 0
        by_id = {write.watch.id: write for write in writes}
        async with self.database.session() as session:
            locked = (
                (
                    await session.execute(
                        select(
                            watches.c.id,
                            watches.c.revision,
                            watches.c.enabled,
                            watches.c.poll_interval_minutes,
                        )
                        .where(watches.c.id.in_(by_id))
                        .order_by(watches.c.id)
                        .with_for_update()
                    )
                )
                .mappings()
                .all()
            )
            current = [
                (by_id[row["id"]], row["poll_interval_minutes"])
                for row in locked
                if row["enabled"] and row["revision"] == by_id[row["id"]].watch.revision
            ]
            union = list(
                {item.key: item for write, _ in current for item in write.sessions}.values()
            )
            inserted = await self._insert_discoveries_in_session(session, union, completed_at)
            for write, interval in current:
                for target in write.targets:
                    await self._upsert_target(session, write.watch.id, target)
                await session.execute(
                    update(watches)
                    .where(watches.c.id == write.watch.id)
                    .values(next_poll_at=completed_at + timedelta(minutes=interval))
                )
            return inserted

    async def insert_discoveries(self, items: list[Session], observed_at: datetime) -> int:
        async with self.database.session() as session:
            return await self._insert_discoveries_in_session(session, items, observed_at)

    async def _insert_discoveries_in_session(
        self, session: AsyncSession, items: list[Session], observed_at: datetime
    ) -> int:
        inserted = 0
        for item in {item.key: item for item in items}.values():
            key = item.key
            new_id = uuid4()
            result = await session.execute(
                pg_insert(sessions)
                .values(
                    id=new_id,
                    provider=item.provider,
                    movie_id=item.movie_id,
                    city=item.city,
                    cinema_id=item.cinema_id,
                    cinema=item.cinema,
                    room_name=item.room,
                    room_key=key.room_key,
                    session_date=item.date,
                    session_time=key.minute,
                    timezone=item.timezone,
                    format=item.format,
                    language=item.language,
                    purchase_url=item.purchase_url,
                    first_seen_at=observed_at,
                )
                .on_conflict_do_nothing(constraint="uq_session_identity")
                .returning(sessions.c.id)
            )
            session_id = result.scalar_one_or_none()
            if session_id is None:
                await session.execute(
                    update(sessions)
                    .where(
                        sessions.c.provider == key.provider,
                        sessions.c.movie_id == key.movie_id,
                        sessions.c.cinema_id == key.cinema_id,
                        sessions.c.room_key == key.room_key,
                        sessions.c.session_date == key.date,
                        sessions.c.session_time == key.minute,
                    )
                    .values(
                        city=item.city,
                        cinema=item.cinema,
                        room_name=item.room,
                        timezone=item.timezone,
                        format=item.format,
                        language=item.language,
                        purchase_url=item.purchase_url,
                    )
                )
                continue
            event_id = uuid4()
            await session.execute(
                insert(outbox).values(
                    id=event_id,
                    session_id=session_id,
                    topic=TOPIC,
                    key=_event_key(key),
                    payload=_event_payload(event_id, item, observed_at),
                )
            )
            inserted += 1
        return inserted

    async def existing_session_keys(self, keys: list[SessionKey]) -> set[SessionKey]:
        if not keys:
            return set()
        columns = (
            sessions.c.provider,
            sessions.c.movie_id,
            sessions.c.cinema_id,
            sessions.c.room_key,
            sessions.c.session_date,
            sessions.c.session_time,
        )
        values = [
            (key.provider, key.movie_id, key.cinema_id, key.room_key, key.date, key.minute)
            for key in keys
        ]
        async with self.database.session() as session:
            rows = (
                await session.execute(select(*columns).where(tuple_(*columns).in_(values)))
            ).all()
        return {SessionKey(*row) for row in rows}

    async def pending_events(self, limit: int) -> list[OutboxEvent]:
        async with self.database.session() as session:
            rows = (
                (
                    await session.execute(
                        select(outbox)
                        .where(outbox.c.published_at.is_(None))
                        .order_by(outbox.c.created_at, outbox.c.id)
                        .limit(limit)
                    )
                )
                .mappings()
                .all()
            )
        return [OutboxEvent(row["id"], row["topic"], row["key"], row["payload"]) for row in rows]

    async def mark_published(self, event_id: UUID) -> None:
        async with self.database.session() as session:
            await session.execute(
                update(outbox)
                .where(outbox.c.id == event_id)
                .values(published_at=func.now(), last_error=None)
            )

    async def record_publish_failure(self, event_id: UUID, error: str) -> None:
        async with self.database.session() as session:
            await session.execute(
                update(outbox)
                .where(outbox.c.id == event_id)
                .values(attempts=outbox.c.attempts + 1, last_error=error[:1000])
            )

    async def pending_count(self) -> int:
        async with self.database.session() as session:
            return int(
                (
                    await session.execute(
                        select(func.count())
                        .select_from(outbox)
                        .where(outbox.c.published_at.is_(None))
                    )
                ).scalar_one()
            )
