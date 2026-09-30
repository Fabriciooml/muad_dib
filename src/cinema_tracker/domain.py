"""Provider-neutral watch and session values."""

from dataclasses import dataclass
from datetime import date, datetime, time
from typing import Protocol
from uuid import UUID


def normalize_name(value: str) -> str:
    return value.strip().casefold()


@dataclass(frozen=True)
class SessionKey:
    provider: str
    movie_id: str
    cinema_id: str
    room_key: str
    date: date
    minute: time


@dataclass(frozen=True)
class Session:
    provider: str
    movie_id: str
    city: str
    cinema_id: str
    cinema: str
    room: str
    date: date
    time: time
    timezone: str
    format: str | None
    language: str | None
    purchase_url: str

    @property
    def key(self) -> SessionKey:
        return SessionKey(
            self.provider,
            self.movie_id,
            self.cinema_id,
            normalize_name(self.room),
            self.date,
            self.time.replace(second=0, microsecond=0),
        )


@dataclass(frozen=True)
class ProviderTarget:
    provider: str
    movie_id: str | None
    resolution_status: str
    last_resolution_at: datetime | None = None
    last_fetch_attempt_at: datetime | None = None
    last_successful_fetch_at: datetime | None = None
    last_fetch_error: str | None = None


@dataclass(frozen=True)
class SessionFilter:
    provider: str
    movie_id: str
    city: str | None = None
    cinema: str | None = None
    room: str | None = None

    def matches(self, session: Session) -> bool:
        if self.provider != session.provider or self.movie_id != session.movie_id:
            return False
        for expected, actual in (
            (self.city, session.city),
            (self.cinema, session.cinema),
            (self.room, session.room),
        ):
            if expected is not None and normalize_name(expected) != normalize_name(actual):
                return False
        return True


@dataclass(frozen=True)
class Watch:
    id: UUID
    movie_title: str
    providers: tuple[str, ...] | None
    targets: tuple[ProviderTarget, ...]
    city: str | None
    cinema: str | None
    room: str | None
    enabled: bool
    poll_interval_minutes: int
    next_poll_at: datetime
    revision: int

    def matches(self, session: Session) -> bool:
        if not self.enabled:
            return False
        return any(
            target.movie_id is not None
            and SessionFilter(
                target.provider,
                target.movie_id,
                self.city,
                self.cinema,
                self.room,
            ).matches(session)
            for target in self.targets
        )


@dataclass(frozen=True)
class MovieMatch:
    title: str
    movie_id: str
    source_url: str


class Provider(Protocol):
    async def resolve_movie(self, title: str) -> MovieMatch: ...

    async def fetch_sessions(self, movie_id: str) -> list[Session]: ...


def select_providers(
    requested: tuple[str, ...] | None, registry_names: tuple[str, ...]
) -> tuple[str, ...]:
    available = dict.fromkeys(registry_names)
    if requested is None:
        return tuple(available)
    selected = tuple(dict.fromkeys(requested))
    if not selected or any(name not in available for name in selected):
        raise ValueError("providers must be a nonempty list of registered names")
    return selected
