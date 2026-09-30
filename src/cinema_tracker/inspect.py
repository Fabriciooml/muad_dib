"""Read-only inspection shared by previews and scheduled watches."""

from dataclasses import dataclass

from cinema_tracker.domain import (
    AmbiguousMovie,
    MovieNotFound,
    Provider,
    ProviderSourceError,
    ProviderTarget,
    Session,
    SessionFilter,
    select_providers,
)


@dataclass(frozen=True)
class InspectionRequest:
    movie_title: str
    providers: tuple[str, ...] | None
    city: str | None = None
    cinema: str | None = None
    room: str | None = None
    targets: tuple[ProviderTarget, ...] = ()


@dataclass(frozen=True)
class ProviderResult:
    provider: str
    status: str
    movie_id: str | None


@dataclass(frozen=True)
class InspectionResult:
    providers: tuple[ProviderResult, ...]
    sessions: tuple[Session, ...]
    targets: tuple[ProviderTarget, ...]


class SessionInspector:
    def __init__(self, providers: dict[str, Provider]) -> None:
        self.providers = providers

    async def resolve_targets(
        self,
        movie_title: str,
        selected: tuple[str, ...] | None,
        cached: tuple[ProviderTarget, ...] = (),
        *,
        strict_ambiguity: bool = True,
    ) -> tuple[ProviderTarget, ...]:
        cached_by_provider = {target.provider: target for target in cached}
        targets = []
        for name in select_providers(selected, tuple(self.providers)):
            target = cached_by_provider.get(name)
            if target is not None and target.movie_id is not None:
                targets.append(target)
                continue
            try:
                match = await self.providers[name].resolve_movie(movie_title)
            except AmbiguousMovie:
                if strict_ambiguity:
                    raise
                targets.append(ProviderTarget(name, None, "pending_ambiguous"))
            except MovieNotFound:
                targets.append(ProviderTarget(name, None, "pending_not_found"))
            except ProviderSourceError:
                targets.append(ProviderTarget(name, None, "pending_source_error"))
            else:
                targets.append(ProviderTarget(name, match.movie_id, "resolved"))
        return tuple(targets)

    async def inspect_many(self, requests: list[InspectionRequest]) -> tuple[InspectionResult, ...]:
        fetched: dict[tuple[str, str], list[Session] | ProviderSourceError] = {}
        results = []
        for request in requests:
            targets = await self.resolve_targets(
                request.movie_title,
                request.providers,
                request.targets,
                strict_ambiguity=False,
            )
            provider_results = []
            matching = []
            for target in targets:
                if target.movie_id is None:
                    provider_results.append(
                        ProviderResult(target.provider, target.resolution_status, None)
                    )
                    continue
                key = (target.provider, target.movie_id)
                if key not in fetched:
                    try:
                        fetched[key] = await self.providers[target.provider].fetch_sessions(
                            target.movie_id
                        )
                    except ProviderSourceError as exc:
                        fetched[key] = exc
                live = fetched[key]
                if isinstance(live, ProviderSourceError):
                    provider_results.append(
                        ProviderResult(target.provider, "source_error", target.movie_id)
                    )
                    continue
                session_filter = SessionFilter(
                    target.provider,
                    target.movie_id,
                    request.city,
                    request.cinema,
                    request.room,
                )
                matching.extend(item for item in live if session_filter.matches(item))
                provider_results.append(ProviderResult(target.provider, "success", target.movie_id))
            unique = tuple({item.key: item for item in matching}.values())
            results.append(InspectionResult(tuple(provider_results), unique, targets))
        return tuple(results)
