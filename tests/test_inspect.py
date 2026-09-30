from datetime import date, time

import pytest

from cinema_tracker.domain import MovieMatch, MovieNotFound, Session
from cinema_tracker.inspect import InspectionRequest, SessionInspector


class FakeProvider:
    def __init__(self, name: str, movie_id: str | None) -> None:
        self.name = name
        self.movie_id = movie_id
        self.fetch_count = 0

    async def resolve_movie(self, title: str) -> MovieMatch:
        if self.movie_id is None:
            raise MovieNotFound(title)
        return MovieMatch(title, self.movie_id, f"https://example.com/{self.movie_id}")

    async def fetch_sessions(self, movie_id: str) -> list[Session]:
        self.fetch_count += 1
        return [
            Session(
                provider=self.name,
                movie_id=movie_id,
                city="Belo Horizonte",
                cinema_id="25",
                cinema="Cineart Boulevard",
                room="Sala 06 IMAX",
                date=date(2026, 12, 15),
                time=time(14, 0),
                timezone="America/Sao_Paulo",
                format="IMAX",
                language="Legendado",
                purchase_url="https://example.com/buy",
            )
        ]


@pytest.mark.asyncio
async def test_inspector_groups_fetches_and_reports_pending_provider_independently():
    cineart = FakeProvider("cineart", "23469")
    future = FakeProvider("future", None)
    inspector = SessionInspector({"cineart": cineart, "future": future})
    requests = [
        InspectionRequest("Duna - Parte Três", None, cinema="Cineart Boulevard"),
        InspectionRequest("Duna - Parte Três", ("cineart",), room="Sala 06 IMAX"),
    ]

    first, second = await inspector.inspect_many(requests)

    assert [result.status for result in first.providers] == ["success", "pending_not_found"]
    assert len(first.sessions) == 1
    assert [result.status for result in second.providers] == ["success"]
    assert len(second.sessions) == 1
    assert cineart.fetch_count == 1
    assert future.fetch_count == 0


@pytest.mark.asyncio
async def test_unexpected_provider_error_is_not_reported_as_movie_not_found():
    class BrokenProvider:
        async def resolve_movie(self, title):
            raise KeyError("broken source data")

        async def fetch_sessions(self, movie_id):
            return []

    inspector = SessionInspector({"broken": BrokenProvider()})
    with pytest.raises(KeyError, match="broken source data"):
        await inspector.resolve_targets("Duna", None)


@pytest.mark.asyncio
async def test_inspector_collapses_duplicate_source_entries_by_session_identity():
    class DuplicateProvider(FakeProvider):
        async def fetch_sessions(self, movie_id):
            one = (await super().fetch_sessions(movie_id))[0]
            return [one, one]

    inspector = SessionInspector({"cineart": DuplicateProvider("cineart", "23469")})
    result = (await inspector.inspect_many([InspectionRequest("Duna", None)]))[0]
    assert len(result.sessions) == 1
