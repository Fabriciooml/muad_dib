from datetime import date, time
from pathlib import Path

import httpx
import pytest

from cinema_tracker.cineart import CineartParseError, CineartProvider, parse_cineart_sessions
from cinema_tracker.domain import AmbiguousMovie, MovieNotFound

FIXTURES = Path(__file__).parent / "fixtures"


def test_parses_all_locations_and_six_boulevard_imax_sessions():
    sessions = parse_cineart_sessions((FIXTURES / "duna_23469.html").read_text(), "23469")

    imax = [
        item
        for item in sessions
        if item.cinema == "Cineart Boulevard" and item.room == "Sala 06 IMAX"
    ]
    assert len(imax) == 6
    assert len(sessions) > len(imax)
    assert imax[0].provider == "cineart"
    assert imax[0].movie_id == "23469"
    assert imax[0].cinema_id == "25"
    assert imax[0].date == date(2026, 12, 15)
    assert imax[0].time == time(14, 0)
    assert imax[0].timezone == "America/Sao_Paulo"
    assert imax[0].format == "IMAX"
    assert imax[0].purchase_url.startswith("https://www.veloxtickets.com/")


@pytest.mark.asyncio
async def test_resolves_title_from_upcoming_catalog_without_movie_id_input():
    def respond(request: httpx.Request) -> httpx.Response:
        fixture = (
            "catalog_em_breve.html" if request.url.path == "/em-breve" else "catalog_empty.html"
        )
        return httpx.Response(200, text=(FIXTURES / fixture).read_text())

    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
        movie = await CineartProvider(client).resolve_movie("  DUNA - PARTE TRES ")

    assert movie.movie_id == "23469"
    assert movie.title == "Duna - Parte Três"
    assert movie.source_url == "https://www.cineart.com.br/filme/23469"


def test_unavailable_page_is_empty_but_malformed_page_fails():
    assert parse_cineart_sessions("Sessões ainda não disponíveis para esse filme", "23469") == []
    with pytest.raises(CineartParseError):
        parse_cineart_sessions("<html><body>broken</body></html>", "23469")
    with pytest.raises(CineartParseError):
        parse_cineart_sessions('<filme-prog :cinemas="{}"></filme-prog>', "23469")
    with pytest.raises(CineartParseError):
        parse_cineart_sessions(
            '<filme-prog :cinemas="{&quot;day&quot;: {&quot;DATA&quot;: &quot;2026-12-15&quot;, &quot;CINEMAS&quot;: {}}}"></filme-prog>',
            "23469",
        )


@pytest.mark.asyncio
async def test_duplicate_title_ids_are_ambiguous_and_missing_title_is_not_found():
    catalog = """<div class="bloco-filme"><a href="/filme/1"></a><div class="text-titulo">Duna</div></div>
    <div class="bloco-filme"><a href="/filme/2"></a><div class="text-titulo">Duna</div></div>"""

    def respond(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200, text=catalog if request.url.path == "/em-breve" else "Sem filmes"
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
        provider = CineartProvider(client)
        with pytest.raises(AmbiguousMovie) as exc:
            await provider.resolve_movie("Duna")
        assert {item.movie_id for item in exc.value.candidates} == {"1", "2"}
        with pytest.raises(MovieNotFound):
            await provider.resolve_movie("Another title")
