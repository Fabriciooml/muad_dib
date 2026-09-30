"""Cineart catalog and session adapter."""

import json
import re
import unicodedata
from datetime import date, time
from html.parser import HTMLParser
from urllib.parse import urlparse

import httpx

from cinema_tracker.domain import (
    AmbiguousMovie,
    MovieMatch,
    MovieNotFound,
    ProviderSourceError,
    Session,
)


class CineartParseError(ProviderSourceError):
    pass


def _normalize_title(value: str) -> str:
    plain = unicodedata.normalize("NFKD", value)
    plain = "".join(char for char in plain if not unicodedata.combining(char))
    return " ".join(plain.casefold().split())


class _CatalogParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.depth = 0
        self.movie_id: str | None = None
        self.title_parts: list[str] = []
        self.in_title = False
        self.matches: list[MovieMatch] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        attrs_map = dict(attrs)
        if tag == "div":
            classes = (attrs_map.get("class") or "").split()
            if self.depth == 0 and "bloco-filme" in classes:
                self.depth = 1
                self.movie_id = None
                self.title_parts = []
            elif self.depth:
                self.depth += 1
                if "text-titulo" in classes:
                    self.in_title = True
        elif tag == "a" and self.depth:
            match = re.fullmatch(r"/filme/(\d+)", urlparse(attrs_map.get("href") or "").path)
            if match:
                self.movie_id = match.group(1)

    def handle_data(self, data: str) -> None:
        if self.in_title:
            self.title_parts.append(data)

    def handle_endtag(self, tag: str) -> None:
        if tag != "div" or not self.depth:
            return
        if self.in_title:
            self.in_title = False
        self.depth -= 1
        if self.depth == 0 and self.movie_id and self.title_parts:
            title = " ".join("".join(self.title_parts).split())
            self.matches.append(
                MovieMatch(
                    title, self.movie_id, f"https://www.cineart.com.br/filme/{self.movie_id}"
                )
            )


def parse_cineart_catalog(page: str) -> list[MovieMatch]:
    parser = _CatalogParser()
    parser.feed(page)
    if not parser.matches and "Sem filmes" not in page:
        raise CineartParseError("Cineart catalog data missing")
    return parser.matches


class _FilmeProgParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.cinemas: str | None = None

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag == "filme-prog":
            self.cinemas = dict(attrs).get(":cinemas")


def parse_cineart_sessions(page: str, movie_id: str) -> list[Session]:
    parser = _FilmeProgParser()
    parser.feed(page)
    if parser.cinemas is None:
        if "Sessões ainda não disponíveis para esse filme" in page:
            return []
        raise CineartParseError("Cineart session data missing")
    try:
        listings = json.loads(parser.cinemas)
        sessions = []
        for entry in listings.values():
            session_date = date.fromisoformat(entry["DATA"])
            for cinema_id, cinema in entry["CINEMAS"].items():
                for room in cinema["SALAS"].values():
                    for showing in room["HORARIOS"]:
                        purchase_url = showing["URL_COMPRA"]
                        if not purchase_url.startswith(("https://", "http://")):
                            raise ValueError("purchase URL must be absolute")
                        sessions.append(
                            Session(
                                provider="cineart",
                                movie_id=str(movie_id),
                                city=cinema["CIDADE"],
                                cinema_id=str(cinema_id),
                                cinema=cinema["CINEMA"],
                                room=room["SALA"],
                                date=session_date,
                                time=time.fromisoformat(showing["HORARIO"]).replace(second=0),
                                timezone="America/Sao_Paulo",
                                format="IMAX" if showing["IMAX"] else room["TIPO"],
                                language=room["LEGENDA"],
                                purchase_url=purchase_url,
                            )
                        )
    except (TypeError, ValueError, KeyError, AttributeError) as exc:
        raise CineartParseError("Malformed Cineart session data") from exc
    return sorted(sessions, key=lambda item: (item.date, item.time, item.cinema, item.room))


class CineartProvider:
    def __init__(self, client: httpx.AsyncClient) -> None:
        self.client = client

    async def resolve_movie(self, title: str) -> MovieMatch:
        candidates: dict[str, MovieMatch] = {}
        source_failed = False
        for path in ("em-cartaz", "estreias", "em-breve"):
            try:
                response = await self.client.get(f"https://www.cineart.com.br/{path}", timeout=10.0)
                response.raise_for_status()
                for movie in parse_cineart_catalog(response.text):
                    if _normalize_title(movie.title) == _normalize_title(title):
                        candidates[movie.movie_id] = movie
            except (httpx.HTTPError, CineartParseError):
                source_failed = True
        if len(candidates) > 1:
            raise AmbiguousMovie(list(candidates.values()))
        if candidates:
            return next(iter(candidates.values()))
        if source_failed:
            raise ProviderSourceError("Cineart catalog unavailable")
        raise MovieNotFound(title)

    async def fetch_sessions(self, movie_id: str) -> list[Session]:
        try:
            response = await self.client.get(
                f"https://www.cineart.com.br/filme/{movie_id}", timeout=10.0
            )
            response.raise_for_status()
        except httpx.HTTPError as exc:
            raise ProviderSourceError("Cineart session request failed") from exc
        return parse_cineart_sessions(response.text, movie_id)
