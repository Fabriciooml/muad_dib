from datetime import UTC, date, datetime, time
from uuid import uuid4

from cinema_tracker.domain import ProviderTarget, Session, Watch


def test_watch_matches_normalized_location_names():
    watch = Watch(
        id=uuid4(),
        movie_title="Duna - Parte Três",
        providers=("cineart",),
        targets=(
            ProviderTarget(provider="cineart", movie_id="23469", resolution_status="resolved"),
        ),
        city=" Belo Horizonte ",
        cinema=" Cineart Boulevard ",
        room="Sala 06 IMAX",
        enabled=True,
        poll_interval_minutes=15,
        next_poll_at=datetime.now(UTC),
        revision=1,
    )
    session = Session(
        provider="cineart",
        movie_id="23469",
        city="belo horizonte",
        cinema_id="25",
        cinema="cineart boulevard",
        room="sala 06 imax",
        date=date(2026, 12, 15),
        time=time(14, 0),
        timezone="America/Sao_Paulo",
        format="IMAX",
        language="legendado",
        purchase_url="https://example.com/buy",
    )
    assert watch.matches(session)
