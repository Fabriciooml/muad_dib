"""SQLAlchemy Core tables for tracker state."""

from sqlalchemy import (
    ARRAY,
    BigInteger,
    Boolean,
    CheckConstraint,
    Column,
    Date,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    LargeBinary,
    MetaData,
    String,
    Table,
    Text,
    Time,
    UniqueConstraint,
    func,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID

metadata = MetaData()

watches = Table(
    "watches",
    metadata,
    Column("id", UUID(as_uuid=True), primary_key=True),
    Column("movie_title", Text, nullable=False),
    Column("providers", ARRAY(Text)),
    Column("city", Text),
    Column("cinema", Text),
    Column("room", Text),
    Column("enabled", Boolean, nullable=False, server_default=text("true")),
    Column("poll_interval_minutes", Integer, nullable=False, server_default=text("15")),
    Column("next_poll_at", DateTime(timezone=True), nullable=False),
    Column("revision", BigInteger, nullable=False, server_default=text("1")),
    Column("created_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
    Column("updated_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
    CheckConstraint("poll_interval_minutes > 0", name="ck_watches_positive_interval"),
    CheckConstraint("revision >= 1", name="ck_watches_positive_revision"),
    CheckConstraint("providers IS NULL OR cardinality(providers) > 0", name="ck_watches_providers"),
)
Index("ix_watches_due", watches.c.next_poll_at, postgresql_where=watches.c.enabled.is_(True))

targets = Table(
    "watch_provider_targets",
    metadata,
    Column(
        "watch_id",
        UUID(as_uuid=True),
        ForeignKey("watches.id", ondelete="CASCADE"),
        primary_key=True,
    ),
    Column("provider", String(64), primary_key=True),
    Column("movie_id", Text),
    Column("resolution_status", String(32), nullable=False),
    Column("last_resolution_at", DateTime(timezone=True)),
    Column("last_fetch_attempt_at", DateTime(timezone=True)),
    Column("last_successful_fetch_at", DateTime(timezone=True)),
    Column("last_fetch_error", Text),
    CheckConstraint(
        "(resolution_status = 'resolved' AND movie_id IS NOT NULL) OR "
        "(resolution_status IN ('pending_not_found', 'pending_source_error', "
        "'pending_ambiguous') AND movie_id IS NULL)",
        name="ck_target_resolution",
    ),
)

sessions = Table(
    "sessions",
    metadata,
    Column("id", UUID(as_uuid=True), primary_key=True),
    Column("provider", String(64), nullable=False),
    Column("movie_id", Text, nullable=False),
    Column("city", Text, nullable=False),
    Column("cinema_id", Text, nullable=False),
    Column("cinema", Text, nullable=False),
    Column("room_name", Text, nullable=False),
    Column("room_key", Text, nullable=False),
    Column("session_date", Date, nullable=False),
    Column("session_time", Time, nullable=False),
    Column("timezone", Text, nullable=False),
    Column("format", Text),
    Column("language", Text),
    Column("purchase_url", Text, nullable=False),
    Column("first_seen_at", DateTime(timezone=True), nullable=False),
    UniqueConstraint(
        "provider",
        "movie_id",
        "cinema_id",
        "room_key",
        "session_date",
        "session_time",
        name="uq_session_identity",
    ),
)

outbox = Table(
    "outbox",
    metadata,
    Column("id", UUID(as_uuid=True), primary_key=True),
    Column("session_id", UUID(as_uuid=True), ForeignKey("sessions.id"), nullable=False),
    Column("topic", Text, nullable=False),
    Column("key", LargeBinary, nullable=False),
    Column("payload", JSONB, nullable=False),
    Column("created_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
    Column("published_at", DateTime(timezone=True)),
    Column("attempts", Integer, nullable=False, server_default=text("0")),
    Column("last_error", Text),
    CheckConstraint("attempts >= 0", name="ck_outbox_attempts"),
    UniqueConstraint("session_id", "topic", name="uq_outbox_session_topic"),
)
Index(
    "ix_outbox_pending",
    outbox.c.created_at,
    outbox.c.id,
    postgresql_where=outbox.c.published_at.is_(None),
)
