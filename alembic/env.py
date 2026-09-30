"""Application-owned migrations using postgres-component connection settings."""

import asyncio
import os

from postgres_component import PostgresComponent, create_migration_engine

from alembic import context
from cinema_tracker.schema import metadata

target_metadata = metadata


def run_migrations_offline() -> None:
    raise RuntimeError("Offline migrations are not supported")


def do_run_migrations(connection) -> None:
    context.configure(connection=connection, target_metadata=target_metadata, compare_type=True)
    with context.begin_transaction():
        context.run_migrations()


async def run_migrations_online() -> None:
    database = PostgresComponent(url=os.environ["DATABASE_URL"])
    engine = create_migration_engine(database.migration_wiring())
    try:
        async with engine.connect() as connection:
            await connection.run_sync(do_run_migrations)
    finally:
        await engine.dispose()


if context.is_offline_mode():
    run_migrations_offline()
else:
    asyncio.run(run_migrations_online())
