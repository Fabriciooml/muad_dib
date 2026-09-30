"""Poll due watches and persist discoveries through one store transaction."""

import asyncio
import logging
from dataclasses import replace
from datetime import UTC, datetime

from prometheus_component import Metrics
from python_components import Component

from cinema_tracker.domain import Provider
from cinema_tracker.inspect import InspectionRequest, SessionInspector
from cinema_tracker.storage import PollWrite, PostgresStore

logger = logging.getLogger(__name__)


class SessionPoller(Component):
    def __init__(
        self,
        store: PostgresStore,
        providers: dict[str, Provider],
        metrics: Metrics | None = None,
        check_interval_seconds: float = 30.0,
    ) -> None:
        self.using([])
        self.store = store
        self.inspector = SessionInspector(providers)
        self.check_interval_seconds = check_interval_seconds
        self._task: asyncio.Task | None = None
        self._polls = metrics.counter("poll_total", "Completed watch polls") if metrics else None
        self._discovered = (
            metrics.counter("sessions_discovered_total", "New sessions") if metrics else None
        )
        self._failures = (
            metrics.counter("poll_failures_total", "Failed watch polls") if metrics else None
        )
        self._last_success = (
            metrics.gauge("poll_last_success_timestamp", "Last successful poll")
            if metrics
            else None
        )

    async def poll_once(self, now: datetime) -> int:
        due = await self.store.list_due_watches(now)
        if not due:
            return 0
        requests = [
            InspectionRequest(
                movie_title=watch.movie_title,
                providers=watch.providers,
                city=watch.city,
                cinema=watch.cinema,
                room=watch.room,
                targets=watch.targets,
            )
            for watch in due
        ]
        inspections = await self.inspector.inspect_many(requests)
        writes = []
        for watch, inspection in zip(due, inspections, strict=True):
            results_by_provider = {result.provider: result for result in inspection.providers}
            targets = []
            for target in inspection.targets:
                result = results_by_provider[target.provider]
                if target.movie_id is None:
                    targets.append(replace(target, last_resolution_at=now))
                elif result.status == "success":
                    targets.append(
                        replace(
                            target,
                            last_fetch_attempt_at=now,
                            last_successful_fetch_at=now,
                            last_fetch_error=None,
                        )
                    )
                else:
                    targets.append(
                        replace(target, last_fetch_attempt_at=now, last_fetch_error=result.status)
                    )
            writes.append(PollWrite(watch, inspection.sessions, tuple(targets)))
        completed_at = datetime.now(UTC)
        inserted = await self.store.apply_poll_results(writes, completed_at)
        if self._polls is not None:
            self._polls.inc(len(due))
        if self._discovered is not None:
            self._discovered.inc(inserted)
        failed_watches = sum(
            any(
                result.status in {"source_error", "pending_source_error"}
                for result in inspection.providers
            )
            for inspection in inspections
        )
        if self._failures is not None:
            self._failures.inc(failed_watches)
        if self._last_success is not None and any(
            result.status == "success"
            for inspection in inspections
            for result in inspection.providers
        ):
            self._last_success.set(completed_at.timestamp())
        return inserted

    async def start(self) -> None:
        self._task = asyncio.create_task(self._run())

    async def shutdown(self) -> None:
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None

    async def _run(self) -> None:
        while True:
            try:
                await self.poll_once(datetime.now(UTC))
            except Exception:
                logger.exception("Watch poll failed")
                if self._failures is not None:
                    self._failures.inc()
            await asyncio.sleep(self.check_interval_seconds)
