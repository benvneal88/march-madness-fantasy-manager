from __future__ import annotations

import threading
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime

from app.integrations.sportsreference import fetch_rosters_for_teams
from app.models.draft import populate_players_from_sportsref_roster


@dataclass
class RosterFetchJob:
    id: str
    draft_id: int
    status: str = "running"
    started_at: str = field(default_factory=lambda: datetime.utcnow().isoformat() + "Z")
    finished_at: str | None = None
    fetched_count: int = 0
    skipped_existing_count: int = 0
    unmatched_count: int = 0
    deferred_count: int = 0
    players_inserted: int = 0
    error: str | None = None


_job_lock = threading.Lock()
_jobs_by_draft: dict[int, RosterFetchJob] = {}


def _serialize(job: RosterFetchJob) -> dict[str, object]:
    return {
        "id": job.id,
        "draft_id": job.draft_id,
        "status": job.status,
        "started_at": job.started_at,
        "finished_at": job.finished_at,
        "fetched_count": job.fetched_count,
        "skipped_existing_count": job.skipped_existing_count,
        "unmatched_count": job.unmatched_count,
        "deferred_count": job.deferred_count,
        "players_inserted": job.players_inserted,
        "error": job.error,
    }


def get_roster_fetch_job_status(draft_id: int) -> dict[str, object] | None:
    with _job_lock:
        job = _jobs_by_draft.get(draft_id)
        if not job:
            return None
        return _serialize(job)


def _run_roster_fetch_job(
    job: RosterFetchJob,
    main_db_url: str,
    database_name: str,
    season_year: int,
    team_names: list[str],
) -> None:
    draft_pending = [name for name in team_names]
    draft_pending = [name for name in draft_pending if name]

    try:
        while draft_pending:
            fetch_result = fetch_rosters_for_teams(
                db_url=main_db_url,
                team_names=draft_pending,
                season_year=season_year,
                max_rosters_per_run=5,
            )

            fetched = fetch_result.get("fetched", [])
            skipped = fetch_result.get("skipped_existing", [])
            unmatched = fetch_result.get("unmatched", [])
            deferred = fetch_result.get("deferred", [])

            with _job_lock:
                job.fetched_count += len(fetched)
                job.skipped_existing_count += len(skipped)
                job.unmatched_count += len(unmatched)
                job.deferred_count = len(deferred)

            # Continue only with deferred teams in the next batch.
            draft_pending = [name for name in deferred if name]

            # If all remaining teams were deferred (rate limit currently full),
            # wait briefly to avoid a tight loop and allow slots to free up.
            if draft_pending and not fetched:
                time.sleep(2)

        players_inserted = populate_players_from_sportsref_roster(
            main_db_url,
            database_name,
            season_year,
        )

        with _job_lock:
            job.players_inserted = players_inserted
            job.deferred_count = 0
            job.status = "completed"
            job.finished_at = datetime.utcnow().isoformat() + "Z"
    except Exception as exc:  # noqa: BLE001
        with _job_lock:
            job.error = str(exc)
            job.status = "failed"
            job.finished_at = datetime.utcnow().isoformat() + "Z"


def start_roster_fetch_job(
    draft_id: int,
    main_db_url: str,
    database_name: str,
    season_year: int,
    team_names: list[str],
) -> tuple[dict[str, object], bool]:
    with _job_lock:
        existing = _jobs_by_draft.get(draft_id)
        if existing and existing.status == "running":
            return _serialize(existing), False

        job = RosterFetchJob(id=uuid.uuid4().hex, draft_id=draft_id)
        _jobs_by_draft[draft_id] = job

    worker = threading.Thread(
        target=_run_roster_fetch_job,
        args=(job, main_db_url, database_name, season_year, team_names),
        daemon=True,
    )
    worker.start()

    return _serialize(job), True
