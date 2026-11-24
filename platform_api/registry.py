"""SQLite job registry: the durable record of every job. It outlives the short-lived RayJob CRs."""

from __future__ import annotations

import json
import sqlite3
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path

from platform_api.models import TERMINAL, Job, JobSpec, JobState

SCHEMA = """
CREATE TABLE IF NOT EXISTS jobs (
    id           TEXT PRIMARY KEY,
    team         TEXT NOT NULL,
    k8s_name     TEXT NOT NULL,
    spec         TEXT NOT NULL,
    state        TEXT NOT NULL,
    message      TEXT,
    submitted_at TEXT NOT NULL,
    started_at   TEXT,
    finished_at  TEXT,
    logs         TEXT
);
CREATE INDEX IF NOT EXISTS jobs_team ON jobs (team, submitted_at);
"""
_TERMINAL_SQL = ",".join(f"'{s}'" for s in sorted(TERMINAL))


def utcnow() -> datetime:
    return datetime.now(UTC)


class Registry:
    def __init__(self, path: Path) -> None:
        self.path = path
        path.parent.mkdir(parents=True, exist_ok=True)
        with self._conn() as c:
            c.execute("PRAGMA journal_mode=WAL")
            c.executescript(SCHEMA)

    @contextmanager
    def _conn(self) -> Iterator[sqlite3.Connection]:
        # One short-lived connection per call: safe across FastAPI's threadpool and the reconciler.
        conn = sqlite3.connect(self.path, timeout=10)
        conn.row_factory = sqlite3.Row
        try:
            with conn:
                yield conn
        finally:
            conn.close()

    @staticmethod
    def _to_job(row: sqlite3.Row) -> Job:
        spec = json.loads(row["spec"])
        spec.pop("files", None)
        return Job(
            id=row["id"],
            team=row["team"],
            k8s_name=row["k8s_name"],
            state=JobState(row["state"]),
            message=row["message"],
            spec=spec,
            submitted_at=datetime.fromisoformat(row["submitted_at"]),
            started_at=row["started_at"] and datetime.fromisoformat(row["started_at"]),
            finished_at=row["finished_at"] and datetime.fromisoformat(row["finished_at"]),
        )

    def insert(
        self,
        job_id: str,
        team: str,
        k8s_name: str,
        spec: JobSpec,
        state: JobState = JobState.SUBMITTED,
        message: str | None = None,
    ) -> Job:
        with self._conn() as c:
            c.execute(
                "INSERT INTO jobs (id, team, k8s_name, spec, state, message, submitted_at)"
                " VALUES (?, ?, ?, ?, ?, ?, ?)",
                (
                    job_id,
                    team,
                    k8s_name,
                    spec.model_dump_json(),
                    state,
                    message,
                    utcnow().isoformat(),
                ),
            )
        job = self.get(job_id)
        assert job is not None
        return job

    def get(self, job_id: str) -> Job | None:
        with self._conn() as c:
            row = c.execute("SELECT * FROM jobs WHERE id = ?", (job_id,)).fetchone()
        return self._to_job(row) if row else None

    def list(self, team: str, limit: int = 50) -> list[Job]:
        with self._conn() as c:
            rows = c.execute(
                "SELECT * FROM jobs WHERE team = ? ORDER BY submitted_at DESC LIMIT ?",
                (team, limit),
            ).fetchall()
        return [self._to_job(r) for r in rows]

    def active(self, team: str | None = None) -> list[Job]:
        """Non-terminal jobs (QUEUED included), oldest first."""
        sql = f"SELECT * FROM jobs WHERE state NOT IN ({_TERMINAL_SQL})"
        args: tuple[str, ...] = ()
        if team is not None:
            sql, args = sql + " AND team = ?", (team,)
        with self._conn() as c:
            rows = c.execute(sql + " ORDER BY submitted_at, rowid", args).fetchall()
        return [self._to_job(r) for r in rows]

    def spec(self, job_id: str) -> JobSpec:
        """The full submitted spec, files included (Job.spec leaves them out)."""
        with self._conn() as c:
            row = c.execute("SELECT spec FROM jobs WHERE id = ?", (job_id,)).fetchone()
        return JobSpec.model_validate_json(row["spec"])

    def transition(
        self,
        job_id: str,
        state: JobState,
        message: str | None = None,
        started_at: datetime | None = None,
        finished_at: datetime | None = None,
    ) -> bool:
        """Move a non-terminal job to `state`; returns True if it changed.

        Terminal states are final, so a cancel can't be overwritten by a reconciler that read
        the CR a moment earlier. started_at is set once: from `started_at` if given, else now
        when the job enters RUNNING. finished_at is set on entering a terminal state."""
        now = utcnow().isoformat()
        started = started_at.isoformat() if started_at else None
        finished = (finished_at.isoformat() if finished_at else now) if state in TERMINAL else None
        if started is None and state == JobState.RUNNING:
            started = now
        with self._conn() as c:
            cur = c.execute(
                f"""UPDATE jobs SET
                      state = ?,
                      message = COALESCE(?, message),
                      started_at = COALESCE(started_at, ?),
                      finished_at = COALESCE(?, finished_at)
                    WHERE id = ? AND state != ? AND state NOT IN ({_TERMINAL_SQL})""",
                (state, message, started, finished, job_id, state),
            )
        return cur.rowcount == 1

    def set_message(self, job_id: str, message: str) -> None:
        with self._conn() as c:
            c.execute("UPDATE jobs SET message = ? WHERE id = ?", (message, job_id))

    def set_logs(self, job_id: str, logs: str) -> None:
        with self._conn() as c:
            c.execute("UPDATE jobs SET logs = ? WHERE id = ?", (logs, job_id))

    def logs(self, job_id: str) -> str | None:
        with self._conn() as c:
            row = c.execute("SELECT logs FROM jobs WHERE id = ?", (job_id,)).fetchone()
        return row["logs"] if row else None
