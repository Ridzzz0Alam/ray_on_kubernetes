"""Per-team usage and cost from the job registry: CPU-hours and GB-hours x rates.

What is billed is what a team *reserves*: the pod requests that count against its quota
(`job_requests` / `service_requests`), for as long as the work holds them:
  - a job from Ray driver start to finish (or now, if it's still running);
  - a service from creation to deletion (or now).
Only the part inside the report window [since, now] counts. Queued jobs hold nothing, so they cost
nothing. GB means GiB (memory requests are in Mi).
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path

import yaml
from pydantic import BaseModel, Field

from platform_api.manifests import job_requests, service_requests
from platform_api.models import JobSpec, Resources, ServiceSpec
from platform_api.registry import Registry

_SINCE = re.compile(r"(\d+)([mhdw])")
_UNITS = {"m": "minutes", "h": "hours", "d": "days", "w": "weeks"}


@dataclass(frozen=True)
class Rates:
    cpu_hour: float
    gb_hour: float
    currency: str = "USD"

    @classmethod
    def load(cls, path: Path) -> Rates:
        data = yaml.safe_load(path.read_text())
        return cls(
            float(data["cpu_hour"]), float(data["gb_hour"]), str(data.get("currency", "USD"))
        )


def parse_since(value: str) -> timedelta:
    """'90m', '24h', '7d', '2w' -> timedelta."""
    m = _SINCE.fullmatch(value.strip())
    if not m or int(m[1]) == 0:
        raise ValueError(f"since must look like 90m, 24h, 7d or 2w, got {value!r}")
    return timedelta(**{_UNITS[m[2]]: int(m[1])})


class CostLine(BaseModel):
    kind: str  # "job" | "service"
    id: str
    hours: float
    cpu_hours: float
    gb_hours: float
    cost: float


class TeamCost(BaseModel):
    team: str
    since: datetime
    until: datetime
    cpu_hours: float
    gb_hours: float
    cost: float
    currency: str
    rates: dict[str, float]
    lines: list[CostLine] = Field(default_factory=list)


def _overlap_hours(start: datetime, end: datetime, since: datetime, until: datetime) -> float:
    return max(0.0, (min(end, until) - max(start, since)).total_seconds()) / 3600


def _line(kind: str, ident: str, req: Resources, hours: float, rates: Rates) -> CostLine:
    cpu_h = req.cpu_m / 1000 * hours
    gb_h = req.mem_mib / 1024 * hours
    return CostLine(
        kind=kind,
        id=ident,
        hours=round(hours, 4),
        cpu_hours=round(cpu_h, 4),
        gb_hours=round(gb_h, 4),
        cost=round(cpu_h * rates.cpu_hour + gb_h * rates.gb_hour, 6),
    )


def team_cost(
    registry: Registry, team: str, since: datetime, until: datetime, rates: Rates
) -> TeamCost:
    lines: list[CostLine] = []
    for job in registry.all_jobs():
        if job.team != team or job.started_at is None:
            continue
        hours = _overlap_hours(job.started_at, job.finished_at or until, since, until)
        if hours > 0:
            req = job_requests(JobSpec.model_validate(job.spec))
            lines.append(_line("job", job.id, req, hours, rates))
    for row in registry.services(team):
        created = datetime.fromisoformat(row["created_at"])
        deleted = datetime.fromisoformat(row["deleted_at"]) if row["deleted_at"] else until
        hours = _overlap_hours(created, deleted, since, until)
        if hours > 0:
            spec = ServiceSpec.model_validate_json(row["spec"])
            lines.append(_line("service", spec.name, service_requests(spec), hours, rates))
    cpu_h = sum(x.cpu_hours for x in lines)
    gb_h = sum(x.gb_hours for x in lines)
    return TeamCost(
        team=team,
        since=since,
        until=until,
        cpu_hours=round(cpu_h, 4),
        gb_hours=round(gb_h, 4),
        cost=round(sum(x.cost for x in lines), 4),
        currency=rates.currency,
        rates={"cpu_hour": rates.cpu_hour, "gb_hour": rates.gb_hour},
        lines=lines,
    )
