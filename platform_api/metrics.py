"""Prometheus metrics computed from the job registry at scrape time.

The registry is the source of truth (it outlives RayJob CRs and API restarts), so nothing is
counted in process memory. Every value below is recomputed from SQLite on each scrape.

    rkp_jobs_total{team,state}            counter    jobs that ended SUCCEEDED/FAILED/CANCELLED
    rkp_jobs_active{team,state}           gauge      unfinished jobs (QUEUED ... RETRYING) now
    rkp_job_queue_seconds{team}           histogram  submit -> Ray driver start, per started job
    rkp_job_runtime_seconds{team}         histogram  driver start -> finish, per finished job
    rkp_job_last_queue_seconds{team}      gauge      queue time of the most recently started job
    rkp_job_oldest_waiting_seconds{team}  gauge      age of the oldest job not started (0 if none)
    rkp_services_live{team}               gauge      RayServices currently deployed
"""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Iterable, Iterator
from datetime import datetime

from prometheus_client.core import (
    CollectorRegistry,
    CounterMetricFamily,
    GaugeMetricFamily,
    HistogramMetricFamily,
    Metric,
)

from platform_api.models import TERMINAL, Job, JobState
from platform_api.registry import Registry, utcnow

QUEUE_BUCKETS = (10, 30, 60, 120, 300, 600, 1800, 3600)
RUNTIME_BUCKETS = (30, 60, 120, 300, 600, 1800, 3600, 7200)
ACTIVE = [s for s in JobState if s not in TERMINAL]


def _histogram(
    name: str, doc: str, values_by_team: dict[str, list[float]], buckets: tuple[int, ...]
) -> HistogramMetricFamily:
    h = HistogramMetricFamily(name, doc, labels=["team"])
    for team, values in sorted(values_by_team.items()):
        cumulative = [(str(float(b)), sum(1 for v in values if v <= b)) for b in buckets]
        h.add_metric([team], [*cumulative, ("+Inf", len(values))], sum(values))
    return h


class RegistryCollector:
    def __init__(self, registry: Registry, teams: Iterable[str]) -> None:
        self.registry = registry
        self.teams = sorted(set(teams))  # zero-filled, so panels show 0 rather than gaps

    def collect(self, now: datetime | None = None) -> Iterator[Metric]:
        now = now or utcnow()
        jobs = self.registry.all_jobs()
        teams = sorted(set(self.teams) | {j.team for j in jobs})

        done = CounterMetricFamily(
            "rkp_jobs", "Jobs that ended, by final state", labels=["team", "state"]
        )
        active = GaugeMetricFamily(
            "rkp_jobs_active", "Unfinished jobs by state", labels=["team", "state"]
        )
        counts: dict[tuple[str, str], int] = defaultdict(int)
        for j in jobs:
            counts[(j.team, j.state)] += 1
        for team in teams:
            for state in sorted(TERMINAL):
                done.add_metric([team, state], counts[(team, state)])
            for state in ACTIVE:
                active.add_metric([team, state], counts[(team, state)])
        yield done
        yield active

        queue: dict[str, list[float]] = {t: [] for t in teams}
        runtime: dict[str, list[float]] = {t: [] for t in teams}
        last_started: dict[str, Job] = {}
        oldest_wait: dict[str, float] = dict.fromkeys(teams, 0.0)
        for j in jobs:
            if j.started_at:
                queue[j.team].append(max(0.0, (j.started_at - j.submitted_at).total_seconds()))
                prev = last_started.get(j.team)
                if prev is None or prev.started_at < j.started_at:  # type: ignore[operator]
                    last_started[j.team] = j
                if j.finished_at:
                    runtime[j.team].append(max(0.0, (j.finished_at - j.started_at).total_seconds()))
            elif j.state not in TERMINAL:
                age = (now - j.submitted_at).total_seconds()
                oldest_wait[j.team] = max(oldest_wait[j.team], age)
        yield _histogram(
            "rkp_job_queue_seconds", "Submit to Ray driver start", queue, QUEUE_BUCKETS
        )
        yield _histogram(
            "rkp_job_runtime_seconds", "Ray driver start to finish", runtime, RUNTIME_BUCKETS
        )

        last = GaugeMetricFamily(
            "rkp_job_last_queue_seconds",
            "Queue time of the most recently started job",
            labels=["team"],
        )
        for team, j in sorted(last_started.items()):
            last.add_metric([team], (j.started_at - j.submitted_at).total_seconds())  # type: ignore[operator]
        yield last

        waiting = GaugeMetricFamily(
            "rkp_job_oldest_waiting_seconds",
            "Age of the oldest job not started yet",
            labels=["team"],
        )
        for team in teams:
            waiting.add_metric([team], oldest_wait[team])
        yield waiting

        services = GaugeMetricFamily("rkp_services_live", "Deployed RayServices", labels=["team"])
        live: dict[str, int] = defaultdict(int)
        for row in self.registry.live_services():
            live[row["team"]] += 1
        for team in sorted(set(teams) | set(live)):
            services.add_metric([team], live[team])
        yield services


def make_registry(registry: Registry, teams: Iterable[str]) -> CollectorRegistry:
    """A dedicated CollectorRegistry (not the global one), so tests and apps don't share state."""
    reg = CollectorRegistry(auto_describe=False)
    reg.register(RegistryCollector(registry, teams))  # type: ignore[arg-type]
    return reg
