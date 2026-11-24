"""Ramp concurrent clients against a served model and watch Ray Serve scale replicas.

    kubectl -n team-a port-forward svc/rkp-svc-sentiment-serve-svc 8001:8000 &
    kubectl -n team-a port-forward svc/<ray-cluster>-head-svc 8265:8265 &   # dashboard
    uv run python scripts/load_test.py --stages 1:20,5:20,10:20,20:40 --cooldown 120

Each stage runs N closed-loop clients (send, wait for the reply, send again) for S seconds. The
replica count comes from the Ray dashboard's Serve API. After the last stage the load stops and
the script waits for replicas to scale back down to the minimum.
"""

from __future__ import annotations

import argparse
import asyncio
import itertools
import json
import statistics
import time
from dataclasses import dataclass, field

import httpx

REVIEWS = [
    "Great product, works perfectly and arrived early.",
    "Terrible quality. It broke after two days and support never answered.",
    "Does what it says. Not amazing, not bad.",
    "Absolutely love it, I bought a second one for my sister.",
    "Waste of money. The battery dies within an hour.",
    "The book was gripping from the first page to the last.",
    "Instructions were confusing and two parts were missing.",
    "Five stars, would recommend to anyone.",
]


@dataclass
class Stage:
    clients: int
    seconds: float
    latencies: list[float] = field(default_factory=list)
    errors: int = 0
    replicas: list[int] = field(default_factory=list)

    def summary(self) -> dict[str, object]:
        lat = self.latencies
        if len(lat) >= 2:
            q = statistics.quantiles(lat, n=100, method="inclusive")
            p50, p95 = q[49], q[94]
        else:
            p50 = p95 = lat[0] if lat else 0.0
        return {
            "clients": self.clients,
            "seconds": self.seconds,
            "requests": len(lat),
            "rps": round(len(lat) / self.seconds, 1),
            "p50_ms": round(p50 * 1000, 1),
            "p95_ms": round(p95 * 1000, 1),
            "errors": self.errors,
            "replicas_min": min(self.replicas, default=0),
            "replicas_max": max(self.replicas, default=0),
        }


async def replica_count(http: httpx.AsyncClient, dashboard: str, app: str, deployment: str) -> int:
    r = await http.get(f"{dashboard}/api/serve/applications/", timeout=5)
    r.raise_for_status()
    dep = r.json()["applications"][app]["deployments"][deployment]
    return sum(1 for rep in dep.get("replicas", []) if rep.get("state") == "RUNNING")


async def client_loop(http: httpx.AsyncClient, url: str, stage: Stage, stop: float, i: int) -> None:
    for text in itertools.islice(itertools.cycle(REVIEWS), i, None):
        if time.monotonic() >= stop:
            return
        t0 = time.monotonic()
        try:
            r = await http.post(url, json={"text": text}, timeout=30)
            r.raise_for_status()
            stage.latencies.append(time.monotonic() - t0)
        except httpx.HTTPError:
            stage.errors += 1
            await asyncio.sleep(0.2)


async def watch_replicas(
    http: httpx.AsyncClient, args: argparse.Namespace, sink: list[Stage], timeline: list
) -> None:
    start = time.monotonic()
    while True:
        try:
            n = await replica_count(http, args.dashboard, args.app, args.deployment)
            sink[-1].replicas.append(n)
            timeline.append((round(time.monotonic() - start, 1), sink[-1].clients, n))
        except (httpx.HTTPError, KeyError):
            pass
        await asyncio.sleep(args.poll)


async def run(args: argparse.Namespace) -> dict[str, object]:
    stages = [Stage(int(c), float(s)) for c, s in (x.split(":") for x in args.stages.split(","))]
    limits = httpx.Limits(max_connections=max(s.clients for s in stages) + 2)
    timeline: list[tuple[float, int, int]] = []
    current: list[Stage] = [Stage(0, 0)]
    async with httpx.AsyncClient(limits=limits) as http:
        current[0].replicas.append(
            await replica_count(http, args.dashboard, args.app, args.deployment)
        )
        print(f"replicas before load: {current[0].replicas[0]}", flush=True)
        watcher = asyncio.create_task(watch_replicas(http, args, current, timeline))
        results = []
        for stage in stages:
            current.append(stage)
            stop = time.monotonic() + stage.seconds
            await asyncio.gather(
                *(client_loop(http, args.url, stage, stop, i) for i in range(stage.clients))
            )
            results.append(stage.summary())
            print(json.dumps(results[-1]), flush=True)

        # Cool-down: no load; wait for Serve to scale back to the minimum.
        cool = Stage(0, args.cooldown)
        current.append(cool)
        t0, back_after = time.monotonic(), None
        while time.monotonic() - t0 < args.cooldown:
            await asyncio.sleep(args.poll)
            if cool.replicas and cool.replicas[-1] <= current[0].replicas[0]:
                back_after = round(time.monotonic() - t0)
                break
        watcher.cancel()
    peak = max(r["replicas_max"] for r in results)  # type: ignore[type-var]
    verdict = {
        "replicas_start": current[0].replicas[0],
        "replicas_peak": peak,
        "scaled_down_after_s": back_after,
        "replicas_end": cool.replicas[-1] if cool.replicas else None,
        "scaled_up": peak >= 2,  # type: ignore[operator]
        "scaled_down": back_after is not None,
    }
    print(json.dumps(verdict), flush=True)
    return {"stages": results, "verdict": verdict, "timeline": timeline}


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawTextHelpFormatter)
    ap.add_argument("--url", default="http://127.0.0.1:8001/predict")
    ap.add_argument("--dashboard", default="http://127.0.0.1:8265")
    ap.add_argument("--app", default="sentiment")
    ap.add_argument("--deployment", default="Sentiment")
    ap.add_argument("--stages", default="1:20,5:20,10:20,20:40", help="clients:seconds,...")
    ap.add_argument(
        "--cooldown", type=float, default=120, help="max seconds to wait for scale-down"
    )
    ap.add_argument("--poll", type=float, default=2.0, help="replica poll interval (s)")
    ap.add_argument("--out", help="write stages, verdict and replica timeline as JSON")
    args = ap.parse_args()
    report = asyncio.run(run(args))
    if args.out:
        with open(args.out, "w") as f:
            json.dump(report, f, indent=2)
    v = report["verdict"]
    raise SystemExit(0 if v["scaled_up"] and v["scaled_down"] else 1)  # type: ignore[index]


if __name__ == "__main__":
    main()
