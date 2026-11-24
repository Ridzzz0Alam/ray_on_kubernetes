"""Print `rkp status --json` objects (concatenated on stdin) as a table with queue/run times.

Exits 0 once every job is in a terminal state, 3 otherwise (scripts/noisy_neighbour.sh polls it).
"""

from __future__ import annotations

import json
import sys
from datetime import datetime

DONE = {"SUCCEEDED", "FAILED", "CANCELLED"}


def seconds(a: str | None, b: str | None) -> str:
    if not (a and b):
        return "-"
    return f"{(datetime.fromisoformat(b) - datetime.fromisoformat(a)).total_seconds():.0f}s"


def main() -> int:
    text, i, jobs = sys.stdin.read(), 0, []
    decoder = json.JSONDecoder()
    while i < len(text):
        if text[i].isspace():
            i += 1
            continue
        job, i = decoder.raw_decode(text, i)
        jobs.append(job)
    print(f"  {'TEAM':7} {'ID':9} {'STATE':10} {'QUEUED':>6} {'RAN':>5}  MESSAGE")
    for j in jobs:
        queued = seconds(j["submitted_at"], j["started_at"])
        ran = seconds(j["started_at"], j["finished_at"])
        msg = (j["message"] or "")[:95]
        print(f"  {j['team']:7} {j['id']:9} {j['state']:10} {queued:>6} {ran:>5}  {msg}")
    return 0 if all(j["state"] in DONE for j in jobs) else 3


if __name__ == "__main__":
    sys.exit(main())
