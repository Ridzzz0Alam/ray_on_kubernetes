#!/usr/bin/env bash
# Multi-tenancy demo: team-a submits more than its quota holds, team-b submits one job.
#
#   make up && make teams && make keys     # once
#   make api                               # in another terminal
#   scripts/noisy_neighbour.sh [n-team-a-jobs] [sleep-seconds]
#
# Each 1-worker job requests 0.8 CPU / 2504Mi; a team's quota (4Gi) holds one at a time, so
# team-a's extra jobs wait QUEUED in the API while team-b's job is admitted straight away.
set -euo pipefail

n=${1:-3}
secs=${2:-60}
rkp() { uv run --quiet rkp "$@"; }

ids_a=()
echo "== $(date +%T) team-a submits $n jobs (sleep ${secs}s, 1 worker each)"
for _ in $(seq "$n"); do
  ids_a+=("$(rkp submit --team team-a --no-wait --workers 1 workloads/sleep.py -- "$secs")")
done
echo "== $(date +%T) team-b submits 1 job"
id_b=$(rkp submit --team team-b --no-wait --workers 1 workloads/hello.py)

summary() {
  for id in "${ids_a[@]}"; do rkp status --team team-a --json "$id"; done
  rkp status --team team-b --json "$id_b"
}

table() { summary | uv run --quiet python scripts/job_table.py; }

start=$SECONDS
while true; do
  echo "== +$((SECONDS - start))s"
  if table; then break; fi
  sleep 15
done
echo "== all done after $((SECONDS - start))s"
