#!/usr/bin/env bash
# GPU-awareness demo: the node has one simulated GPU (`make gpu-sim`, example.com/gpu: 1).
#
#   make up && make teams && make keys     # `make teams` also advertises the sim GPU
#   make api                               # in another terminal
#   scripts/gpu_contention.sh [gpu-seconds]
#
# team-a's GPU job takes the GPU. team-b's GPU job is admitted (team-b has GPU quota) but its
# gpu-sim worker pod stays Pending at the scheduler until the GPU frees up. team-b's CPU job
# is scheduled and finishes in the meantime: CPU work isn't blocked by GPU contention.
set -euo pipefail

secs=${1:-90}
rkp() { uv run --quiet rkp "$@"; }
# Sizes: team-b must hold its GPU job and its CPU job at once inside a 4Gi requests quota, and
# each Ray head reserves 1536Mi. GPU jobs: 640Mi workers (a Ray worker node's own processes use
# ~0.45 GB; at 512Mi Ray's memory monitor kept killing the GPU task). CPU job: 512Mi (hello is
# tiny). 2056Mi + 1992Mi = 4048Mi <= 4096Mi.
gpu=(--no-wait --memory 640Mi --workers 0 --gpus 1)
cpu=(--no-wait --memory 512Mi --workers 1)

echo "== $(date +%T) team-a: GPU job (holds the GPU ~${secs}s)"
a_gpu=$(rkp submit --team team-a "${gpu[@]}" workloads/gpu_sim.py -- "$secs")
sleep 20
echo "== $(date +%T) team-b: GPU job (30s) + CPU job"
b_gpu=$(rkp submit --team team-b "${gpu[@]}" workloads/gpu_sim.py -- 30)
b_cpu=$(rkp submit --team team-b "${cpu[@]}" workloads/hello.py)

summary() {
  rkp status --team team-a --json "$a_gpu"
  rkp status --team team-b --json "$b_gpu"
  rkp status --team team-b --json "$b_cpu"
}

start=$SECONDS
while true; do
  echo "== +$((SECONDS - start))s"
  kubectl --context kind-rkp get pods -A -l ray.io/group=gpu-sim --no-headers \
    -o custom-columns='  POD:.metadata.name,NS:.metadata.namespace,PHASE:.status.phase' 2>/dev/null || true
  if summary | uv run --quiet python scripts/job_table.py; then break; fi
  sleep 15
done
echo "== all done after $((SECONDS - start))s"
for id in "$a_gpu" "$b_gpu"; do
  team="team-a"
  [[ $id == "$b_gpu" ]] && team="team-b"
  echo "== $team $id: where the GPU steps ran"
  rkp logs --team "$team" "$id" | grep -E "sim_gpu|gpu step" | sed 's/^/  /' | head -4
done
