#!/usr/bin/env bash
# Kill one Ray worker pod of a training job right after its first checkpoint lands.
#
#   scripts/chaos_kill_worker.sh <team> <job-id> [timeout-seconds]
#
# The kind node's /shared is the repo's ./.shared, so the checkpoints Ray Train writes to
# /shared/checkpoints/<team>/<job-id>/checkpoint_<timestamp>/ are visible here on the host.
# The first one appears at the end of epoch 1, so the kill lands during epoch 2.
set -euo pipefail

team=${1:?usage: $0 <team> <job-id> [timeout-seconds]}
job=${2:?usage: $0 <team> <job-id> [timeout-seconds]}
timeout=${3:-600}
ctx=${KUBECTX:-kind-rkp}
root=$(cd "$(dirname "$0")/.." && pwd)
run_dir="$root/.shared/checkpoints/$team/$job"

echo "waiting up to ${timeout}s for the first checkpoint in $run_dir"
deadline=$((SECONDS + timeout))
until compgen -G "$run_dir/checkpoint_2*/" >/dev/null; do
  if ((SECONDS > deadline)); then
    echo "no checkpoint after ${timeout}s" >&2
    exit 1
  fi
  sleep 2
done
echo "checkpoint found: $(basename "$(compgen -G "$run_dir/checkpoint_2*/" | head -1)")"

pod=$(kubectl --context "$ctx" -n "$team" get pods \
  -l "rkp.io/job-id=$job,ray.io/node-type=worker" \
  -o jsonpath='{.items[0].metadata.name}')
if [[ -z $pod ]]; then
  echo "no worker pod found for job $job" >&2
  exit 1
fi
kubectl --context "$ctx" -n "$team" delete pod "$pod" --grace-period=0 --wait=false
echo "killed worker pod $pod"
