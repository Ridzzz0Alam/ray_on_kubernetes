# Demo: serving with autoscaling (Ray Serve)

A trained checkpoint is served as a `RayService` behind `POST /predict`. Ray Serve scales replicas from 1 to 3
under ramped load, then back to 1 once the load stops.

Recorded 2026-11-24 on the dev laptop (single-node kind, KubeRay 1.7.2, Ray 2.59.0, `bert-tiny`).

## How it works

- `POST /services {"name": "sentiment", "job_id": "<training job>"}`: the job must be the team's own and SUCCEEDED.
  The API builds a RayService ([`platform_api/manifests.py`](../../platform_api/manifests.py) `build_rayservice`)
  and ships [`workloads/serve/app.py`](../../workloads/serve/app.py) and `profiles.py` in a ConfigMap owned by the RayService.
- The app loads the **latest** `checkpoint_<ts>/state.pt` of that run from the team's checkpoint dir (mounted read-only).
- `serveConfigV2` autoscaling: `min_replicas: 1`, `max_replicas: 3`, `target_ongoing_requests: 2`,
  `upscale_delay_s: 5`, `downscale_delay_s: 30` (Ray's defaults are 30s and 600s, too slow for a demo).
- Replicas take 0.5 Ray CPU each and scale **inside one fixed 2-CPU worker pod**. There's no pod autoscaling on the laptop.
- A service counts against the team quota (head 0.25 CPU / 1536Mi + worker 1 CPU / 1024Mi of requests). It isn't
  queued: if the quota is busy you get a 409, and if it could never fit you get a 422.

## Run it

```bash
make up && make teams && make keys    # once
make api                              # in another terminal
uv run rkp submit --team team-a workloads/train --script train.py -- --profile tiny --epochs 2
#   e31235ff ... [136s] SUCCEEDED
curl -s -X POST localhost:8000/services -H "X-API-Key: $KEY" \
     -d '{"name": "sentiment", "job_id": "e31235ff"}'          # → DEPLOYING, RUNNING ~35s later
kubectl -n team-a port-forward svc/rkp-svc-sentiment-serve-svc 8001:8000 &
kubectl -n team-a port-forward svc/rkp-svc-sentiment-head-svc 8265:8265 &   # dashboard: replica count
uv run python scripts/load_test.py --stages 1:20,5:20,10:20,20:40 --cooldown 150
curl -s -X DELETE localhost:8000/services/sentiment -H "X-API-Key: $KEY"  # → DELETED, all pods gone
```

## Output

```
$ curl -s localhost:8001/predict -d '{"text": "Great product, works perfectly and arrived early."}'
{"predictions":[{"label":"positive","score":0.9779}],"checkpoint":"checkpoint_2026-11-24_08-35-24.778152"}
$ curl -s localhost:8001/
{"checkpoint":"checkpoint_2026-11-24_08-35-24.778152","epoch":2,"usage":"POST /predict"}
```

Load test (closed-loop clients: each one sends, waits for the reply, sends again):

```
replicas before load: 1
{"clients": 1, "seconds": 20.0, "requests": 1607, "rps": 80.3, "p50_ms": 12.0, "p95_ms": 15.7, "errors": 0, "replicas_min": 1, "replicas_max": 1}
{"clients": 5, "seconds": 20.0, "requests": 1819, "rps": 91.0, "p50_ms": 53.2, "p95_ms": 77.5, "errors": 0, "replicas_min": 1, "replicas_max": 1}
{"clients": 10, "seconds": 20.0, "requests": 1747, "rps": 87.3, "p50_ms": 108.3, "p95_ms": 177.9, "errors": 0, "replicas_min": 1, "replicas_max": 3}
{"clients": 20, "seconds": 40.0, "requests": 3775, "rps": 94.4, "p50_ms": 210.2, "p95_ms": 270.0, "errors": 0, "replicas_min": 3, "replicas_max": 3}
{"replicas_start": 1, "replicas_peak": 3, "scaled_down_after_s": 38, "replicas_end": 1, "scaled_up": true, "scaled_down": true}
```

Replica count over time:

```
t=   0.0s  clients= 1  replicas=1
t=  42.6s  clients=10  replicas=2
t=  54.9s  clients=10  replicas=3
t= 137.9s  clients= 0  replicas=1    (38s after the load stopped)
```

**Result:** replicas scaled **1 → 3** under load and **back to 1** 38s after the load stopped, with 0 errors across 8,948 requests.
p50 latency was **12 ms** with 1 client and 210 ms with 20. The node peaked at **5.06 GiB** (excluding page cache).

## What limits throughput here (measured)

Throughput stayed at about 80–95 req/s however many replicas ran, so latency grew with the client count. During a 20-client burst:

| Process / container | CPU |
|---|---|
| `ray::ProxyActor` (Serve's HTTP proxy, on the head) | **100%** (one core) |
| `ray-head` container (limit 1 CPU) | 98% |
| each `Sentiment` replica | 27–36% |

The bottleneck is the **single Serve HTTP proxy**. `kubectl port-forward` sends all traffic to one pod, so one Python
proxy handles every request. bert-tiny inference takes only a few ms, so that proxy saturates before the replicas do.
Autoscaling still works as designed, because Serve scales on in-flight requests and those queue up. But at this model
size, more replicas don't add capacity. To make replicas count you'd want heavier per-request work (DistilBERT, or `@serve.batch`), several
proxies behind a real load balancer instead of a port-forward, and a head with more than 1 CPU. These were left out of the
laptop build on purpose.

## Reproduce as a test

`tests/test_k8s_e2e.py::test_serve_latest_checkpoint_then_delete` trains 1 epoch, serves the checkpoint, calls `/predict`
through a port-forward, deletes the service and checks that no pods are left (about 3 min).
