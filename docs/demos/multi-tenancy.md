# Demo: multi-tenancy (noisy neighbour)

Team-a submits more work than its quota holds, and team-b submits one job at the same time.
Team-a's extra jobs wait in the API's queue, and team-b's job is admitted straight away and succeeds.

Recorded 2026-11-24 on the dev laptop (single-node kind, KubeRay 1.7.2, Ray 2.59.0).

## How it works

Each team namespace has a `ResourceQuota` and a `LimitRange` ([`infra/k8s/teams/quotas.yaml`](../../infra/k8s/teams/quotas.yaml)):

| | team-a | team-b |
|---|---|---|
| `requests.cpu` | 2 | 2 |
| `requests.memory` | 4Gi | 4Gi |
| `count/rayjobs.ray.io` | 3 | 3 |

A RayJob is a gang of pods (head, workers and a submitter), but Kubernetes checks quota one pod at a time.
So the API admits **whole jobs** ([`platform_api/admission.py`](../../platform_api/admission.py)):

- A job bigger than the team's whole quota is **rejected** with a 422 when it's submitted.
- A job that doesn't fit right now is **QUEUED**. Nothing is created in Kubernetes, and the message shows what it needs and what's in use.
- Each reconcile (5s) admits queued jobs oldest-first while they fit, and stops at the first one that doesn't (FIFO, so a big job isn't starved).
- The ResourceQuota still enforces the same ceiling per pod, as a backstop.

A 1-worker job requests 0.8 CPU / 2504Mi (head 0.25 / 1536Mi, worker 0.5 / 768Mi, submitter 0.05 / 200Mi).
One fits in 4Gi and two don't, so each team runs one such job at a time.

## Run it

```bash
make up && make teams && make keys    # once
make api                              # in another terminal
scripts/noisy_neighbour.sh 3 60       # 3 team-a jobs that sleep 60s, plus 1 team-b hello job
```

## Output

Snapshots from one run (the full run polls every 15s; QUEUED = submit → Ray driver start, which includes cluster start-up):

```
== 17:01:34 team-a submits 3 jobs (sleep 60s, 1 worker each)
== 17:01:36 team-b submits 1 job
== +0s
  TEAM    ID        STATE      QUEUED   RAN  MESSAGE
  team-a  44cb1e1b  SUBMITTED       -     -
  team-a  79a1499d  QUEUED          -     -  waiting for quota: needs 0.8 CPU / 2504Mi, team-a uses 0.8 CPU / 2504Mi of 2 CPU / 4096Mi
  team-a  54a3ea77  QUEUED          -     -  waiting for quota: needs 0.8 CPU / 2504Mi, team-a uses 0.8 CPU / 2504Mi of 2 CPU / 4096Mi (1 qu
  team-b  d751a7ba  SUBMITTED       -     -
== +86s
  TEAM    ID        STATE      QUEUED   RAN  MESSAGE
  team-a  44cb1e1b  RUNNING       34s     -
  team-a  79a1499d  QUEUED          -     -  waiting for quota: needs 0.8 CPU / 2504Mi, team-a uses 0.8 CPU / 2504Mi of 2 CPU / 4096Mi
  team-a  54a3ea77  QUEUED          -     -  waiting for quota: needs 0.8 CPU / 2504Mi, team-a uses 0.8 CPU / 2504Mi of 2 CPU / 4096Mi (1 qu
  team-b  d751a7ba  SUCCEEDED     34s   35s  Job finished successfully.
== +152s
  TEAM    ID        STATE      QUEUED   RAN  MESSAGE
  team-a  44cb1e1b  SUCCEEDED     34s   63s  Job finished successfully.
  team-a  79a1499d  RUNNING      137s     -  admitted from queue
  team-a  54a3ea77  QUEUED          -     -  waiting for quota: needs 0.8 CPU / 2504Mi, team-a uses 0.8 CPU / 2504Mi of 2 CPU / 4096Mi
  team-b  d751a7ba  SUCCEEDED     34s   35s  Job finished successfully.
== +319s
  TEAM    ID        STATE      QUEUED   RAN  MESSAGE
  team-a  44cb1e1b  SUCCEEDED     34s   63s  Job finished successfully.
  team-a  79a1499d  SUCCEEDED    137s   62s  Job finished successfully.
  team-a  54a3ea77  SUCCEEDED    244s   62s  Job finished successfully.
  team-b  d751a7ba  SUCCEEDED     34s   35s  Job finished successfully.
== all done after 320s
```

**Result:** team-a's queue time grows with each job it adds (34s → 137s → 244s), while team-b's stays at 34s,
the same as an unloaded cluster. Team-b's job finished while two of team-a's jobs were still queued.

A job that could never fit is rejected when it's submitted:

```
$ uv run rkp submit --team team-a --no-wait --workers 4 --memory 2Gi workloads/hello.py
error: HTTP 422: job requests 2.3 CPU / 5832Mi but team-a's whole quota is 2 CPU / 4096Mi: use fewer workers or less memory
```

**Resources:** the node peaked at **5.20 GiB** (excluding page cache) with one cluster per team running at once.

## RBAC

The API's ServiceAccount `rkp-system/rkp-api` gets a ClusterRole that is bound only inside `team-a` and `team-b`
([`infra/k8s/teams/rbac.yaml`](../../infra/k8s/teams/rbac.yaml)). `make test-k8s` checks it with `kubectl auth can-i --as`:

| Can it… | |
|---|---|
| create / delete `rayjobs.ray.io` in team-a / team-b | yes |
| read `pods/log` and `resourcequotas` in team namespaces | yes |
| create RayJobs in `default` or `kube-system` | no |
| delete pods, read secrets, or update its own quota | no |

In dev the API runs on the host with your kubeconfig. The ServiceAccount is what it would use in-cluster.

## Reproduce as a test

`tests/test_k8s_e2e.py::test_noisy_neighbour_queues_team_a_but_not_team_b` does the same thing with 2 team-a jobs.
It checks that team-b SUCCEEDS while team-a's second job is still QUEUED (with no RayJob created), and that cancelling team-a's first job admits the queued one.
