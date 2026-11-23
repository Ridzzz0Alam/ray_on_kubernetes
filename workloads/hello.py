"""Smoke-test workload: spread a few tasks over the Ray workers and report where they ran."""

import socket
import time

import ray

ray.init()


@ray.remote(num_cpus=1)
def where(i: int) -> str:
    time.sleep(5)
    return f"task {i} ran on {socket.gethostname()}"


for line in ray.get([where.remote(i) for i in range(6)]):
    print(line)
nodes = [n for n in ray.nodes() if n["Alive"]]
print(f"alive nodes: {len(nodes)}, cluster CPUs: {ray.cluster_resources().get('CPU')}")
print("hello from rkp")
