"""A job that needs a (simulated) GPU: its Ray tasks request {"sim_gpu": 1}.

Submit with `gpus=1` (the API adds a gpu-sim worker that holds one example.com/gpu and
advertises Ray's sim_gpu resource):

    rkp submit workloads/gpu_sim.py --gpus 1 --workers 0 -- 60

Ray only places the tasks on a node that has sim_gpu, so they run on the gpu-sim worker.
"""

import socket
import sys
import time

import ray


@ray.remote(num_cpus=1, resources={"sim_gpu": 1})
def gpu_step(step: int, seconds: float) -> str:
    time.sleep(seconds)  # stand-in for a kernel; the point is *where* it runs and that it waits
    return socket.gethostname()


def main() -> None:
    seconds = float(sys.argv[1]) if len(sys.argv) > 1 else 60
    ray.init()
    print(f"cluster resources: {ray.cluster_resources()}", flush=True)
    steps = 6
    t0 = time.monotonic()
    for i in range(steps):
        host = ray.get(gpu_step.remote(i, seconds / steps))
        print(
            f"gpu step {i + 1}/{steps} ran on {host} after {time.monotonic() - t0:.0f}s", flush=True
        )
    print("done", flush=True)


if __name__ == "__main__":
    main()
