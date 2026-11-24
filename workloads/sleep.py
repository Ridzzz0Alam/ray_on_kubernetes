"""A job that just holds its cluster for N seconds (default 120): fills a team's quota in demos."""

import sys
import time

seconds = int(sys.argv[1]) if len(sys.argv) > 1 else 120
for i in range(seconds):
    if i % 10 == 0:
        print(f"sleeping {i}/{seconds}s", flush=True)
    time.sleep(1)
print("done", flush=True)
