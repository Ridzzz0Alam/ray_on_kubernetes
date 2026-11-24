"""Distributed sentiment fine-tuning with Ray Train (PyTorch DDP on CPU / gloo).

Submit with:
    rkp submit workloads/train --script train.py -- --profile tiny --epochs 2

Needs `make data` first: it reads pre-tokenized tensors and weights from /shared/data,
with no network access.
"""

from __future__ import annotations

import argparse
import os
import tempfile
import time
from typing import Any

import ray.train
import ray.train.torch
from profiles import PROFILES
from ray.train import CheckpointConfig, FailureConfig, RunConfig, ScalingConfig
from ray.train.torch import TorchTrainer


def train_loop(config: dict[str, Any]) -> None:
    import torch
    import torch.distributed as dist
    from torch.utils.data import DataLoader, TensorDataset
    from transformers import AutoModelForSequenceClassification

    torch.set_num_threads(1)  # 2 workers on 2 physical cores: don't oversubscribe
    torch.manual_seed(config["seed"])
    profile = PROFILES[config["profile"]]
    ctx = ray.train.get_context()
    rank = ctx.get_world_rank()

    def loader(split: str, shuffle: bool) -> DataLoader:
        t = torch.load(profile.data_dir / f"{split}.pt")
        ds = TensorDataset(t["input_ids"], t["attention_mask"], t["labels"])
        dl = DataLoader(ds, batch_size=profile.batch_size, shuffle=shuffle)
        return ray.train.torch.prepare_data_loader(dl)  # adds a DistributedSampler

    train_dl, eval_dl = loader("train", True), loader("eval", False)
    model = AutoModelForSequenceClassification.from_pretrained(profile.model_dir, num_labels=2)
    model = ray.train.torch.prepare_model(model)  # wraps in DDP
    unwrapped = model.module if hasattr(model, "module") else model  # what gets checkpointed
    optim = torch.optim.AdamW(model.parameters(), lr=profile.lr, weight_decay=0.01)
    total_steps = config["epochs"] * len(train_dl)
    sched = torch.optim.lr_scheduler.LambdaLR(optim, lambda s: max(0.0, 1 - s / total_steps))

    # A restarted worker group (FailureConfig) or a relaunched driver (RayJob backoffLimit, same
    # RunConfig name) both get the latest reported checkpoint here.
    start_epoch = 1
    ckpt = ray.train.get_checkpoint()
    if ckpt:
        with ckpt.as_directory() as d:
            state = torch.load(os.path.join(d, "state.pt"), weights_only=False)
        unwrapped.load_state_dict(state["model"])
        optim.load_state_dict(state["optim"])
        sched.load_state_dict(state["sched"])
        start_epoch = state["epoch"] + 1
        if rank == 0:
            print(f"[rank 0] resumed from checkpoint at epoch {state['epoch']}", flush=True)

    for epoch in range(start_epoch, config["epochs"] + 1):
        t0 = time.monotonic()
        if hasattr(train_dl.sampler, "set_epoch"):
            train_dl.sampler.set_epoch(epoch)
        model.train()
        # [train_loss_sum, train_n, eval_loss_sum, eval_correct, eval_n], summed over workers
        stats = torch.zeros(5)
        for ids, mask, labels in train_dl:
            out = model(input_ids=ids, attention_mask=mask, labels=labels)
            optim.zero_grad()
            out.loss.backward()
            optim.step()
            sched.step()
            stats[0] += out.loss.item() * len(labels)
            stats[1] += len(labels)

        model.eval()
        with torch.no_grad():
            for ids, mask, labels in eval_dl:
                out = model(input_ids=ids, attention_mask=mask, labels=labels)
                stats[2] += out.loss.item() * len(labels)
                stats[3] += (out.logits.argmax(-1) == labels).sum().item()
                stats[4] += len(labels)
        if dist.is_initialized():
            dist.all_reduce(stats)

        metrics = {
            "epoch": epoch,
            "train_loss": round(float(stats[0] / stats[1]), 4),
            "eval_loss": round(float(stats[2] / stats[4]), 4),
            "eval_accuracy": round(float(stats[3] / stats[4]), 4),
            "epoch_seconds": round(time.monotonic() - t0, 1),
        }
        if rank != 0:
            ray.train.report(metrics)  # every worker must call report; only rank 0 saves
            continue
        print(f"[rank 0] {metrics}", flush=True)
        with tempfile.TemporaryDirectory() as d:
            state = {
                "model": unwrapped.state_dict(),
                "optim": optim.state_dict(),
                "sched": sched.state_dict(),
                "epoch": epoch,
            }
            torch.save(state, os.path.join(d, "state.pt"))
            ray.train.report(metrics, checkpoint=ray.train.Checkpoint.from_directory(d))


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--profile", choices=sorted(PROFILES), default="tiny")
    ap.add_argument("--epochs", type=int, default=2)
    ap.add_argument("--workers", type=int, default=2)
    ap.add_argument("--seed", type=int, default=0)
    ap.add_argument(
        "--sim-gpu",
        action="store_true",
        help='each Train worker also requests {"sim_gpu": 1} (submit with gpus=1)',
    )
    args = ap.parse_args()

    profile = PROFILES[args.profile]
    if not (profile.data_dir / "meta.json").exists():
        raise SystemExit(
            f"{profile.data_dir} missing: run `make data PROFILE={args.profile}` first"
        )

    # The platform mounts the team's checkpoint dir at /shared/checkpoints and sets RKP_JOB_ID.
    storage = os.environ.get("RKP_STORAGE", "/shared/checkpoints")
    name = os.environ.get("RKP_JOB_ID", f"local-{int(time.time())}")
    trainer = TorchTrainer(
        train_loop,
        train_loop_config={"profile": args.profile, "epochs": args.epochs, "seed": args.seed},
        scaling_config=ScalingConfig(
            num_workers=args.workers,
            use_gpu=False,  # simulated GPU: placement by the sim_gpu resource, torch on CPU
            resources_per_worker={"CPU": 1, **({"sim_gpu": 1} if args.sim_gpu else {})},
        ),
        run_config=RunConfig(
            name=name,
            storage_path=storage,
            checkpoint_config=CheckpointConfig(num_to_keep=2),
            failure_config=FailureConfig(max_failures=3),  # worker/node loss: restart the group
        ),
    )
    t0 = time.monotonic()
    result = trainer.fit()
    print(f"final metrics: {result.metrics}")
    print(f"checkpoint: {result.checkpoint.path if result.checkpoint else None}")
    print(f"training wall time: {time.monotonic() - t0:.0f}s")


if __name__ == "__main__":
    main()
