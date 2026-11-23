"""One-time download + tokenization into /shared/data, so training pods never touch the network.

Run through `make data` (inside the Ray image, with ./.shared mounted at /shared).
"""

from __future__ import annotations

import argparse
import json
import time

import torch
from datasets import load_dataset
from profiles import DATASET, DATASET_REVISION, MAX_LEN, N_EVAL, N_TRAIN, PROFILES, Profile
from transformers import (
    AutoModelForSequenceClassification,
    AutoTokenizer,
    BertForSequenceClassification,
    BertTokenizerFast,
)


def take(split: str, n: int) -> tuple[list[str], list[int]]:
    """A shuffled subset, streamed so the full 3.6M rows are never downloaded."""
    stream = load_dataset(DATASET, split=split, revision=DATASET_REVISION, streaming=True)
    rows = list(stream.shuffle(seed=0, buffer_size=20_000).take(n))
    return [f"{r['title']}. {r['content']}" for r in rows], [int(r["label"]) for r in rows]


def prepare(profile: Profile) -> None:
    if (profile.data_dir / "meta.json").exists() and (profile.model_dir / "config.json").exists():
        print(f"already prepared: {profile.data_dir}, {profile.model_dir}")
        return
    t0 = time.monotonic()
    tok_cls, model_cls = (
        (BertTokenizerFast, BertForSequenceClassification)
        if profile.arch == "bert"
        else (AutoTokenizer, AutoModelForSequenceClassification)
    )
    tok = tok_cls.from_pretrained(profile.model, revision=profile.revision)
    model = model_cls.from_pretrained(profile.model, revision=profile.revision, num_labels=2)
    profile.model_dir.mkdir(parents=True, exist_ok=True)
    tok.save_pretrained(profile.model_dir)
    model.save_pretrained(profile.model_dir)

    profile.data_dir.mkdir(parents=True, exist_ok=True)
    for split, n in (("train", N_TRAIN), ("test", N_EVAL)):
        texts, labels = take(split, n)
        enc = tok(
            texts, truncation=True, max_length=MAX_LEN, padding="max_length", return_tensors="pt"
        )
        out = {
            "input_ids": enc["input_ids"],
            "attention_mask": enc["attention_mask"],
            "labels": torch.tensor(labels),
        }
        torch.save(out, profile.data_dir / f"{'eval' if split == 'test' else split}.pt")
        print(f"{split}: {len(labels)} rows, positive share {sum(labels) / len(labels):.2f}")
    meta = {
        "dataset": DATASET,
        "revision": DATASET_REVISION,
        "model": profile.model,
        "model_revision": profile.revision,
        "n_train": N_TRAIN,
        "n_eval": N_EVAL,
        "max_len": MAX_LEN,
    }
    (profile.data_dir / "meta.json").write_text(json.dumps(meta, indent=2))
    print(f"prepared {profile.data_dir} in {time.monotonic() - t0:.0f}s")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--profile", choices=sorted(PROFILES), default="tiny")
    prepare(PROFILES[ap.parse_args().profile])
