"""Model profiles and data locations shared by prepare_data.py (run once) and train.py.

Revisions are pinned so the data and weights are reproducible.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

DATA_ROOT = Path(os.environ.get("RKP_DATA", "/shared/data"))

DATASET = "fancyzhx/amazon_polarity"
DATASET_REVISION = "9d9c45c18f8c3cf1b23a3c27917b60cbf28f3289"
N_TRAIN, N_EVAL, MAX_LEN = 4000, 1000, 128


@dataclass(frozen=True)
class Profile:
    model: str
    revision: str
    batch_size: int
    lr: float
    # "bert" for repos without model_type/tokenizer config (bert-tiny), else "auto".
    # prepare_data.py re-saves the weights with a full config, so train.py always uses Auto*.
    arch: str = "auto"

    @property
    def model_dir(self) -> Path:
        return DATA_ROOT / "models" / self.model.replace("/", "--")

    @property
    def data_dir(self) -> Path:
        tokenizer = self.model.split("/")[-1]
        return DATA_ROOT / f"amazon_polarity-{N_TRAIN}-{N_EVAL}-{MAX_LEN}-{tokenizer}"


PROFILES = {
    # ~4.4M params: the default, so a 2-epoch run fits in a few minutes on 2 CPU cores.
    "tiny": Profile(
        "prajjwal1/bert-tiny", "6f75de8b60a9f8a2fdf7b69cbd86d9e64bcb3837", 32, 1e-3, arch="bert"
    ),
    # ~66M params: opt-in only, much slower on CPU and needs ~2.5Gi per worker.
    "distilbert": Profile(
        "distilbert/distilbert-base-uncased", "12040accade4e8a0f71eabdb258fecc2e7e948be", 16, 5e-5
    ),
}
