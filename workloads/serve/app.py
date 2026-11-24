"""Ray Serve sentiment classifier: the latest checkpoint of a training job behind `POST /predict`.

Deployed by the platform as a RayService (`POST /services`). The serveConfigV2 calls `build(args)`
with `run_dir` (the training run's checkpoint dir) and `profile`, and sets the autoscaling config.

    curl -s localhost:8001/predict -d '{"text": "Great product, works perfectly."}'
    {"predictions": [{"label": "positive", "score": 0.97}], "checkpoint": "checkpoint_2026-..."}
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from profiles import MAX_LEN, PROFILES
from ray import serve
from starlette.requests import Request
from starlette.responses import JSONResponse

LABELS = ("negative", "positive")
MAX_TEXTS = 32


def latest_checkpoint(run_dir: Path) -> Path:
    """Ray Train names checkpoints checkpoint_<timestamp>/, so the last one sorted is the newest."""
    ckpts = sorted(p for p in run_dir.glob("checkpoint_2*") if p.is_dir())
    if not ckpts:
        raise FileNotFoundError(
            f"no checkpoint_*/ in {run_dir}: was the job trained with Phase 4+?"
        )
    return ckpts[-1]


@serve.deployment
class Sentiment:
    def __init__(self, run_dir: str, profile: str) -> None:
        import torch
        from transformers import AutoModelForSequenceClassification, AutoTokenizer

        torch.set_num_threads(1)  # several replicas share one worker pod's cores
        p = PROFILES[profile]
        ckpt = latest_checkpoint(Path(run_dir))
        state = torch.load(ckpt / "state.pt", map_location="cpu", weights_only=False)
        model = AutoModelForSequenceClassification.from_pretrained(p.model_dir, num_labels=2)
        model.load_state_dict(state["model"])
        self.model = model.eval()
        self.tokenizer = AutoTokenizer.from_pretrained(p.model_dir)
        self.checkpoint = ckpt.name
        self.epoch = state["epoch"]
        self._torch = torch

    def predict(self, texts: list[str]) -> list[dict[str, Any]]:
        enc = self.tokenizer(
            texts, truncation=True, max_length=MAX_LEN, padding=True, return_tensors="pt"
        )
        with self._torch.no_grad():
            probs = self.model(**enc).logits.softmax(-1)
        return [
            {"label": LABELS[int(i)], "score": round(float(row[i]), 4)}
            for row, i in zip(probs, probs.argmax(-1), strict=True)
        ]

    async def __call__(self, request: Request) -> JSONResponse:
        if request.url.path.rstrip("/") != "/predict" or request.method != "POST":
            info = {"checkpoint": self.checkpoint, "epoch": self.epoch, "usage": "POST /predict"}
            return JSONResponse(info, status_code=200 if request.method == "GET" else 405)
        try:
            body = await request.json()
            texts = body["texts"] if "texts" in body else [body["text"]]
            if not texts or len(texts) > MAX_TEXTS or not all(isinstance(t, str) for t in texts):
                raise ValueError
        except (ValueError, KeyError, TypeError):
            msg = f'send {{"text": "..."}} or {{"texts": [up to {MAX_TEXTS} strings]}}'
            return JSONResponse({"error": msg}, status_code=422)
        return JSONResponse({"predictions": self.predict(texts), "checkpoint": self.checkpoint})


def build(args: dict[str, str]) -> serve.Application:
    """Application builder named in serveConfigV2's import_path (`app:build`)."""
    return Sentiment.bind(args["run_dir"], args.get("profile", "tiny"))
