"""Runtime settings. Image and Ray versions come from infra/versions.env (the single source)."""

from __future__ import annotations

import os
import re
from dataclasses import dataclass
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
_TEAM = re.compile(r"[a-z0-9]([-a-z0-9]{0,30}[a-z0-9])?")  # must be a valid namespace name


def read_env_file(path: Path) -> dict[str, str]:
    pairs: dict[str, str] = {}
    for line in path.read_text().splitlines():
        line = line.strip()
        if line and not line.startswith("#"):
            key, _, value = line.partition("=")
            pairs[key.strip()] = value.strip()
    return pairs


def load_api_keys(path: Path) -> dict[str, str]:
    """Read `{teams: {team-a: <key>, ...}}` and return {key: team}."""
    if not path.exists():
        raise FileNotFoundError(f"{path} not found: run `make keys` to generate dev API keys")
    teams = (yaml.safe_load(path.read_text()) or {}).get("teams") or {}
    keys: dict[str, str] = {}
    for team, key in teams.items():
        if not _TEAM.fullmatch(team):
            raise ValueError(f"team {team!r} is not a valid namespace name")
        if not isinstance(key, str) or len(key) < 16:
            raise ValueError(f"API key for {team!r} must be a string of at least 16 chars")
        if key in keys:
            raise ValueError(f"teams {keys[key]!r} and {team!r} share an API key")
        keys[key] = team
    return keys


@dataclass(frozen=True)
class Settings:
    api_keys: dict[str, str]  # key -> team (= namespace)
    db_path: Path
    image: str
    ray_version: str
    kube_context: str | None = None
    reconcile_interval: float = 5.0
    # host:port for a second, metrics-only listener that Prometheus in kind can reach (make api).
    metrics_addr: str | None = None
    rates_path: Path = ROOT / "config" / "rates.yaml"

    @classmethod
    def from_env(cls) -> Settings:
        versions = read_env_file(ROOT / "infra" / "versions.env")
        keys_file = Path(os.environ.get("RKP_API_KEYS_FILE", ROOT / "config" / "api_keys.yaml"))
        return cls(
            api_keys=load_api_keys(keys_file),
            db_path=Path(os.environ.get("RKP_DB", ROOT / ".rkp" / "registry.db")),
            image=os.environ.get("RKP_IMAGE", versions["RKP_IMAGE"]),
            ray_version=versions["RAY_VERSION"],
            kube_context=os.environ.get("RKP_KUBE_CONTEXT", "kind-rkp") or None,
            reconcile_interval=float(os.environ.get("RKP_RECONCILE_INTERVAL", "5")),
            metrics_addr=os.environ.get("RKP_METRICS_ADDR") or None,
        )
