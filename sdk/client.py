"""Thin Python client for the rkp platform API."""

from __future__ import annotations

import os
import time
import tomllib
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any

import httpx

TERMINAL = {"SUCCEEDED", "FAILED", "CANCELLED"}
MAX_CODE_BYTES = 512 * 1024
DEFAULT_URL = "http://127.0.0.1:8000"
# The API's 409 hint when quota is held only by pods of finished work that are still terminating.
TRANSIENT_BUSY = "retry in a few seconds"


class RkpError(Exception):
    def __init__(self, status: int, detail: str) -> None:
        super().__init__(f"HTTP {status}: {detail}")
        self.status = status
        self.detail = detail


def config_path() -> Path:
    """RKP_CONFIG, else ~/.config/rkp.toml, else the repo's dev config (written by `make keys`)."""
    if env := os.environ.get("RKP_CONFIG"):
        return Path(env)
    home = Path.home() / ".config" / "rkp.toml"
    return home if home.exists() else Path("config") / "rkp.toml"


def configured_teams() -> list[str]:
    """Teams that have a key in the config file (the dev config has team-a and team-b)."""
    path = config_path()
    cfg: dict[str, Any] = tomllib.loads(path.read_text()) if path.exists() else {}
    return sorted(cfg.get("keys") or {}) or [t for t in [cfg.get("default_team")] if t]


def resolve(team: str | None = None) -> tuple[str, str]:
    """Return (api_url, api_key). Env vars RKP_API_URL / RKP_API_KEY win over the config file."""
    path = config_path()
    cfg: dict[str, Any] = tomllib.loads(path.read_text()) if path.exists() else {}
    url = os.environ.get("RKP_API_URL") or cfg.get("api_url") or DEFAULT_URL
    key = os.environ.get("RKP_API_KEY")
    if not key:
        team = team or cfg.get("default_team")
        key = (cfg.get("keys") or {}).get(team) if team else None
    if not key:
        raise RkpError(401, f"no API key for team {team!r}: set RKP_API_KEY or add it to {path}")
    return url, key


def load_code(path: Path) -> dict[str, str]:
    """A script file, or a directory of flat files (subdirectories and dotfiles are skipped)."""
    files = [path] if path.is_file() else sorted(p for p in path.iterdir() if p.is_file())
    code = {p.name: p.read_text() for p in files if not p.name.startswith(".")}
    if sum(len(c.encode()) for c in code.values()) > MAX_CODE_BYTES:
        raise ValueError(f"{path} is larger than {MAX_CODE_BYTES // 1024} KiB")
    return code


class Client:
    def __init__(self, url: str, api_key: str, *, http: httpx.Client | None = None) -> None:
        """`http` lets tests pass in-process clients (e.g. FastAPI's TestClient)."""
        self._http = http or httpx.Client(base_url=url, timeout=30)
        self._http.headers["X-API-Key"] = api_key

    @classmethod
    def from_config(cls, team: str | None = None) -> Client:
        return cls(*resolve(team))

    def _call(self, method: str, path: str, **kwargs: Any) -> httpx.Response:
        r = self._http.request(method, path, **kwargs)
        if r.is_error:
            try:
                detail = r.json().get("detail", r.text)
            except ValueError:
                detail = r.text
            raise RkpError(r.status_code, str(detail))
        return r

    def submit(self, entrypoint: str, files: dict[str, str] | None = None, **spec: Any) -> dict:
        body = {"entrypoint": entrypoint, "files": files or {}, **spec}
        return self._call("POST", "/jobs", json=body).json()

    def get(self, job_id: str) -> dict:
        return self._call("GET", f"/jobs/{job_id}").json()

    def list(self, limit: int = 50) -> list[dict]:
        return self._call("GET", "/jobs", params={"limit": limit}).json()

    def logs(self, job_id: str, tail: int | None = None) -> str:
        params = {"tail": tail} if tail else {}
        return self._call("GET", f"/jobs/{job_id}/logs", params=params).text

    def cost(self, since: str = "7d") -> dict:
        """Usage and cost of this key's team over the last `since` (90m, 24h, 7d, 2w)."""
        return self._call("GET", "/cost", params={"since": since}).json()

    def cancel(self, job_id: str) -> dict:
        return self._call("DELETE", f"/jobs/{job_id}").json()

    def serve(
        self,
        name: str,
        job_id: str,
        *,
        retry_for: float = 120,
        poll: float = 3.0,
        on_retry: Callable[[str], None] | None = None,
        **spec: Any,
    ) -> dict:
        """Create a service. A 409 saying "retry in a few seconds" (a finished job's pods still
        hold quota) is retried for up to `retry_for` seconds; any other error is raised."""
        deadline = time.monotonic() + retry_for
        body = {"name": name, "job_id": job_id, **spec}
        while True:
            try:
                return self._call("POST", "/services", json=body).json()
            except RkpError as e:
                if e.status != 409 or TRANSIENT_BUSY not in e.detail:
                    raise
                if time.monotonic() + poll > deadline:
                    raise
                if on_retry:
                    on_retry(e.detail)
                time.sleep(poll)

    def service(self, name: str) -> dict:
        return self._call("GET", f"/services/{name}").json()

    def services(self) -> list[dict]:
        return self._call("GET", "/services").json()

    def unserve(self, name: str) -> dict:
        return self._call("DELETE", f"/services/{name}").json()

    def watch_service(self, name: str, poll: float = 5.0, timeout: float = 600) -> Iterator[dict]:
        """Yield the service each time its state changes, until it leaves DEPLOYING."""
        deadline = time.monotonic() + timeout
        last = None
        while True:
            svc = self.service(name)
            if svc["state"] != last:
                last = svc["state"]
                yield svc
            if last != "DEPLOYING":
                return
            if time.monotonic() > deadline:
                raise TimeoutError(f"service {name} still DEPLOYING after {timeout:.0f}s")
            time.sleep(poll)

    def watch(self, job_id: str, poll: float = 2.0, timeout: float = 1800) -> Iterator[dict]:
        """Yield the job each time its state changes, until it reaches a terminal state."""
        deadline = time.monotonic() + timeout
        last = None
        while True:
            job = self.get(job_id)
            if job["state"] != last:
                last = job["state"]
                yield job
            if last in TERMINAL:
                return
            if time.monotonic() > deadline:
                raise TimeoutError(f"job {job_id} still {last} after {timeout:.0f}s")
            time.sleep(poll)
