"""API schemas: the job spec users submit and the job record they get back."""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator

MAX_CODE_BYTES = 512 * 1024  # ConfigMaps cap at 1 MiB; keep well under it
_FILENAME = re.compile(r"[A-Za-z0-9_.-]+")
_MEMORY = re.compile(r"(\d+)(Mi|Gi)")
_SHELL_META = re.compile(r"[;&|<>`$\n\\]")


class JobState(StrEnum):
    QUEUED = "QUEUED"  # waiting in the API for team quota; no Kubernetes objects yet
    SUBMITTED = "SUBMITTED"
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    RETRYING = "RETRYING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"


TERMINAL = frozenset({JobState.SUCCEEDED, JobState.FAILED, JobState.CANCELLED})


@dataclass(frozen=True)
class Resources:
    """Pod resource *requests*: what a ResourceQuota's requests.cpu / requests.memory count."""

    cpu_m: int = 0
    mem_mib: int = 0

    def __add__(self, other: Resources) -> Resources:
        return Resources(self.cpu_m + other.cpu_m, self.mem_mib + other.mem_mib)

    def fits_in(self, other: Resources) -> bool:
        return self.cpu_m <= other.cpu_m and self.mem_mib <= other.mem_mib

    def __str__(self) -> str:
        return f"{self.cpu_m / 1000:g} CPU / {self.mem_mib}Mi"


def memory_mib(quantity: str) -> int:
    """'1536Mi' -> 1536, '2Gi' -> 2048."""
    m = _MEMORY.fullmatch(quantity)
    if not m:
        raise ValueError(f"memory must look like 512Mi or 2Gi, got {quantity!r}")
    return int(m[1]) * (1024 if m[2] == "Gi" else 1)


class JobSpec(BaseModel):
    """What a team asks for. The namespace is never part of it: it comes from the API key."""

    model_config = ConfigDict(extra="forbid")

    entrypoint: str = Field(min_length=1, description="Shell command, run from the code directory")
    files: dict[str, str] = Field(default_factory=dict, description="filename -> file content")
    workers: int = Field(default=2, ge=1, le=4)
    cpu: int = Field(default=1, ge=1, le=2, description="Ray CPUs (and CPU limit) per worker")
    # 1536Mi: a Ray Train worker with torch + transformers peaked at ~1.3GB (Phase 3)
    memory: str = Field(default="1536Mi", description="Memory limit per worker, e.g. 2Gi")
    resources: dict[str, float] = Field(
        default_factory=dict, description="Ray custom resources per worker, e.g. {'sim_gpu': 1}"
    )
    retries: int = Field(default=0, ge=0, le=5, description="RayJob backoffLimit")

    @field_validator("files")
    @classmethod
    def _check_files(cls, files: dict[str, str]) -> dict[str, str]:
        for name in files:
            if not _FILENAME.fullmatch(name) or name.startswith("."):
                raise ValueError(f"bad file name {name!r}: use flat names like train.py")
        if sum(len(c.encode()) for c in files.values()) > MAX_CODE_BYTES:
            raise ValueError(f"code is larger than {MAX_CODE_BYTES // 1024} KiB")
        return files

    @field_validator("entrypoint")
    @classmethod
    def _check_entrypoint(cls, entrypoint: str) -> str:
        # The KubeRay submitter passes the entrypoint through a shell; operators would run
        # their right-hand side in the submitter pod instead of on the Ray cluster.
        if _SHELL_META.search(entrypoint):
            raise ValueError("entrypoint must be a plain command: no ; & | < > ` $ or newlines")
        return entrypoint

    @field_validator("memory")
    @classmethod
    def _check_memory(cls, memory: str) -> str:
        if not 256 <= memory_mib(memory) <= 4096:
            raise ValueError("memory must be between 256Mi and 4Gi")
        return memory

    @field_validator("resources")
    @classmethod
    def _check_resources(cls, resources: dict[str, float]) -> dict[str, float]:
        for name, amount in resources.items():
            if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", name) or name in {"CPU", "GPU"}:
                raise ValueError(f"bad custom resource name {name!r}")
            if amount <= 0:
                raise ValueError(f"resource {name!r} must be > 0")
        return resources


class Job(BaseModel):
    """A row of the job registry, as returned by the API."""

    id: str
    team: str
    k8s_name: str
    state: JobState
    message: str | None = None
    spec: dict[str, Any] = Field(description="The submitted JobSpec, without file contents")
    submitted_at: datetime
    started_at: datetime | None = None
    finished_at: datetime | None = None
