from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from platform_api.config import Settings
from platform_api.main import create_app
from platform_api.registry import Registry
from tests.fakes import FakeKube

KEY_A = "key-team-a-0123456789"
KEY_B = "key-team-b-0123456789"


@dataclass
class Platform:
    kube: FakeKube
    registry: Registry
    http: TestClient

    def as_team(self, key: str) -> dict[str, str]:
        return {"X-API-Key": key}


@pytest.fixture
def platform(tmp_path: Path) -> Iterator[Platform]:
    kube = FakeKube()
    settings = Settings(
        api_keys={KEY_A: "team-a", KEY_B: "team-b"},
        db_path=tmp_path / "registry.db",
        image="rkp-ray:test",
        ray_version="2.59.0",
    )
    app = create_app(settings, kube, run_reconciler=False)
    with TestClient(app) as http:
        yield Platform(kube, app.state.registry, http)
