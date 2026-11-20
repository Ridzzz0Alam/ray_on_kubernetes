"""Guard rails for infra/versions.env: every version must be pinned exactly."""

import re
from pathlib import Path

VERSIONS_FILE = Path(__file__).resolve().parents[1] / "infra" / "versions.env"
REQUIRED = {
    "KIND_VERSION",
    "KUBECTL_VERSION",
    "HELM_VERSION",
    "KIND_NODE_IMAGE",
    "KUBERAY_CHART_VERSION",
    "RAY_VERSION",
    "RAY_IMAGE",
    "PYTHON_VERSION",
    "RKP_IMAGE",
}


def load_versions() -> dict[str, str]:
    pairs = {}
    for line in VERSIONS_FILE.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        key, _, value = line.partition("=")
        pairs[key.strip()] = value.strip()
    return pairs


def test_all_required_keys_present() -> None:
    assert load_versions().keys() >= REQUIRED


def test_no_floating_versions() -> None:
    for key, value in load_versions().items():
        assert value, f"{key} is empty"
        assert "latest" not in value.lower(), f"{key} uses a floating tag: {value}"


def test_semver_pins_are_exact() -> None:
    v = load_versions()
    for key in ("KIND_VERSION", "KUBECTL_VERSION", "HELM_VERSION"):
        assert re.fullmatch(r"v\d+\.\d+\.\d+", v[key]), f"{key}={v[key]}"
    for key in ("KUBERAY_CHART_VERSION", "RAY_VERSION"):
        assert re.fullmatch(r"\d+\.\d+\.\d+", v[key]), f"{key}={v[key]}"


def test_node_image_is_digest_pinned() -> None:
    assert re.search(r"@sha256:[0-9a-f]{64}$", load_versions()["KIND_NODE_IMAGE"])


def test_ray_image_matches_ray_and_python_versions() -> None:
    v = load_versions()
    py_tag = "py" + v["PYTHON_VERSION"].replace(".", "")
    assert v["RAY_IMAGE"] == f"rayproject/ray:{v['RAY_VERSION']}-{py_tag}-cpu"


ROOT = VERSIONS_FILE.parents[1]


def test_rkp_image_is_used_by_manifests_and_dockerfile() -> None:
    v = load_versions()
    assert re.fullmatch(rf"rkp-ray:{re.escape(v['RAY_VERSION'])}-\d+", v["RKP_IMAGE"])
    dockerfile = (ROOT / "docker" / "Dockerfile").read_text()
    assert f"ARG RAY_IMAGE={v['RAY_IMAGE']}" in dockerfile
    manifest = (ROOT / "infra" / "k8s" / "examples" / "hello-rayjob.yaml").read_text()
    images = set(re.findall(r"image:\s*(\S+)", manifest))
    assert images == {v["RKP_IMAGE"]}, images
    assert f'rayVersion: "{v["RAY_VERSION"]}"' in manifest


def test_python_deps_in_image_are_pinned() -> None:
    for line in (ROOT / "docker" / "requirements.txt").read_text().splitlines():
        line = line.split("#")[0].strip()
        if line:
            assert re.fullmatch(r"[A-Za-z0-9_.-]+==[\w.]+", line), line
