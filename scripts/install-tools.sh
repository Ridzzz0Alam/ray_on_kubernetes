#!/usr/bin/env bash
# Install pinned kind / kubectl / helm into ~/.local/bin with checksum verification.
# Idempotent: skips a tool if the pinned version is already installed.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck source=/dev/null
source "$ROOT/infra/versions.env"

BIN_DIR="${BIN_DIR:-$HOME/.local/bin}"
OS="$(uname -s | tr '[:upper:]' '[:lower:]')"
case "$(uname -m)" in
  x86_64) ARCH=amd64 ;;
  aarch64 | arm64) ARCH=arm64 ;;
  *) echo "unsupported arch: $(uname -m)" >&2; exit 1 ;;
esac

mkdir -p "$BIN_DIR"
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

verify() { # <file> <expected-sha256>
  echo "$2  $1" | sha256sum --check --quiet -
}

install_kind() {
  if command -v kind >/dev/null && kind version | grep -q "$KIND_VERSION "; then
    echo "kind $KIND_VERSION already installed"; return
  fi
  local url="https://kind.sigs.k8s.io/dl/${KIND_VERSION}/kind-${OS}-${ARCH}"
  curl -fsSLo "$TMP/kind" "$url"
  verify "$TMP/kind" "$(curl -fsSL "${url}.sha256sum" | awk '{print $1}')"
  install -m 0755 "$TMP/kind" "$BIN_DIR/kind"
  echo "installed kind $KIND_VERSION"
}

install_kubectl() {
  if command -v kubectl >/dev/null && kubectl version --client 2>/dev/null | grep -q "$KUBECTL_VERSION"; then
    echo "kubectl $KUBECTL_VERSION already installed"; return
  fi
  local url="https://dl.k8s.io/release/${KUBECTL_VERSION}/bin/${OS}/${ARCH}/kubectl"
  curl -fsSLo "$TMP/kubectl" "$url"
  verify "$TMP/kubectl" "$(curl -fsSL "${url}.sha256")"
  install -m 0755 "$TMP/kubectl" "$BIN_DIR/kubectl"
  echo "installed kubectl $KUBECTL_VERSION"
}

install_helm() {
  if command -v helm >/dev/null && helm version --short 2>/dev/null | grep -q "^$HELM_VERSION"; then
    echo "helm $HELM_VERSION already installed"; return
  fi
  local tgz="helm-${HELM_VERSION}-${OS}-${ARCH}.tar.gz"
  curl -fsSLo "$TMP/$tgz" "https://get.helm.sh/${tgz}"
  verify "$TMP/$tgz" "$(curl -fsSL "https://get.helm.sh/${tgz}.sha256sum" | awk '{print $1}')"
  tar -xzf "$TMP/$tgz" -C "$TMP"
  install -m 0755 "$TMP/${OS}-${ARCH}/helm" "$BIN_DIR/helm"
  echo "installed helm $HELM_VERSION"
}

install_kind
install_kubectl
install_helm

case ":$PATH:" in
  *":$BIN_DIR:"*) ;;
  *) echo "NOTE: add $BIN_DIR to your PATH" ;;
esac
