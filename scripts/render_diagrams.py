"""Render docs/diagrams/src/*.json into editable .excalidraw files + sketch-style .svg files.

Sources are compact Excalidraw element skeletons (rectangle / text / arrow, with optional
`label`). Pure Python, no browser:

- `<name>.excalidraw`: full Excalidraw scene (open at excalidraw.com or with the VS Code
  Excalidraw extension; edit there, then copy changes back into the source JSON).
- `<name>.svg`: hand-drawn-style render for the README, with the Excalidraw "Virgil"
  font inlined so it renders on GitHub (which blocks external fonts inside <img> SVGs).

Usage: make diagrams
"""

from __future__ import annotations

import base64
import json
import random
import sys
import urllib.request
from html import escape
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "docs" / "diagrams" / "src"
OUT = ROOT / "docs" / "diagrams"
FONT_CACHE = ROOT / ".cache" / "Virgil.woff2"
FONT_URL = "https://unpkg.com/@excalidraw/excalidraw@0.17.6/dist/excalidraw-assets/Virgil.woff2"
PAD = 24
LINE_HEIGHT = 1.25
CHAR_W = 0.55  # rough average glyph width for Virgil, as a fraction of font size


# --------------------------------------------------------------------------- .excalidraw


def _base(el: dict, rng: random.Random, idx: int) -> dict:
    return {
        "id": el.get("id", f"el{idx}"),
        "type": el["type"],
        "x": el["x"],
        "y": el["y"],
        "width": el.get("width", 0),
        "height": el.get("height", 0),
        "angle": 0,
        "strokeColor": el.get("strokeColor", "#1e1e1e"),
        "backgroundColor": el.get("backgroundColor", "transparent"),
        "fillStyle": "solid",
        "strokeWidth": el.get("strokeWidth", 2),
        "strokeStyle": el.get("strokeStyle", "solid"),
        "roughness": 1,
        "opacity": el.get("opacity", 100),
        "groupIds": [],
        "frameId": None,
        "roundness": el.get("roundness"),
        "seed": rng.randint(1, 2**31),
        "version": 1,
        "versionNonce": rng.randint(1, 2**31),
        "isDeleted": False,
        "boundElements": [],
        "updated": 1,
        "link": None,
        "locked": False,
    }


def _text(id_: str, text: str, size: int, x: float, y: float, color: str, container=None):
    lines = text.split("\n")
    w = max(len(line) for line in lines) * size * CHAR_W
    h = len(lines) * size * LINE_HEIGHT
    return {
        "id": id_,
        "type": "text",
        "x": x,
        "y": y,
        "width": w,
        "height": h,
        "angle": 0,
        "strokeColor": color,
        "backgroundColor": "transparent",
        "fillStyle": "solid",
        "strokeWidth": 2,
        "strokeStyle": "solid",
        "roughness": 1,
        "opacity": 100,
        "groupIds": [],
        "frameId": None,
        "roundness": None,
        "seed": 1,
        "version": 1,
        "versionNonce": 1,
        "isDeleted": False,
        "boundElements": None,
        "updated": 1,
        "link": None,
        "locked": False,
        "text": text,
        "originalText": text,
        "fontSize": size,
        "fontFamily": 1,
        "textAlign": "center" if container else "left",
        "verticalAlign": "middle" if container else "top",
        "containerId": container,
        "lineHeight": LINE_HEIGHT,
        "autoResize": True,
    }


def to_excalidraw(skeleton: list[dict]) -> dict:
    rng = random.Random(42)
    elements: list[dict] = []
    for i, el in enumerate(skeleton):
        if el["type"] == "text":
            elements.append(
                _text(
                    f"el{i}",
                    el["text"],
                    el.get("fontSize", 20),
                    el["x"],
                    el["y"],
                    el.get("strokeColor", "#1e1e1e"),
                )
            )
            continue
        base = _base(el, rng, i)
        if el["type"] == "arrow":
            base.update(
                points=[[0, 0], [el["width"], el["height"]]],
                lastCommittedPoint=None,
                startBinding=None,
                endBinding=None,
                startArrowhead=None,
                endArrowhead="arrow",
                roundness={"type": 2},
            )
        elements.append(base)
        if label := el.get("label"):
            tid = f"{base['id']}-label"
            base["boundElements"].append({"type": "text", "id": tid})
            cx, cy = el["x"] + el["width"] / 2, el["y"] + el["height"] / 2
            t = _text(tid, label["text"], label.get("fontSize", 20), 0, 0, "#1e1e1e", base["id"])
            t["x"], t["y"] = cx - t["width"] / 2, cy - t["height"] / 2
            elements.append(t)
    return {
        "type": "excalidraw",
        "version": 2,
        "source": "https://excalidraw.com",
        "elements": elements,
        "appState": {"viewBackgroundColor": "#ffffff", "gridSize": None},
        "files": {},
    }


# --------------------------------------------------------------------------------- SVG


def _jitter(rng: random.Random, amount: float = 1.2) -> float:
    return rng.uniform(-amount, amount)


def _rough_line(rng, x1, y1, x2, y2) -> str:
    """Two slightly-off quadratic strokes, like Excalidraw's sketchy lines."""
    paths = []
    for _ in range(2):
        mx = (x1 + x2) / 2 + _jitter(rng, 1.5)
        my = (y1 + y2) / 2 + _jitter(rng, 1.5)
        paths.append(
            f"M{x1 + _jitter(rng):.1f} {y1 + _jitter(rng):.1f} "
            f"Q{mx:.1f} {my:.1f} {x2 + _jitter(rng):.1f} {y2 + _jitter(rng):.1f}"
        )
    return " ".join(paths)


def _text_svg(text: str, size: int, x: float, y: float, color: str, anchor: str) -> str:
    lines = text.split("\n")
    tspans = "".join(
        f'<tspan x="{x:.1f}" dy="{size * LINE_HEIGHT if i else 0:.1f}">{escape(line)}</tspan>'
        for i, line in enumerate(lines)
    )
    return (
        f'<text x="{x:.1f}" y="{y:.1f}" font-size="{size}" fill="{color}" '
        f'text-anchor="{anchor}" dominant-baseline="hanging">{tspans}</text>'
    )


def _label_block(label: dict, cx: float, cy: float, bg: str | None) -> str:
    size = label.get("fontSize", 20)
    lines = label["text"].split("\n")
    h = len(lines) * size * LINE_HEIGHT
    top = cy - h / 2
    out = ""
    if bg:
        w = max(len(line) for line in lines) * size * CHAR_W + 10
        out += f'<rect x="{cx - w / 2:.1f}" y="{top - 2:.1f}" width="{w:.1f}" '
        out += f'height="{h + 4:.1f}" fill="{bg}" rx="4"/>'
    return out + _text_svg(label["text"], size, cx, top, "#1e1e1e", "middle")


def to_svg(skeleton: list[dict], font_b64: str | None) -> str:
    rng = random.Random(7)
    xs, ys, body = [], [], []
    for el in skeleton:
        x, y = el["x"], el["y"]
        w, h = el.get("width", 0), el.get("height", 0)
        stroke = el.get("strokeColor", "#1e1e1e")
        sw = el.get("strokeWidth", 2)
        dash = ' stroke-dasharray="8 6"' if el.get("strokeStyle") == "dashed" else ""
        op = el.get("opacity", 100) / 100

        if el["type"] == "text":
            size = el.get("fontSize", 20)
            lines = el["text"].split("\n")
            xs += [x, x + max(map(len, lines)) * size * CHAR_W]
            ys += [y, y + len(lines) * size * LINE_HEIGHT]
            body.append(_text_svg(el["text"], size, x, y, stroke, "start"))
            continue

        xs += [x, x + w]
        ys += [y, y + h]
        if el["type"] == "rectangle":
            rx = 14 if el.get("roundness") else 0
            fill = el.get("backgroundColor", "transparent")
            fill = "none" if fill == "transparent" else fill
            body.append(
                f'<g opacity="{op}"><rect x="{x}" y="{y}" width="{w}" height="{h}" rx="{rx}" '
                f'fill="{fill}" stroke="{stroke}" stroke-width="{sw}"{dash}/>'
                f'<rect x="{x + _jitter(rng):.1f}" y="{y + _jitter(rng):.1f}" '
                f'width="{w + _jitter(rng):.1f}" height="{h + _jitter(rng):.1f}" rx="{rx}" '
                f'fill="none" stroke="{stroke}" stroke-width="{sw * 0.5}" '
                f'stroke-opacity="0.6"{dash}/></g>'
            )
            if label := el.get("label"):
                body.append(_label_block(label, x + w / 2, y + h / 2, None))
        elif el["type"] == "arrow":
            x2, y2 = x + w, y + h
            body.append(
                f'<path d="{_rough_line(rng, x, y, x2, y2)}" fill="none" stroke="{stroke}" '
                f'stroke-width="{sw}" stroke-linecap="round"{dash}/>'
            )
            length = (w * w + h * h) ** 0.5 or 1
            ux, uy = w / length, h / length
            head = 14
            p1 = (x2 - head * ux + head * 0.5 * uy, y2 - head * uy - head * 0.5 * ux)
            p2 = (x2 - head * ux - head * 0.5 * uy, y2 - head * uy + head * 0.5 * ux)
            body.append(
                f'<path d="M{p1[0]:.1f} {p1[1]:.1f} L{x2:.1f} {y2:.1f} L{p2[0]:.1f} {p2[1]:.1f}" '
                f'fill="none" stroke="{stroke}" stroke-width="{sw}" stroke-linecap="round" '
                f'stroke-linejoin="round"/>'
            )
            if label := el.get("label"):
                body.append(_label_block(label, x + w / 2, y + h / 2, "#ffffff"))
        else:
            sys.exit(f"unsupported element type: {el['type']}")

    minx, miny = min(xs) - PAD, min(ys) - PAD
    width, height = max(xs) - minx + PAD, max(ys) - miny + PAD
    font_face = (
        f'@font-face{{font-family:"Virgil";src:url("data:font/woff2;base64,{font_b64}");}}'
        if font_b64
        else ""
    )
    return (
        f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="{minx:.0f} {miny:.0f} '
        f'{width:.0f} {height:.0f}" width="{width:.0f}" height="{height:.0f}">'
        f"<style>{font_face}text{{font-family:Virgil,'Comic Sans MS','Segoe Print',"
        f"cursive;}}</style>"
        f'<rect x="{minx:.0f}" y="{miny:.0f}" width="{width:.0f}" height="{height:.0f}" '
        f'fill="#ffffff"/>' + "".join(body) + "</svg>\n"
    )


# -------------------------------------------------------------------------------- main


def load_font() -> str | None:
    try:
        if not FONT_CACHE.exists():
            FONT_CACHE.parent.mkdir(parents=True, exist_ok=True)
            FONT_CACHE.write_bytes(urllib.request.urlopen(FONT_URL, timeout=30).read())
        return base64.b64encode(FONT_CACHE.read_bytes()).decode()
    except OSError as exc:  # offline: fall back to system handwriting fonts
        print(f"warning: could not fetch Virgil font ({exc}); using fallback fonts")
        return None


def main() -> None:
    sources = sorted(SRC.glob("*.json"))
    if not sources:
        sys.exit(f"no diagram sources in {SRC}")
    font_b64 = load_font()
    for src in sources:
        skeleton = json.loads(src.read_text())
        scene = to_excalidraw(skeleton)
        (OUT / f"{src.stem}.excalidraw").write_text(json.dumps(scene, indent=2) + "\n")
        (OUT / f"{src.stem}.svg").write_text(to_svg(skeleton, font_b64))
        print(f"rendered {src.stem}: {len(scene['elements'])} elements")


if __name__ == "__main__":
    main()
