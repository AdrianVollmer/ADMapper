#!/usr/bin/env python3
"""
Graph layout test harness for ADMapper.

Generates synthetic test graphs, renders them through every layout algorithm,
produces an interactive HTML file per combination (using the existing export
template), and writes an index.html grid for side-by-side visual comparison.

Usage (from the repo root):
    python3 tools/graph_layout_test.py [output_dir]

Default output_dir: e2e/reports/<timestamp>/layout-test/
  (matching the convention used by e2e/run_tests.py)

The script builds the admapper binary if it is not already present, then
starts the web server in headless mode to call the real layout API, so the
output faithfully reflects what users see in the app.
"""

import json
import math
import os
import random
import shutil
import socket
import subprocess
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

# ── Paths ──────────────────────────────────────────────────────────────────────

SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parent
TEMPLATE_PATH = REPO_ROOT / "src" / "frontend" / "export-graph-template.html"
MANIFEST_PATH = REPO_ROOT / "src" / "backend" / "Cargo.toml"
BINARY_PATH = REPO_ROOT / "src" / "backend" / "target" / "debug" / "admapper"
REPORTS_BASE = REPO_ROOT / "e2e" / "reports"

_TIMESTAMP = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
OUTPUT_DIR = (
    Path(sys.argv[1]).resolve()
    if len(sys.argv) > 1
    else REPORTS_BASE / _TIMESTAMP / "layout-test"
)

# ── Node type → colour (mirrors export-graph-template.html) ───────────────────

NODE_COLORS = {
    "User": "#22b8cf",
    "Group": "#fab005",
    "Computer": "#f03e3e",
    "Domain": "#7950f2",
    "GPO": "#fd7e14",
    "OU": "#20c997",
    "Container": "#db2777",
    "Unknown": "#adb5bd",
}


def node_color(node_type: str) -> str:
    return NODE_COLORS.get(node_type, NODE_COLORS["Unknown"])


# ── Layout algorithm descriptors ───────────────────────────────────────────────

LAYOUTS = [
    {
        "id": "force",
        "label": "Force Directed",
        "request": {"algorithm": "force_directed"},
    },
    {
        "id": "hier_lr",
        "label": "Hierarchical L→R",
        "request": {"algorithm": "hierarchical", "direction": "left_to_right"},
    },
    {
        "id": "hier_tb",
        "label": "Hierarchical T→B",
        "request": {"algorithm": "hierarchical", "direction": "top_to_bottom"},
    },
    {
        "id": "circular",
        "label": "Circular",
        "request": {"algorithm": "circular"},
    },
    {
        "id": "grid",
        "label": "Grid",
        "request": {"algorithm": "grid"},
    },
    {
        "id": "lattice",
        "label": "Lattice",
        "request": {"algorithm": "lattice"},
    },
]

# ── Graph builders ─────────────────────────────────────────────────────────────


def N(node_id, label, node_type, **props):
    return {"id": node_id, "label": label, "type": node_type, "properties": dict(props)}


def E(source, target, rel_type="Edge"):
    return {"source": source, "target": target, "type": rel_type}


def chain(n, node_type="User"):
    nodes = [N(f"n{i}", f"N{i}", node_type) for i in range(n)]
    edges = [E(f"n{i}", f"n{i+1}") for i in range(n - 1)]
    return nodes, edges


def star(n_leaves):
    nodes = [N("hub", "Hub", "Group")]
    edges = []
    for i in range(n_leaves):
        nodes.append(N(f"leaf{i}", f"L{i}", "User"))
        edges.append(E("hub", f"leaf{i}", "Contains"))
    return nodes, edges


def binary_tree(depth):
    types_by_depth = {0: "Domain", 1: "Group", 2: "Computer"}
    nodes = [N("root", "Root", "Domain")]
    edges = []
    queue = [("root", 0)]
    idx = 0
    while queue:
        parent, d = queue.pop(0)
        if d >= depth:
            continue
        for _ in range(2):
            nid = f"n{idx}"
            idx += 1
            ntype = types_by_depth.get(d + 1, "User")
            nodes.append(N(nid, nid, ntype))
            edges.append(E(parent, nid, "Contains"))
            queue.append((nid, d + 1))
    return nodes, edges


def clusters(sizes, bridges=None):
    """Multiple clusters with optional bridge edges between them."""
    TYPES = ["User", "Computer", "Group", "OU"]
    nodes, edges = [], []
    cluster_hubs = []
    for ci, size in enumerate(sizes):
        ids = [f"c{ci}n{ni}" for ni in range(size)]
        cluster_hubs.append(ids[0])
        ntype_hub = TYPES[ci % len(TYPES)]
        nodes.append(N(ids[0], f"C{ci}Hub", ntype_hub))
        for ni in range(1, size):
            ntype = TYPES[(ci + ni) % len(TYPES)]
            nodes.append(N(ids[ni], f"C{ci}N{ni}", ntype))
            # fan out from hub with some back-connections
            target = ids[ni // 2] if ni > 1 else ids[0]
            edges.append(E(ids[ni], target, "MemberOf"))
    if bridges:
        for a, b in bridges:
            edges.append(E(cluster_hubs[a], cluster_hubs[b], "TrustedBy"))
    return nodes, edges


def ad_structure_small():
    nodes = [
        N("domain", "CORP.LOCAL", "Domain"),
        N("da_grp", "DOMAIN ADMINS", "Group"),
        N("du_grp", "DOMAIN USERS", "Group"),
        N("dc_grp", "DOMAIN COMPUTERS", "Group"),
        N("dc01", "DC01.CORP.LOCAL", "Computer"),
        N("srv01", "SRV01.CORP.LOCAL", "Computer"),
        N("srv02", "SRV02.CORP.LOCAL", "Computer"),
        N("wks01", "WKS001.CORP.LOCAL", "Computer"),
        N("wks02", "WKS002.CORP.LOCAL", "Computer"),
        N("wks03", "WKS003.CORP.LOCAL", "Computer"),
        N("adm1", "JOHN.ADMIN", "User"),
        N("adm2", "JANE.ADMIN", "User"),
        N("usr1", "ALICE.USER", "User"),
        N("usr2", "BOB.USER", "User"),
        N("usr3", "CHARLIE.USER", "User"),
        N("ou_u", "USERS OU", "OU"),
        N("ou_c", "COMPUTERS OU", "OU"),
        N("gpo1", "DEFAULT DOMAIN POLICY", "GPO"),
    ]
    edges = [
        E("adm1", "da_grp", "MemberOf"),
        E("adm2", "da_grp", "MemberOf"),
        E("usr1", "du_grp", "MemberOf"),
        E("usr2", "du_grp", "MemberOf"),
        E("usr3", "du_grp", "MemberOf"),
        E("dc01", "dc_grp", "MemberOf"),
        E("srv01", "dc_grp", "MemberOf"),
        E("srv02", "dc_grp", "MemberOf"),
        E("wks01", "dc_grp", "MemberOf"),
        E("wks02", "dc_grp", "MemberOf"),
        E("wks03", "dc_grp", "MemberOf"),
        E("domain", "ou_u", "Contains"),
        E("domain", "ou_c", "Contains"),
        E("gpo1", "domain", "GPLink"),
        E("da_grp", "domain", "GenericAll"),
        E("wks01", "usr1", "HasSession"),
        E("wks02", "usr2", "HasSession"),
        E("usr3", "wks03", "AdminTo"),
    ]
    return nodes, edges


def grid_topology(rows, cols):
    nodes, edges = [], []
    for r in range(rows):
        for c in range(cols):
            nid = f"r{r}c{c}"
            nodes.append(N(nid, f"R{r}C{c}", "Computer"))
            if c > 0:
                edges.append(E(nid, f"r{r}c{c-1}"))
            if r > 0:
                edges.append(E(nid, f"r{r-1}c{c}"))
    return nodes, edges


def multi_hub_spokes(n_hubs, n_spokes):
    TYPES = ["User", "Computer", "OU"]
    nodes, edges = [], []
    hub_ids = [f"hub{hi}" for hi in range(n_hubs)]
    for hi, hub_id in enumerate(hub_ids):
        nodes.append(N(hub_id, f"HUB{hi}", "Group"))
        for si in range(n_spokes):
            sid = f"h{hi}s{si}"
            nodes.append(N(sid, f"H{hi}S{si}", TYPES[si % len(TYPES)]))
            edges.append(E(hub_id, sid, "Contains"))
    for hi in range(n_hubs):
        edges.append(E(hub_ids[hi], hub_ids[(hi + 1) % n_hubs], "TrustedBy"))
    return nodes, edges


def random_graph(n_nodes, n_edges, seed=42):
    rng = random.Random(seed)
    TYPES = ["User", "Computer", "Group", "OU", "GPO"]
    nodes = [N(f"n{i}", f"N{i}", TYPES[i % len(TYPES)]) for i in range(n_nodes)]
    edge_set = set()
    edges = []
    attempts = 0
    while len(edges) < n_edges and attempts < n_edges * 20:
        attempts += 1
        a = rng.randint(0, n_nodes - 1)
        b = rng.randint(0, n_nodes - 1)
        if a != b and (a, b) not in edge_set:
            edge_set.add((a, b))
            edges.append(E(f"n{a}", f"n{b}"))
    return nodes, edges


def dense_core_periphery(n_core, n_peri):
    nodes = [N(f"core{i}", f"Core{i}", "Group") for i in range(n_core)]
    nodes += [N(f"peri{i}", f"Peri{i}", "User") for i in range(n_peri)]
    edges = []
    for i in range(n_core):
        for j in range(i + 1, n_core):
            edges.append(E(f"core{i}", f"core{j}", "GenericAll"))
    for i in range(n_peri):
        edges.append(E(f"peri{i}", f"core{i % n_core}", "MemberOf"))
    return nodes, edges


# ── Test graph catalogue ───────────────────────────────────────────────────────


def make_test_graphs():
    specs = [
        ("tiny_chain", "Tiny Chain", "5 nodes, linear A→B→C→D→E",
            *chain(5)),
        ("star_small", "Small Star", "1 hub + 8 leaves",
            *star(8)),
        ("binary_tree", "Binary Tree", "Full binary tree, depth 4 (15 nodes)",
            *binary_tree(4)),
        ("ad_small", "AD Structure (small)", "Domain · groups · users · computers",
            *ad_structure_small()),
        ("clusters_isolated", "Three Isolated Clusters", "3 clusters of 8 nodes, no bridges",
            *clusters([8, 8, 8])),
        ("clusters_bridged", "Three Bridged Clusters", "3 clusters of 8 nodes, weakly connected",
            *clusters([8, 8, 8], bridges=[(0, 1), (1, 2)])),
        ("clusters_mixed", "Mixed Cluster Sizes", "Clusters of 4 · 12 · 25 nodes, connected",
            *clusters([4, 12, 25], bridges=[(0, 1), (1, 2)])),
        ("grid_5x5", "Grid 5×5", "25 nodes, square-grid connectivity",
            *grid_topology(5, 5)),
        ("multi_hub", "Multi-Hub Spokes", "3 hubs × 8 spokes, hubs in a ring",
            *multi_hub_spokes(3, 8)),
        ("deep_chain", "Deep Chain", "30 nodes, pure linear chain",
            *chain(30, node_type="Computer")),
        ("large_random", "Large Random", "50 nodes, 80 random edges",
            *random_graph(50, 80)),
        ("dense_core", "Dense Core + Periphery", "10 fully-connected core nodes + 20 leaf nodes",
            *dense_core_periphery(10, 20)),
    ]

    graphs = []
    for item in specs:
        gid, title, desc, nodes, edges = item
        graphs.append({"id": gid, "title": title, "desc": desc, "nodes": nodes, "edges": edges})
    return graphs


# ── Git metadata ───────────────────────────────────────────────────────────────


def get_commit() -> str:
    env_commit = os.environ.get("GIT_COMMIT", "")
    if env_commit and env_commit != "unknown":
        return env_commit
    try:
        r = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            capture_output=True, text=True, cwd=str(REPO_ROOT),
        )
        if r.returncode == 0:
            return r.stdout.strip()
    except Exception:
        pass
    return "unknown"


# ── Binary / server management ─────────────────────────────────────────────────


def build_binary():
    # rust-embed requires the frontend build directory to exist
    build_dir = REPO_ROOT / "build"
    if not build_dir.exists():
        build_dir.mkdir()
        (build_dir / "index.html").write_text("<html><body>placeholder</body></html>")

    print("Building admapper binary (headless, no desktop)…", flush=True)
    result = subprocess.run(
        [
            "cargo", "build",
            "--manifest-path", str(MANIFEST_PATH),
            "--no-default-features",
            "--features", "crustdb",
        ],
        cwd=str(REPO_ROOT),
    )
    if result.returncode != 0:
        sys.exit("ERROR: cargo build failed")


def ensure_binary():
    if not BINARY_PATH.exists():
        build_binary()
    if not BINARY_PATH.exists():
        sys.exit(f"ERROR: binary not found at {BINARY_PATH}")
    return BINARY_PATH


def free_port():
    with socket.socket() as s:
        s.bind(("", 0))
        return s.getsockname()[1]


def wait_for_server(port: int, timeout: float = 30.0) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            urllib.request.urlopen(f"http://127.0.0.1:{port}/api/health", timeout=1)
            return True
        except urllib.error.HTTPError:
            return True  # server up, just returned a non-200
        except Exception:
            time.sleep(0.3)
    return False


# ── Layout API ─────────────────────────────────────────────────────────────────


def call_layout(port: int, graph: dict, layout: dict) -> dict:
    """POST to the layout endpoint; returns {node_id: (x, y)}."""
    node_ids = [n["id"] for n in graph["nodes"]]
    idx = {nid: i for i, nid in enumerate(node_ids)}

    edges_idx = []
    for e in graph["edges"]:
        si = idx.get(e["source"])
        ti = idx.get(e["target"])
        if si is not None and ti is not None:
            edges_idx.append([si, ti])

    payload = json.dumps({"nodes": node_ids, "edges": edges_idx, **layout["request"]}).encode()
    req = urllib.request.Request(
        f"http://127.0.0.1:{port}/api/graph/layout",
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=60) as resp:
        result = json.loads(resp.read())

    return {p["id"]: (p["x"], p["y"]) for p in result["positions"]}


# ── HTML generation ────────────────────────────────────────────────────────────


def build_export_payload(graph: dict, positions: dict) -> dict:
    nodes_out = []
    for n in graph["nodes"]:
        x, y = positions.get(n["id"], (0.0, 0.0))
        nodes_out.append({
            "id": n["id"],
            "label": n["label"],
            "type": n["type"],
            "properties": n.get("properties", {}),
            "x": x,
            "y": y,
        })
    edges_out = [
        {
            "id": f"{e['source']}->{e['target']}:{e['type']}",
            "source": e["source"],
            "target": e["target"],
            "type": e["type"],
        }
        for e in graph["edges"]
    ]
    return {
        "nodes": nodes_out,
        "edges": edges_out,
        "exportedAt": datetime.now(timezone.utc).isoformat(),
    }


def render_interactive_html(template: str, data: dict, title: str, commit: str) -> str:
    html = template.replace(
        "__GRAPH_DATA_PLACEHOLDER__",
        json.dumps(data, indent=2),
    )
    html = html.replace(
        "<title>ADMapper Graph Export</title>",
        f"<title>{title}</title>",
    )
    commit_short = commit[:7] if commit != "unknown" else "unknown"
    # Append commit info to the export-date span text (runs after the template JS sets it)
    html = html.replace(
        'document.getElementById("export-date").textContent = "Exported " + new Date(DATA.exportedAt).toLocaleString();',
        f'document.getElementById("export-date").textContent = "commit {commit_short} · " + new Date(DATA.exportedAt).toLocaleString();',
    )
    return html


# ── SVG thumbnail ──────────────────────────────────────────────────────────────

THUMB_W = 220
THUMB_H = 155
THUMB_PAD = 10


def render_thumbnail(graph: dict, positions: dict) -> str:
    if not positions:
        return (
            f'<svg xmlns="http://www.w3.org/2000/svg" width="{THUMB_W}" height="{THUMB_H}">'
            f'<text x="50%" y="50%" fill="#4b5563" text-anchor="middle" dominant-baseline="middle"'
            f' font-size="11" font-family="system-ui">No positions</text></svg>'
        )

    xs = [v[0] for v in positions.values()]
    ys = [v[1] for v in positions.values()]
    min_x, max_x = min(xs), max(xs)
    min_y, max_y = min(ys), max(ys)
    span_x = max_x - min_x or 1.0
    span_y = max_y - min_y or 1.0

    W = THUMB_W - 2 * THUMB_PAD
    H = THUMB_H - 2 * THUMB_PAD

    def tx(x):
        return THUMB_PAD + (x - min_x) / span_x * W

    def ty(y):
        # layout y is math-positive-up; SVG y increases downward
        return THUMB_PAD + (1.0 - (y - min_y) / span_y) * H

    parts = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{THUMB_W}" height="{THUMB_H}"'
        f' style="background:#0f172a;display:block">',
    ]

    # Edges first (under nodes)
    for e in graph["edges"]:
        sp = positions.get(e["source"])
        tp = positions.get(e["target"])
        if sp and tp:
            x1, y1 = tx(sp[0]), ty(sp[1])
            x2, y2 = tx(tp[0]), ty(tp[1])
            parts.append(
                f'<line x1="{x1:.1f}" y1="{y1:.1f}" x2="{x2:.1f}" y2="{y2:.1f}"'
                f' stroke="#2d3748" stroke-width="1"/>'
            )

    # Nodes
    n_count = len(graph["nodes"])
    r = max(2.0, min(5.0, 70.0 / math.sqrt(n_count)))
    for n in graph["nodes"]:
        pos = positions.get(n["id"])
        if pos:
            cx, cy = tx(pos[0]), ty(pos[1])
            color = node_color(n["type"])
            parts.append(
                f'<circle cx="{cx:.1f}" cy="{cy:.1f}" r="{r:.1f}" fill="{color}"/>'
            )

    parts.append("</svg>")
    return "\n".join(parts)


# ── Index HTML ─────────────────────────────────────────────────────────────────


def render_index(graphs: list, results: dict, commit: str, timestamp: str) -> str:
    header_cells = "".join(
        f'<th class="lh">{l["label"]}</th>' for l in LAYOUTS
    )

    rows = []
    for g in graphs:
        cells = []
        for l in LAYOUTS:
            key = (g["id"], l["id"])
            entry = results.get(key)
            if entry is None:
                cells.append('<td class="cell err">error</td>')
            else:
                positions, rel_path = entry
                svg = render_thumbnail(g, positions)
                cells.append(
                    f'<td class="cell">'
                    f'<a href="{rel_path}" target="_blank" title="Open interactive view">'
                    f'{svg}</a></td>'
                )

        n = len(g["nodes"])
        e = len(g["edges"])
        rows.append(
            f'<tr>'
            f'<td class="gh">'
            f'<div class="g-title">{g["title"]}</div>'
            f'<div class="g-desc">{g["desc"]}</div>'
            f'<div class="g-stats">{n} nodes · {e} edges</div>'
            f'</td>'
            + "".join(cells)
            + "</tr>"
        )

    rows_html = "\n".join(rows)
    lh_min = THUMB_W + 8
    commit_short = commit[:7] if commit != "unknown" else "unknown"

    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="UTF-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1.0"/>
  <title>ADMapper — Layout Test Grid — {commit_short}</title>
  <style>
    *, *::before, *::after {{ box-sizing: border-box; margin: 0; padding: 0; }}
    body {{
      background: #111827;
      color: #e5e7eb;
      font-family: system-ui, -apple-system, sans-serif;
      font-size: 13px;
    }}
    #topbar {{
      padding: 14px 20px 12px;
      border-bottom: 1px solid #1f2937;
      display: flex;
      align-items: baseline;
      gap: 12px;
    }}
    #topbar h1 {{ font-size: 16px; color: #818cf8; font-weight: 700; }}
    #topbar p  {{ font-size: 11px; color: #6b7280; }}
    .wrap {{ overflow-x: auto; padding: 16px 20px 40px; }}
    table {{ border-collapse: collapse; }}
    th, td {{ border: 1px solid #1f2937; vertical-align: top; }}
    thead th {{
      background: #1f2937;
      padding: 7px 10px;
      color: #9ca3af;
      font-size: 11px;
      font-weight: 600;
      text-align: center;
      white-space: nowrap;
      position: sticky;
      top: 0;
      z-index: 1;
    }}
    th.lh {{ min-width: {lh_min}px; }}
    .gh {{
      padding: 10px 12px;
      min-width: 175px;
      max-width: 195px;
      background: #161e2e;
      vertical-align: middle;
    }}
    .g-title {{ font-weight: 600; color: #c7d2fe; margin-bottom: 4px; font-size: 12px; }}
    .g-desc  {{ color: #6b7280; font-size: 11px; margin-bottom: 5px; line-height: 1.4; }}
    .g-stats {{ color: #374151; font-size: 10px; }}
    .cell {{ padding: 3px; background: #0f172a; text-align: center; }}
    .cell a {{
      display: inline-block;
      border-radius: 3px;
      overflow: hidden;
      transition: box-shadow 0.12s;
      line-height: 0;
    }}
    .cell a:hover {{ box-shadow: 0 0 0 2px #818cf8; }}
    .cell.err {{ color: #ef4444; font-size: 11px; padding: 8px; vertical-align: middle; text-align: center; }}
    tr:hover .gh {{ background: #1c2a42; }}
  </style>
</head>
<body>
  <div id="topbar">
    <h1>ADMapper — Layout Test Grid</h1>
    <p>Click a thumbnail to open the interactive graph.
       &nbsp;·&nbsp; <code>{commit_short}</code> &nbsp;·&nbsp; {timestamp}</p>
  </div>
  <div class="wrap">
    <table>
      <thead>
        <tr>
          <th>Graph</th>
          {header_cells}
        </tr>
      </thead>
      <tbody>
        {rows_html}
      </tbody>
    </table>
  </div>
</body>
</html>
"""


# ── Main ───────────────────────────────────────────────────────────────────────


def update_latest_symlink(output_dir: Path) -> None:
    """Update e2e/reports/latest to point at the current run's parent timestamp dir."""
    # output_dir is e2e/reports/<timestamp>/layout-test  (or a custom path)
    # We point latest at the <timestamp> dir, one level up, only when it's inside reports_base.
    try:
        parent = output_dir.parent
        if parent.parent == REPORTS_BASE:
            latest = REPORTS_BASE / "latest"
            if latest.is_symlink() or latest.exists():
                if latest.is_symlink():
                    latest.unlink()
                elif latest.is_dir():
                    shutil.rmtree(latest)
                else:
                    latest.unlink()
            latest.symlink_to(parent.name)
    except Exception:
        pass  # symlink creation is best-effort


def main():
    commit = get_commit()
    commit_short = commit[:7] if commit != "unknown" else "unknown"
    timestamp = _TIMESTAMP

    print(f"Output:  {OUTPUT_DIR}", flush=True)
    print(f"Commit:  {commit_short}", flush=True)

    binary = ensure_binary()
    print(f"Binary:  {binary}", flush=True)

    template = TEMPLATE_PATH.read_text(encoding="utf-8")

    graphs_dir = OUTPUT_DIR / "graphs"
    graphs_dir.mkdir(parents=True, exist_ok=True)

    port = free_port()
    print(f"Port:    {port}\n", flush=True)

    proc = subprocess.Popen(
        [str(binary), "--headless", "--port", str(port)],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )

    try:
        if not wait_for_server(port):
            proc.terminate()
            sys.exit("ERROR: server did not start in time")
        print("Server ready.\n", flush=True)

        graphs = make_test_graphs()
        results: dict = {}

        for g in graphs:
            n_nodes = len(g["nodes"])
            n_edges = len(g["edges"])
            print(f"[ {g['title']} ]  {n_nodes} nodes, {n_edges} edges", flush=True)

            for l in LAYOUTS:
                key = (g["id"], l["id"])
                try:
                    positions = call_layout(port, g, l)
                    data = build_export_payload(g, positions)
                    title = f"{g['title']} — {l['label']}"
                    html = render_interactive_html(template, data, title, commit)
                    rel = f"graphs/{g['id']}__{l['id']}.html"
                    (OUTPUT_DIR / rel).write_text(html, encoding="utf-8")
                    results[key] = (positions, rel)
                    print(f"  {l['label']:<22} OK", flush=True)
                except Exception as exc:
                    print(f"  {l['label']:<22} ERROR: {exc}", file=sys.stderr, flush=True)
                    results[key] = None

            print()

        index_html = render_index(graphs, results, commit, timestamp)
        index_path = OUTPUT_DIR / "index.html"
        index_path.write_text(index_html, encoding="utf-8")

        update_latest_symlink(OUTPUT_DIR)

        ok = sum(1 for v in results.values() if v is not None)
        total = len(results)
        print(f"Done — {ok}/{total} graphs rendered.")
        print(f"Open:  {index_path}")

    finally:
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()


if __name__ == "__main__":
    main()
