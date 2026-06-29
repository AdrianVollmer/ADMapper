#!/usr/bin/env python3
"""
Graph layout test harness for ADMapper.

Generates synthetic test graphs, renders them through every layout algorithm,
produces an interactive HTML file per combination (using the existing export
template), and writes an index.html grid for side-by-side visual comparison.

Usage (from the repo root):
    python3 tools/graph_layout_test.py [output_dir]

Default output_dir: output/graph-layout-test/

The script builds the admapper binary if it is not already present, then
starts the web server in headless mode to call the real layout API, so the
output faithfully reflects what users see in the app.
"""

import json
import math
import os
import random
import socket
import statistics
import subprocess
import sys
import tempfile
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

OUTPUT_DIR = Path(sys.argv[1]).resolve() if len(sys.argv) > 1 else REPO_ROOT / "output" / "graph-layout-test"

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
    {
        "id": "radial",
        "label": "Radial",
        "request": {"algorithm": "radial"},
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


def star(n_leaves, invert=False):
    nodes = [N("hub", "Hub", "Group")]
    edges = []
    for i in range(n_leaves):
        nodes.append(N(f"leaf{i}", f"L{i}", "User"))
        if invert:
            edges.append(E(f"leaf{i}", "hub", "MemberOf"))
        else:
            edges.append(E("hub", f"leaf{i}", "Contains"))
    return nodes, edges


def binary_tree(depth, invert=False):
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
            if invert:
                edges.append(E(nid, parent, "MemberOf"))
            else:
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


def uneven_stars(sizes, bridges=None):
    """Multiple star clusters with wildly different satellite counts."""
    TYPES = ["User", "Computer", "OU", "GPO"]
    nodes, edges = [], []
    hub_ids = []
    for ci, n_leaves in enumerate(sizes):
        hub_id = f"star{ci}_hub"
        hub_ids.append(hub_id)
        nodes.append(N(hub_id, f"Star{ci}Hub", "Group"))
        for li in range(n_leaves):
            lid = f"star{ci}_leaf{li}"
            ntype = TYPES[(ci + li) % len(TYPES)]
            nodes.append(N(lid, f"S{ci}L{li}", ntype))
            edges.append(E(hub_id, lid, "Contains"))
    if bridges:
        for a, b in bridges:
            edges.append(E(hub_ids[a], hub_ids[b], "TrustedBy"))
    return nodes, edges


def shared_parents(n_children, n_parents=2):
    """Many child nodes each connected to the same set of parent nodes.

    Creates n_children nodes that all point to the same n_parents parents.
    In hierarchical L-R layout, parents end up on the right with identical
    barycenters -- a stress test for overlap avoidance.
    """
    CHILD_TYPES = ["User", "Computer", "OU", "GPO"]
    nodes = []
    edges = []
    parent_ids = []
    for pi in range(n_parents):
        pid = f"parent{pi}"
        parent_ids.append(pid)
        nodes.append(N(pid, f"Parent{pi}", "Group"))
    for ci in range(n_children):
        cid = f"child{ci}"
        ctype = CHILD_TYPES[ci % len(CHILD_TYPES)]
        nodes.append(N(cid, f"Child{ci}", ctype))
        for pid in parent_ids:
            edges.append(E(cid, pid, "MemberOf"))
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
        ("star_small", "Small Star", "1 hub + 8 leaves (outward)",
            *star(8)),
        ("star_inverted", "Inverted Star", "1 hub + 8 leaves (inward)",
            *star(8, invert=True)),
        ("binary_tree", "Binary Tree", "Full binary tree, depth 4 (15 nodes)",
            *binary_tree(4)),
        ("binary_tree_inv", "Inverted Binary Tree", "Inverted binary tree, depth 4 (15 nodes)",
            *binary_tree(4, invert=True)),
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
        ("uneven_stars", "Uneven Star Clusters", "3 stars: 200 + 3 + 5 satellites, bridged",
            *uneven_stars([200, 3, 5], bridges=[(0, 1), (1, 2)])),
        ("shared_parents", "Shared Parents", "100 children all with same 2 parents (barycenter tie)",
            *shared_parents(100, 2)),
    ]

    graphs = []
    for item in specs:
        gid, title, desc, nodes, edges = item
        graphs.append({"id": gid, "title": title, "desc": desc, "nodes": nodes, "edges": edges})
    return graphs


# ── Layout quality scoring ────────────────────────────────────────────────────


def _pairwise_distances(positions: dict) -> list[float]:
    """All pairwise Euclidean distances between positioned nodes."""
    pts = list(positions.values())
    dists = []
    for i in range(len(pts)):
        for j in range(i + 1, len(pts)):
            dx = pts[i][0] - pts[j][0]
            dy = pts[i][1] - pts[j][1]
            dists.append(math.sqrt(dx * dx + dy * dy))
    return dists


def _edge_lengths(graph: dict, positions: dict) -> list[float]:
    """Euclidean lengths of all edges with both endpoints positioned."""
    lengths = []
    for e in graph["edges"]:
        sp = positions.get(e["source"])
        tp = positions.get(e["target"])
        if sp and tp:
            dx = sp[0] - tp[0]
            dy = sp[1] - tp[1]
            lengths.append(math.sqrt(dx * dx + dy * dy))
    return lengths


def _count_crossings(graph: dict, positions: dict) -> int:
    """Count edge crossings using line-segment intersection tests."""
    segs = []
    for e in graph["edges"]:
        sp = positions.get(e["source"])
        tp = positions.get(e["target"])
        if sp and tp:
            segs.append((sp, tp))

    def ccw(a, b, c):
        return (c[1] - a[1]) * (b[0] - a[0]) > (b[1] - a[1]) * (c[0] - a[0])

    def intersects(s1, s2):
        a, b = s1
        c, d = s2
        # Skip if they share an endpoint
        if a == c or a == d or b == c or b == d:
            return False
        return ccw(a, c, d) != ccw(b, c, d) and ccw(a, b, c) != ccw(a, b, d)

    crossings = 0
    for i in range(len(segs)):
        for j in range(i + 1, len(segs)):
            if intersects(segs[i], segs[j]):
                crossings += 1
    return crossings


def _grid_occupancy(positions: dict) -> float:
    """Fraction of grid cells that contain at least one node (0..1).

    Grid size adapts to node count so the metric stays meaningful for
    both small (5-node) and large (200+ node) graphs.
    """
    n = len(positions)
    if n < 2:
        return 1.0

    # Scale grid so that perfect uniform coverage yields ~100% occupancy.
    # Aim for roughly 1 node per cell on average.
    grid_size = max(2, min(16, int(math.sqrt(n))))

    pts = list(positions.values())
    xs = [p[0] for p in pts]
    ys = [p[1] for p in pts]
    min_x, max_x = min(xs), max(xs)
    min_y, max_y = min(ys), max(ys)
    span_x = max_x - min_x or 1.0
    span_y = max_y - min_y or 1.0

    occupied = set()
    for x, y in pts:
        ci = min(int((x - min_x) / span_x * grid_size), grid_size - 1)
        ri = min(int((y - min_y) / span_y * grid_size), grid_size - 1)
        occupied.add((ri, ci))

    return len(occupied) / (grid_size * grid_size)


def _nearest_neighbor_stats(positions: dict) -> tuple[float, float]:
    """Compute nearest-neighbor distance statistics.

    Returns (clark_evans_r, nn_cv):
      - clark_evans_r: ratio of observed mean NN distance to the expected
        mean under a uniform distribution over the bounding area.
        R < 1 = clustered, R ~ 1 = uniform, R > 1 = dispersed/regular.
      - nn_cv: coefficient of variation of NN distances.
        Low = evenly spaced, high = irregular spacing.
    """
    pts = list(positions.values())
    n = len(pts)
    if n < 3:
        return 1.0, 0.0

    nn_dists = []
    for i in range(n):
        best = float("inf")
        for j in range(n):
            if i == j:
                continue
            dx = pts[i][0] - pts[j][0]
            dy = pts[i][1] - pts[j][1]
            d = math.sqrt(dx * dx + dy * dy)
            if d < best:
                best = d
        nn_dists.append(best)

    mean_nn = statistics.mean(nn_dists)

    # Clark-Evans: expected mean NN for n points uniform in area A = 0.5 * sqrt(A/n)
    xs = [p[0] for p in pts]
    ys = [p[1] for p in pts]
    w = max(xs) - min(xs) or 1.0
    h = max(ys) - min(ys) or 1.0
    area = w * h
    expected_nn = 0.5 * math.sqrt(area / n)
    r = mean_nn / expected_nn if expected_nn > 1e-9 else 1.0

    # CV
    cv = statistics.stdev(nn_dists) / mean_nn if mean_nn > 1e-9 else 0.0

    return r, cv


def score_layout(graph: dict, positions: dict) -> dict:
    """Compute layout quality metrics.

    Returns a dict with individual metrics and an overall score (0..100).

    Metrics
    -------
    distribution : float  0..1
        Grid-cell occupancy -- fraction of an adaptive grid covered by nodes.
    dispersion : float  0..1
        Clark-Evans R statistic, clamped to 0..1.  Measures whether nodes
        actually *fill* the bounding area or clump into tight clusters with
        large empty gaps.  R~1 = uniform use of space, R<<1 = clustered.
    regularity : float  0..1
        1 - clamp(nearest-neighbor CV).  Measures how *consistent* the
        spacing is (independent of whether the space is well-used).
    edge_consistency : float  0..1
        1 - clamp(edge-length CV). Edges should have similar lengths.
    min_separation : float  0..1
        Fraction of node pairs with distance > a minimal threshold.
    crossings : int
        Raw count of edge crossings (lower is better; -1 = skipped).
    crossing_penalty : float  0..1
        1 - clamp(crossings / max_possible). Penalises crossings.
    overall : float  0..100
        Weighted combination of the above.
    """
    n = len(positions)
    if n < 2:
        return {
            "distribution": 1.0, "dispersion": 1.0, "regularity": 1.0,
            "edge_consistency": 1.0, "min_separation": 1.0,
            "crossings": 0, "crossing_penalty": 1.0, "overall": 100.0,
        }

    # -- Distribution: grid occupancy
    distribution = _grid_occupancy(positions)

    # -- Dispersion + Regularity from nearest-neighbor analysis
    clark_evans_r, nn_cv = _nearest_neighbor_stats(positions)
    # R is theoretically 0..2.15 for perfectly regular hex grids, but
    # for our purposes clamp to 0..1 (uniform = 1, clustered = 0).
    dispersion = min(1.0, clark_evans_r)
    regularity = max(0.0, 1.0 - nn_cv / 2.0)

    # -- Edge-length consistency
    e_lens = _edge_lengths(graph, positions)
    if len(e_lens) >= 2:
        e_mean = statistics.mean(e_lens)
        e_cv = statistics.stdev(e_lens) / e_mean if e_mean > 1e-9 else 0.0
        edge_consistency = max(0.0, 1.0 - e_cv / 3.0)
    else:
        edge_consistency = 1.0

    # -- Minimum separation: what fraction of pairs are above a threshold?
    #    Threshold = 1% of bounding-box diagonal.
    pts = list(positions.values())
    xs = [p[0] for p in pts]
    ys = [p[1] for p in pts]
    diag = math.sqrt((max(xs) - min(xs)) ** 2 + (max(ys) - min(ys)) ** 2) or 1.0
    threshold = diag * 0.01
    pw = _pairwise_distances(positions)
    min_separation = sum(1 for d in pw if d > threshold) / len(pw) if pw else 1.0

    # -- Crossings (skip for large graphs -- O(E^2) gets expensive)
    n_edges = len(graph["edges"])
    if n_edges <= 500:
        crossings = _count_crossings(graph, positions)
        max_cross = max(1, n_edges * (n_edges - 1) // 2)
        crossing_penalty = max(0.0, 1.0 - crossings / max_cross)
    else:
        crossings = -1  # skipped
        crossing_penalty = 1.0  # neutral

    # -- Overall (weighted average)
    #    Dispersion is the heaviest weight: it catches the "tight clusters
    #    floating in a sea of whitespace" problem that other metrics miss.
    overall = (
        distribution * 10
        + dispersion * 30
        + regularity * 10
        + edge_consistency * 15
        + min_separation * 10
        + crossing_penalty * 25
    )

    return {
        "distribution": round(distribution, 3),
        "dispersion": round(dispersion, 3),
        "regularity": round(regularity, 3),
        "edge_consistency": round(edge_consistency, 3),
        "min_separation": round(min_separation, 3),
        "crossings": crossings,
        "crossing_penalty": round(crossing_penalty, 3),
        "overall": round(overall, 1),
    }


def _score_color(value: float) -> str:
    """Map a 0..1 metric to a CSS color: red -> yellow -> green."""
    if value >= 0.75:
        return "#22c55e"  # green
    if value >= 0.5:
        return "#eab308"  # yellow
    return "#ef4444"  # red


def _overall_color(score: float) -> str:
    """Map overall score (0..100) to a CSS color."""
    return _score_color(score / 100.0)


def render_score_badge(scores: dict) -> str:
    """Render a compact HTML score badge for a thumbnail cell."""
    o = scores["overall"]
    color = _overall_color(o)
    tooltip_parts = [
        f"Distribution: {scores['distribution']:.0%}",
        f"Dispersion: {scores['dispersion']:.0%}",
        f"Regularity: {scores['regularity']:.0%}",
        f"Edge consistency: {scores['edge_consistency']:.0%}",
        f"Min separation: {scores['min_separation']:.0%}",
    ]
    if scores["crossings"] >= 0:
        tooltip_parts.append(f"Crossings: {scores['crossings']}")
    tooltip = "&#10;".join(tooltip_parts)

    return (
        f'<div class="score" style="color:{color}" title="{tooltip}">'
        f'{o:.0f}</div>'
    )


# ── Binary / server management ─────────────────────────────────────────────────


def build_binary():
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


def render_interactive_html(template: str, data: dict, title: str) -> str:
    html = template.replace(
        "__GRAPH_DATA_PLACEHOLDER__",
        json.dumps(data, indent=2),
    )
    html = html.replace(
        "<title>ADMapper Graph Export</title>",
        f"<title>{title}</title>",
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
        # Sigma.js uses y-up (positive y = up on screen); SVG uses y-down.
        # Flip y so thumbnails match what sigma.js renders.
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


def render_index(graphs: list, results: dict) -> str:
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
                positions, rel_path, scores = entry
                svg = render_thumbnail(g, positions)
                badge = render_score_badge(scores)
                cells.append(
                    f'<td class="cell">'
                    f'<a href="{rel_path}" target="_blank" title="Open interactive view">'
                    f'{svg}</a>{badge}</td>'
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

    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="UTF-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1.0"/>
  <title>ADMapper — Layout Test Grid</title>
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
    .score {{ font-size: 11px; font-weight: 700; text-align: center; padding: 2px 0; font-variant-numeric: tabular-nums; }}
    tr:hover .gh {{ background: #1c2a42; }}
  </style>
</head>
<body>
  <div id="topbar">
    <h1>ADMapper — Layout Test Grid</h1>
    <p>Click a thumbnail to open the interactive graph.</p>
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


def main():
    print(f"Output: {OUTPUT_DIR}", flush=True)

    binary = ensure_binary()
    print(f"Binary: {binary}", flush=True)

    template = TEMPLATE_PATH.read_text(encoding="utf-8")

    graphs_dir = OUTPUT_DIR / "graphs"
    graphs_dir.mkdir(parents=True, exist_ok=True)

    port = free_port()
    print(f"Starting admapper on port {port}…", flush=True)

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
                    html = render_interactive_html(template, data, title)
                    rel = f"graphs/{g['id']}__{l['id']}.html"
                    (OUTPUT_DIR / rel).write_text(html, encoding="utf-8")
                    scores = score_layout(g, positions)
                    results[key] = (positions, rel, scores)
                    print(f"  {l['label']:<22} OK  score={scores['overall']:.0f}", flush=True)
                except Exception as exc:
                    print(f"  {l['label']:<22} ERROR: {exc}", file=sys.stderr, flush=True)
                    results[key] = None

            print()

        index_html = render_index(graphs, results)
        index_path = OUTPUT_DIR / "index.html"
        index_path.write_text(index_html, encoding="utf-8")

        ok = sum(1 for v in results.values() if v is not None)
        total = len(results)
        print(f"Done — {ok}/{total} graphs rendered.")
        print(f"Open: {index_path}")

    finally:
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()


if __name__ == "__main__":
    main()
