"""Build the self-contained visual report (report/abinitio_to_pyspark_demo.html).

Inputs: the legacy .mp graphs (parsed for the left-hand diagrams and the
Ab Initio code panels), the PySpark modules (inspected for the right-hand code
panels) and work/results/run_results.json written by run_demo.py. Every number
in the page comes from that results file.
"""
from __future__ import annotations

import html
import inspect
import json
import re
import sys
from decimal import Decimal
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

from gmrd_spark import ld_trades_tdw, txfm_trade_enrichment  # noqa: E402
from gmrd_spark.dml import REPO_ROOT, load_record, typed_schema  # noqa: E402
from gmrd_spark.mp_graph import Graph, parse_mp  # noqa: E402

TYPE_STYLE = {  # Ab Initio component type -> (fill, stroke, glyph)
    "input_file": ("#e7f6ec", "#2f9e5b", "▤"),
    "reformat": ("#e8f0fe", "#3367d6", "ƒ"),
    "gather": ("#e6f7f7", "#14868c", "⋃"),
    "lookup": ("#f3e8fd", "#8e44c9", "⌕"),
    "output_file": ("#fff1e5", "#d9730d", "▦"),
    "output_table": ("#fff1e5", "#d9730d", "⛁"),
    "partition_by_expression": ("#fff8db", "#b7950b", "⑃"),
    "run_program": ("#f1f3f4", "#6b7280", "⚙"),
}
GRAPHS = [
    ("txfm_trade_enrichment", REPO_ROOT / "mp/transform/txfm_trade_enrichment.mp"),
    ("ld_trades_tdw", REPO_ROOT / "mp/load/ld_trades_tdw.mp"),
]
NODE_W, NODE_H, COL_W, ROW_H, PAD = 150, 46, 190, 64, 24
esc = html.escape


# ---------------------------------------------------------------------------- layout + SVG
def layered_layout(nodes: list[str], edges: list[tuple[str, str]], min_layer: dict[str, int]) -> dict:
    layer = {n: min_layer.get(n, 0) for n in nodes}
    for _ in nodes:  # longest-path layering
        for a, b in edges:
            layer[b] = max(layer[b], layer[a] + 1, min_layer.get(b, 0))
    cols: dict[int, list[str]] = {}
    for n in nodes:
        cols.setdefault(layer[n], []).append(n)
    pos: dict[str, tuple[float, float]] = {}
    height = max(len(v) for v in cols.values())
    for li in sorted(cols):
        col = cols[li]
        if li > 0:
            def bary(n):
                ps = [pos[a][1] for a, b in edges if b == n and a in pos]
                return sum(ps) / len(ps) if ps else 0
            col.sort(key=bary)
        off = (height - len(col)) * ROW_H / 2
        for i, n in enumerate(col):
            pos[n] = (PAD + li * COL_W, PAD + 22 + off + i * ROW_H)
    width = PAD * 2 + (max(cols) + 1) * COL_W - (COL_W - NODE_W)
    return {"pos": pos, "layer": layer, "w": width, "h": PAD * 2 + 22 + height * ROW_H - (ROW_H - NODE_H)}


def _edge_path(p1, p2) -> str:
    x1, y1 = p1[0] + NODE_W, p1[1] + NODE_H / 2
    x2, y2 = p2[0], p2[1] + NODE_H / 2
    mx = (x1 + x2) / 2
    return f"M{x1},{y1} C{mx},{y1} {mx},{y2} {x2},{y2}"


def _node(x, y, title, subtitle, ab_type, comp, extra_cls="") -> str:
    fill, stroke, glyph = TYPE_STYLE.get(ab_type, ("#fff", "#999", "?"))
    return (
        f'<g class="node {extra_cls}" data-comp="{esc(comp)}">'
        f'<rect x="{x}" y="{y}" width="{NODE_W}" height="{NODE_H}" rx="8" fill="{fill}" stroke="{stroke}" stroke-width="1.6"/>'
        f'<circle cx="{x}" cy="{y + NODE_H / 2}" r="3.5" fill="{stroke}"/>'
        f'<circle cx="{x + NODE_W}" cy="{y + NODE_H / 2}" r="3.5" fill="{stroke}"/>'
        f'<text x="{x + 10}" y="{y + 19}" class="glyph" fill="{stroke}">{glyph}</text>'
        f'<text x="{x + 28}" y="{y + 19}" class="ntitle">{esc(title)}</text>'
        f'<text x="{x + 28}" y="{y + 35}" class="nsub">{esc(subtitle)}</text></g>'
    )


def abinitio_svg(g: Graph) -> str:
    names = [c.name for c in g.components]
    edges = [(a, b) for a, b, _ in g.edges]
    phase_idx = {p: i for i, p in enumerate(g.phases)}
    min_layer: dict[str, int] = {}
    # Phase breaks: a phase starts after every component of earlier phases.
    lay = layered_layout(names, edges, {})
    for p in g.phases[1:]:
        prev = [c.name for c in g.components if phase_idx[c.phase] < phase_idx[p]]
        start = max(max(lay["layer"][n] for n in prev) + 1, 0)
        for c in g.components:
            if c.phase == p:
                min_layer[c.name] = max(min_layer.get(c.name, 0), start)
        lay = layered_layout(names, edges, min_layer)
    pos = lay["pos"]
    phase_breaks = []
    for p in g.phases[1:]:
        firsts = [c.name for c in g.components if c.phase == p and not c.inputs]
        prev_sinks = [c.name for c in g.components if phase_idx[c.phase] == phase_idx[p] - 1
                      and not any(a == c.name for a, _ in edges)]
        phase_breaks += [(a, b) for b in firsts for a in prev_sinks]
    out = [f'<svg viewBox="0 0 {lay["w"]} {lay["h"]}" class="dag" xmlns="http://www.w3.org/2000/svg">']
    for p in g.phases:  # phase bands
        xs = [pos[c.name][0] for c in g.components if c.phase == p]
        x0, x1 = min(xs) - 12, max(xs) + NODE_W + 12
        out.append(f'<rect x="{x0}" y="6" width="{x1 - x0}" height="{lay["h"] - 12}" rx="10" class="phase"/>'
                   f'<text x="{x0 + 8}" y="22" class="phasel">phase: {esc(p)}</text>')
    for a, b, port in g.edges:
        cls = "edge lookup" if port == "lookup" else "edge"
        out.append(f'<path d="{_edge_path(pos[a], pos[b])}" class="{cls}" marker-end="url(#arr)"/>')
    for a, b in phase_breaks:
        out.append(f'<path d="{_edge_path(pos[a], pos[b])}" class="edge pbreak" marker-end="url(#arr)"/>')
    for c in g.components:
        x, y = pos[c.name]
        out.append(_node(x, y, c.name, c.type, c.type, c.name))
    out.append(_ARROW_DEFS + "</svg>")
    return "".join(out)


def pyspark_svg(steps: list[dict]) -> str:
    names = [s["name"] for s in steps]
    edges = [(p, s["name"]) for s in steps for p in s["parents"]]
    lay = layered_layout(names, edges, {})
    pos = lay["pos"]
    rows = {s["name"]: s["rows"] for s in steps}
    out = [f'<svg viewBox="0 0 {lay["w"]} {lay["h"]}" class="dag spark" xmlns="http://www.w3.org/2000/svg">',
           f'<text x="{PAD}" y="22" class="phasel">PySpark (local[4]) – edge labels = rows from count()</text>']
    for a, b in edges:
        p1, p2 = pos[a], pos[b]
        out.append(f'<path d="{_edge_path(p1, p2)}" class="edge sp" marker-end="url(#arr2)"/>')
        if rows.get(a) is not None and rows.get(b) is not None:
            lx, ly = (p1[0] + NODE_W + p2[0]) / 2, (p1[1] + p2[1]) / 2 + NODE_H / 2 - 4
            out.append(f'<rect x="{lx - 17}" y="{ly - 10}" width="34" height="16" rx="8" class="cntbg"/>'
                       f'<text x="{lx}" y="{ly + 2}" class="cnt">{rows[a]}</text>')
    for s in steps:
        x, y = pos[s["name"]]
        label = s["name"].replace("split_by_source:", "filter:")
        sub = s["construct"] if s["status"] != "stub" else "not migrated (stub)"
        sub = (sub[:19] + "…") if len(sub) > 20 else sub
        out.append(_node(x, y, label, sub, s["ab_type"], s["ab_component"], "stub" if s["status"] == "stub" else ""))
    out.append(_ARROW_DEFS + "</svg>")
    return "".join(out)


_ARROW_DEFS = (
    '<defs><marker id="arr" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto">'
    '<path d="M0,0 L10,5 L0,10 z" fill="#64748b"/></marker>'
    '<marker id="arr2" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto">'
    '<path d="M0,0 L10,5 L0,10 z" fill="#e25a1c"/></marker></defs>'
)


# ---------------------------------------------------------------------------- code highlighting
_PY_KW = r"\b(def|return|for|in|if|else|elif|not|and|or|import|from|as|None|True|False|raise|with|lambda)\b"
_AB_KW = r"\b(if|else|in|out|lookup|is_defined|decimal_strip|string_substring|current_timestamp|CAST|AS|DELETE|FROM|WHERE|AND|DATE|FORMAT|DECIMAL|BIGINT|INTEGER|TIMESTAMP|record|end|type|string|decimal)\b"


def highlight(code: str, lang: str) -> str:
    kw = _PY_KW if lang == "py" else _AB_KW
    tok = re.compile(r'(?P<c>#[^\n]*|/\*.*?\*/|"""[\s\S]*?""")|(?P<s>"[^"\n]*"|\'[^\'\n]*\')|(?P<k>' + kw + r")|(?P<f>\bF\.\w+)", re.S)
    out, last = [], 0
    for m in tok.finditer(code):
        out.append(esc(code[last:m.start()]))
        cls = {"c": "tc", "s": "ts", "k": "tk", "f": "tf"}[m.lastgroup]
        out.append(f'<span class="{cls}">{esc(m.group())}</span>')
        last = m.end()
    out.append(esc(code[last:]))
    return "".join(out)


def dedent_block(text: str) -> str:
    lines = [ln.rstrip() for ln in text.strip("\n").splitlines()]
    ind = min((len(ln) - len(ln.lstrip()) for ln in lines if ln.strip()), default=0)
    return "\n".join(ln[ind:] for ln in lines)


def code_pair(title, ab_label, ab_code, py_label, py_code, blurb="") -> str:
    return f"""
    <div class="pair">
      <h3>{esc(title)}</h3>{f'<p class="muted">{blurb}</p>' if blurb else ''}
      <div class="cols2">
        <div class="code ab"><div class="ch"><span class="pill ab">Ab Initio</span>{esc(ab_label)}</div>
          <pre>{highlight(ab_code, 'ab')}</pre></div>
        <div class="code py"><div class="ch"><span class="pill py">PySpark</span>{esc(py_label)}</div>
          <pre>{highlight(py_code, 'py')}</pre></div>
      </div>
    </div>"""


def src_label(fn) -> str:
    lines, start = inspect.getsourcelines(fn)
    return f"{inspect.getsourcefile(fn).split('pyspark_migration/')[-1]}:{start}-{start + len(lines) - 1}"


# ---------------------------------------------------------------------------- charts
def bar_chart_rows(res: dict) -> str:
    steps = {s["name"]: s for s in res["steps"]}
    srcs = ["CALYPSO", "MUREX", "SUMMIT"]
    stages = [("extract", lambda s: steps[f"read_{s.lower()}"]["rows"]),
              ("normalised", lambda s: steps[f"normalise_{s.lower()}"]["rows"]),
              ("enriched", lambda s: steps[f"split_by_source:{s}"]["rows"]),
              ("staged", lambda s: res["staged_run1"][ld_trades_tdw.SOURCES[s]])]
    colors = ["#94a3b8", "#3367d6", "#8e44c9", "#d9730d"]
    vmax = max(f(s) for _, f in stages for s in srcs)
    w, h, base = 520, 230, 190
    out = [f'<svg viewBox="0 0 {w} {h}" class="chart">']
    gx = 60
    for s in srcs:
        for i, (lbl, f) in enumerate(stages):
            v = f(s)
            bh = v / vmax * 150
            x = gx + i * 28
            out.append(f'<rect x="{x}" y="{base - bh}" width="22" height="{bh}" fill="{colors[i]}" rx="3"/>'
                       f'<text x="{x + 11}" y="{base - bh - 4}" class="bv">{v}</text>')
        out.append(f'<text x="{gx + 53}" y="{base + 18}" class="bl">{s}</text>')
        gx += 150
    out.append(f'<line x1="50" y1="{base}" x2="{w - 10}" y2="{base}" stroke="#cbd5e1"/>')
    lx = 60
    for i, (lbl, _) in enumerate(stages):
        out.append(f'<rect x="{lx}" y="8" width="11" height="11" fill="{colors[i]}" rx="2"/>'
                   f'<text x="{lx + 15}" y="18" class="lg">{lbl}</text>')
        lx += 100
    out.append("</svg>")
    return "".join(out)


def donut(data: dict[str, int], colors: dict[str, str]) -> str:
    import math
    total = sum(data.values())
    cx, cy, r, sw = 90, 90, 62, 26
    out = ['<svg viewBox="0 0 400 180" class="chart">']
    a0 = -math.pi / 2
    for k, v in data.items():
        a1 = a0 + 2 * math.pi * v / total
        large = 1 if a1 - a0 > math.pi else 0
        x0, y0 = cx + r * math.cos(a0), cy + r * math.sin(a0)
        x1, y1 = cx + r * math.cos(a1 - 1e-6), cy + r * math.sin(a1 - 1e-6)
        out.append(f'<path d="M{x0:.2f},{y0:.2f} A{r},{r} 0 {large} 1 {x1:.2f},{y1:.2f}" stroke="{colors[k]}" '
                   f'stroke-width="{sw}" fill="none"/>')
        a0 = a1
    out.append(f'<text x="{cx}" y="{cy - 2}" class="dn">{total}</text><text x="{cx}" y="{cy + 16}" class="dl">trades</text>')
    y = 50
    for k, v in data.items():
        out.append(f'<rect x="190" y="{y - 10}" width="12" height="12" rx="2" fill="{colors[k]}"/>'
                   f'<text x="208" y="{y}" class="lg">{esc(k)}: <tspan font-weight="700">{v}</tspan></text>')
        y += 28
    out.append("</svg>")
    return "".join(out)


def stacked_notional(res: dict) -> str:
    rows = res["notional_gbp_by_source_asset_class"]
    acs = ["IR", "FX", "EQ", "CR", "CO", "OTHER", "NULL"]
    colors = {"IR": "#3367d6", "FX": "#14868c", "EQ": "#2f9e5b", "CR": "#8e44c9", "CO": "#b7950b",
              "OTHER": "#94a3b8", "NULL": "#ef4444"}
    by = {}
    for r in rows:
        by.setdefault(r["source"], {})[r["asset_class"]] = float(Decimal(r["notional_gbp"]))
    vmax = max(sum(v.values()) for v in by.values())
    w, base = 520, 200
    out = [f'<svg viewBox="0 0 {w} 250" class="chart">']
    x = 70
    for src in ["CALYPSO", "MUREX", "SUMMIT"]:
        y = base
        for ac in acs:
            v = by.get(src, {}).get(ac, 0)
            if not v:
                continue
            bh = v / vmax * 160
            out.append(f'<rect x="{x}" y="{y - bh}" width="70" height="{bh}" fill="{colors[ac]}"><title>{src} {ac}: '
                       f'£{v:,.0f}</title></rect>')
            y -= bh
        tot = sum(by.get(src, {}).values())
        out.append(f'<text x="{x + 35}" y="{y - 6}" class="bv">£{tot / 1e6:,.0f}m</text>'
                   f'<text x="{x + 35}" y="{base + 18}" class="bl">{src}</text>')
        x += 140
    out.append(f'<line x1="60" y1="{base}" x2="{w - 20}" y2="{base}" stroke="#cbd5e1"/>')
    lx = 30
    for ac in acs:
        out.append(f'<rect x="{lx}" y="232" width="11" height="11" fill="{colors[ac]}" rx="2"/>'
                   f'<text x="{lx + 15}" y="242" class="lg">{ac if ac != "NULL" else "NULL (no rule)"}</text>')
        lx += 58 if ac != "OTHER" else 66
    out.append("</svg>")
    return "".join(out)


# ---------------------------------------------------------------------------- sections
def show_ws(v: str) -> str:
    """Render leading/trailing blanks visibly so decimal_strip's effect is obvious."""
    if v is None:
        return '<span class="null">NULL</span>'
    if v == "":
        return '<span class="null">∅</span>'
    lead = len(v) - len(v.lstrip(" "))
    trail = len(v) - len(v.rstrip(" "))
    core = esc(v.strip(" "))
    return f'<span class="ws">{"·" * lead}</span>{core}<span class="ws">{"·" * trail}</span>'


RAW_FIELDS = {
    "CALYPSO": ["trade_ref", "product_type", "book_id", "notional_amount", "notional_ccy", "market_value"],
    "MUREX": ["mx_trade_id", "mx_product_type", "mx_book", "mx_nominal", "mx_nominal_ccy", "mx_mkt_value"],
    "SUMMIT": ["smt_deal_no", "smt_product_group", "smt_book", "smt_notional", "smt_notional_ccy", "smt_mkt_value"],
}
OUT_FIELDS = ["trade_id", "source_system", "asset_class", "entity_code", "notional_amount", "notional_amount_gbp",
              "market_value", "market_value_gbp"]
CHANGED = {"source_system", "asset_class", "entity_code", "notional_amount", "notional_amount_gbp", "market_value",
           "market_value_gbp"}


def samples_html(res: dict) -> str:
    out = []
    for s in res["samples"]:
        raw, enr = s["raw"], s["enriched"]
        rf = RAW_FIELDS[s["source"]]
        out.append(f"""
        <div class="sample">
          <div class="sbox before"><div class="sh">BEFORE · {esc(s['source'])} extract row (synthetic)</div>
            <table>{''.join(f'<tr><th>{esc(f)}</th><td>{show_ws(raw.get(f))}</td></tr>' for f in rf)}</table></div>
          <div class="sarrow">→<small>PySpark</small></div>
          <div class="sbox after"><div class="sh">AFTER · trades_enriched row (trade_record_t)</div>
            <table>{''.join(f'<tr class="{"chg" if f in CHANGED else ""}"><th>{esc(f)}</th><td>{show_ws(enr.get(f))}</td></tr>' for f in OUT_FIELDS)}</table></div>
        </div>""")
    return "".join(out)


def mapping_table(res: dict, graphs: dict[str, Graph]) -> str:
    seen, rows = set(), []
    for s in res["steps"]:
        key = (s["graph"], s["ab_component"])
        if key in seen:
            continue
        seen.add(key)
        comp = graphs[s["graph"]].component(s["ab_component"])
        fill, stroke, glyph = TYPE_STYLE.get(comp.type, ("#fff", "#999", "?"))
        same = [x for x in res["steps"] if (x["graph"], x["ab_component"]) == key]
        rows_txt = " / ".join(str(x["rows"]) for x in same if x["rows"] is not None) or "–"
        constructs = sorted({x["construct"] for x in same})
        status = '<span class="pill stub">stub</span>' if s["status"] == "stub" else '<span class="pill ok">ported</span>'
        rows.append(
            f'<tr data-comp="{esc(comp.name)}"><td class="muted">{esc(s["graph"])}<br><small>{esc(comp.phase)}</small></td>'
            f'<td><span class="chip" style="background:{fill};border-color:{stroke};color:{stroke}">{glyph} {esc(comp.type)}</span>'
            f'<br><code>{esc(comp.name)}</code></td><td class="arrow">→</td>'
            f'<td>{"<br>".join(f"<code>{esc(c)}</code>" for c in constructs)}<br>'
            f'<small class="muted">{esc(s["source_ref"] or "")}</small>'
            f'{f"<br><small class=note>{esc(s["note"])}</small>" if s["note"] else ""}</td>'
            f'<td class="num">{rows_txt}</td><td>{status}</td></tr>')
    return ("<table class='map'><thead><tr><th>Graph / phase</th><th>Ab Initio component</th><th></th>"
            "<th>PySpark construct</th><th>Rows (this run)</th><th></th></tr></thead><tbody>"
            + "".join(rows) + "</tbody></table>")


def checks_table(res: dict) -> str:
    rows = "".join(
        f'<tr><td><span class="pill {"ok" if c["status"] == "PASS" else "bad"}">{c["status"]}</span></td>'
        f'<td>{esc(c["name"])}{f"<br><small class=muted>{esc(c["detail"])}</small>" if c["detail"] else ""}</td>'
        f'<td class="num">{esc(str(c["expected"]))}</td><td class="num">{esc(str(c["actual"]))}</td></tr>'
        for c in res["checks"])
    return ("<table class='checks'><thead><tr><th></th><th>Check</th><th>Expected</th><th>Actual</th></tr></thead>"
            f"<tbody>{rows}</tbody></table>")


def compact_plan(plan: str) -> str:
    lines = []
    for ln in plan.splitlines():
        ln = re.sub(r"#\d+L?", "", ln)
        ln = re.sub(r"\[plan_id=\d+\]", "", ln)
        lines.append(ln if len(ln) <= 170 else ln[:167] + "…")
    return "\n".join(lines)


def dml_vs_schema() -> tuple[str, str]:
    ref = "dml/trade_record.dml::trade_record_t"
    fields = load_record(ref)
    dml_txt = (REPO_ROOT / "dml/trade_record.dml").read_text()
    m = re.search(r"type trade_record_t =.*?end;", dml_txt, re.S)
    dml_lines = m.group(0).splitlines()
    ab = "\n".join(dml_lines[:14] + ["    ...", *dml_lines[-3:]])
    schema = typed_schema(ref)
    py = ["# generated at runtime by gmrd_spark.dml.typed_schema(", f'#   "{ref}")', "StructType(["]
    py += [f'    StructField("{f.name}", {f.dataType.simpleString()}),' for f in schema.fields[:12]]
    py += [f"    ...  # {len(fields)} fields in total", "])"]
    return ab, "\n".join(py)


CSS = """
:root{--ink:#0f172a;--muted:#64748b;--line:#e2e8f0;--bg:#f8fafc;--ab:#1f4e79;--py:#e25a1c}
*{box-sizing:border-box}body{margin:0;font:14px/1.5 -apple-system,Segoe UI,Roboto,Helvetica,Arial,sans-serif;color:var(--ink);background:var(--bg)}
header{background:linear-gradient(120deg,#0b2545 0%,#13315c 55%,#8d2b0b 100%);color:#fff;padding:34px 48px 30px}
header h1{margin:0 0 6px;font-size:28px;letter-spacing:-.3px}header p{margin:0;opacity:.85;max-width:980px}
.badges{margin-top:14px;display:flex;gap:8px;flex-wrap:wrap}.badge{background:rgba(255,255,255,.14);border:1px solid rgba(255,255,255,.25);padding:3px 10px;border-radius:999px;font-size:12px}
.badge.warn{background:#fde68a;color:#78350f;border-color:#f59e0b;font-weight:600}
main{max-width:1380px;margin:0 auto;padding:24px 32px 60px}
section{background:#fff;border:1px solid var(--line);border-radius:14px;padding:22px 26px;margin:18px 0;box-shadow:0 1px 2px rgba(15,23,42,.04)}
section>h2{margin:0 0 4px;font-size:19px}section>p.lead{margin:0 0 14px;color:var(--muted)}
.kpis{display:grid;grid-template-columns:repeat(6,1fr);gap:12px;margin-top:-46px}
.kpi{background:#fff;border:1px solid var(--line);border-radius:12px;padding:14px 16px;box-shadow:0 4px 14px rgba(15,23,42,.08)}
.kpi b{display:block;font-size:26px;letter-spacing:-.5px}.kpi span{color:var(--muted);font-size:12px}
.kpi.good b{color:#15803d}
.cols2{display:grid;grid-template-columns:1fr 1fr;gap:16px}
.panel{border:1px solid var(--line);border-radius:12px;padding:10px 12px;background:#fcfdff;overflow:auto}
.panel h4{margin:2px 0 6px;font-size:13px;display:flex;gap:8px;align-items:center}
svg.dag{width:100%;height:auto;min-width:560px}
.phase{fill:#f1f5f9;stroke:#cbd5e1;stroke-dasharray:4 3}.phasel{font-size:11px;fill:#475569;font-weight:600;letter-spacing:.3px}
.edge{fill:none;stroke:#64748b;stroke-width:1.6}.edge.lookup{stroke:#8e44c9;stroke-dasharray:5 3}.edge.pbreak{stroke:#94a3b8;stroke-dasharray:2 4}
.edge.sp{stroke:#e25a1c;stroke-width:1.8}
.ntitle{font-size:11.5px;font-weight:700;fill:#0f172a}.nsub{font-size:10px;fill:#475569;font-family:ui-monospace,Menlo,Consolas,monospace}
.glyph{font-size:14px;font-weight:700}.cntbg{fill:#fff;stroke:#e25a1c}.cnt{font-size:10px;font-weight:700;fill:#e25a1c;text-anchor:middle}
.node{cursor:pointer;transition:opacity .15s}.node.stub rect{stroke-dasharray:4 3;fill:#f8fafc}
.dim .node{opacity:.25}.dim .node.hl{opacity:1}.node.hl rect{stroke-width:3;filter:drop-shadow(0 0 6px rgba(226,90,28,.45))}
tr.hl td{background:#fff7ed}
.legend{display:flex;gap:12px;flex-wrap:wrap;font-size:12px;color:var(--muted);margin:10px 0 0}
.chip{display:inline-block;border:1px solid;border-radius:6px;padding:1px 7px;font-size:11.5px;font-weight:600}
table{border-collapse:collapse;width:100%}th,td{text-align:left;padding:7px 9px;border-bottom:1px solid var(--line);vertical-align:top}
thead th{font-size:12px;color:var(--muted);font-weight:600;background:#f8fafc}
td.num{text-align:right;font-variant-numeric:tabular-nums;font-family:ui-monospace,Menlo,Consolas,monospace}
td.arrow{color:var(--py);font-size:18px;font-weight:700;width:20px}
code{font-family:ui-monospace,Menlo,Consolas,monospace;font-size:12px;background:#f1f5f9;padding:1px 5px;border-radius:4px}
.muted{color:var(--muted)}small.note{color:#9a3412}
.pill{display:inline-block;padding:1px 9px;border-radius:999px;font-size:11px;font-weight:700;letter-spacing:.3px}
.pill.ok{background:#dcfce7;color:#166534}.pill.bad{background:#fee2e2;color:#991b1b}.pill.stub{background:#f1f5f9;color:#475569;border:1px dashed #94a3b8}
.pill.ab{background:#dbeafe;color:var(--ab)}.pill.py{background:#ffedd5;color:var(--py)}
.pair{margin:6px 0 22px}.pair h3{margin:0 0 4px;font-size:15px}
.code{border:1px solid var(--line);border-radius:10px;overflow:hidden;background:#0b1220}
.code .ch{background:#111a2e;color:#cbd5e1;font-size:12px;padding:7px 10px;display:flex;gap:8px;align-items:center;font-family:ui-monospace,Menlo,Consolas,monospace}
.code pre{margin:0;padding:12px 14px;color:#e2e8f0;font:12px/1.55 ui-monospace,Menlo,Consolas,monospace;max-height:430px;overflow:auto}
.code.ab{border-top:3px solid #3b82f6}.code.py{border-top:3px solid var(--py)}
.tk{color:#c084fc}.ts{color:#86efac}.tc{color:#64748b;font-style:italic}.tf{color:#fdba74}
.sample{display:grid;grid-template-columns:1fr 70px 1fr;gap:6px;align-items:center;margin:10px 0}
.sbox{border:1px solid var(--line);border-radius:10px;overflow:hidden}.sbox .sh{font-size:11.5px;font-weight:700;padding:6px 10px;letter-spacing:.3px}
.before .sh{background:#f1f5f9;color:#334155}.after .sh{background:#fff1e5;color:#9a3412}
.sbox td{font-family:ui-monospace,Menlo,Consolas,monospace;font-size:12px;white-space:pre}.sbox th{width:42%;font-weight:600;font-size:12px;color:#475569}
.sbox tr.chg td{background:#fef3c7}.ws{color:#ef4444;font-weight:700}.null{color:#94a3b8;font-style:italic}
.sarrow{text-align:center;font-size:30px;color:var(--py);font-weight:700;line-height:1}.sarrow small{display:block;font-size:10px;letter-spacing:.4px}
.charts{display:grid;grid-template-columns:1.1fr 1fr;gap:16px}.chart{width:100%;height:auto}
.bv{font-size:10.5px;text-anchor:middle;fill:#334155;font-weight:700}.bl{font-size:11px;text-anchor:middle;fill:#475569;font-weight:600}
.lg{font-size:11.5px;fill:#334155}.dn{font-size:26px;font-weight:800;text-anchor:middle;fill:#0f172a}.dl{font-size:11px;text-anchor:middle;fill:#64748b}
.findings{display:grid;grid-template-columns:repeat(3,1fr);gap:12px}
.finding{border:1px solid #fed7aa;background:#fffbf5;border-radius:12px;padding:12px 14px}
.finding h4{margin:0 0 4px;font-size:13.5px}.finding p{margin:0 0 6px;color:#475569;font-size:12.5px}
.finding .obs{font-family:ui-monospace,Menlo,Consolas,monospace;font-size:11.5px;color:#9a3412;background:#fff1e5;border-radius:6px;padding:5px 7px}
details pre{background:#0b1220;color:#cbd5e1;padding:12px;border-radius:10px;font:11px/1.5 ui-monospace,Menlo,Consolas,monospace;overflow:auto;max-height:520px}
.honesty{border-left:4px solid #f59e0b;background:#fffbeb}
.run pre{background:#0b1220;color:#e2e8f0;padding:12px 14px;border-radius:10px;font:12.5px/1.6 ui-monospace,Menlo,Consolas,monospace}
"""

JS = """
document.querySelectorAll('[data-comp]').forEach(el=>{
  const c=el.dataset.comp;
  el.addEventListener('mouseenter',()=>{
    document.querySelectorAll('svg.dag').forEach(s=>s.classList.add('dim'));
    document.querySelectorAll('[data-comp="'+c+'"]').forEach(x=>x.classList.add('hl'));});
  el.addEventListener('mouseleave',()=>{
    document.querySelectorAll('svg.dag').forEach(s=>s.classList.remove('dim'));
    document.querySelectorAll('.hl').forEach(x=>x.classList.remove('hl'));});
});
"""


def build(results_path: Path, out_path: Path) -> Path:
    res = json.loads(results_path.read_text())
    graphs = {name: parse_mp(path) for name, path in GRAPHS}
    steps_by_graph = {g: [s for s in res["steps"] if s["graph"] == g] for g in graphs}
    n_components = sum(len(g.components) for g in graphs.values())
    n_stub = len({(s["graph"], s["ab_component"]) for s in res["steps"] if s["status"] == "stub"})
    passed = sum(c["status"] == "PASS" for c in res["checks"])
    cells = next(c for c in res["checks"] if c["name"].startswith("row-level"))["detail"].split()[0]
    trades = next(s for s in res["steps"] if s["name"] == "fx_lookup")["rows"]
    secs = sum(res["timings_sec"].values())

    dag_html = ""
    for name, g in graphs.items():
        rel = g.path.relative_to(REPO_ROOT)
        dag_html += f"""
        <h3 style="margin:14px 0 8px">{esc(name)}</h3>
        <div class="cols2">
          <div class="panel"><h4><span class="pill ab">Ab Initio</span> graph parsed from <code>{esc(str(rel))}</code></h4>{abinitio_svg(g)}</div>
          <div class="panel"><h4><span class="pill py">PySpark</span> DAG as executed in this run</h4>{pyspark_svg(steps_by_graph[name])}</div>
        </div>"""

    tg = graphs["txfm_trade_enrichment"]
    lg = graphs["ld_trades_tdw"]
    m = txfm_trade_enrichment
    dml_ab, dml_py = dml_vs_schema()
    split_expr = "\n".join(re.findall(r"<expression[^>]*>.*?</expression>", lg.path.read_text()))
    code_html = "".join([
        code_pair("DML record format → Spark schema", "dml/trade_record.dml", dml_ab, "gmrd_spark/dml.py", dml_py,
                  "DML files are parsed at runtime, so the Spark schemas stay in sync with the legacy record types."),
        code_pair("reformat normalise_calypso → DataFrame.select", "txfm_trade_enrichment.mp · normalise_calypso",
                  dedent_block(tg.component("normalise_calypso").props["transform"]), src_label(m.normalise_calypso),
                  inspect.getsource(m.derive_asset_class) + "\n\n" + inspect.getsource(m.normalise_calypso)),
        code_pair("lookup fx_lookup → broadcast join", "txfm_trade_enrichment.mp · fx_lookup",
                  "lookup_key: " + tg.component("fx_lookup").props["lookup_key"] + "\n\n"
                  + dedent_block(tg.component("fx_lookup").props["transform"]), src_label(m.fx_lookup),
                  inspect.getsource(m.fx_lookup)),
        code_pair("partition_by_expression + output_table → filter + staged write",
                  "ld_trades_tdw.mp · split_by_source / load_calypso_stg",
                  dedent_block(split_expr) + "\n\n/* load_calypso_stg pre_sql */\n"
                  + dedent_block(lg.component("load_calypso_stg").props["pre_sql"]),
                  src_label(ld_trades_tdw.load_staging_table),
                  inspect.getsource(ld_trades_tdw.split_by_source) + "\n\n"
                  + inspect.getsource(ld_trades_tdw.load_staging_table)),
    ])

    fx_colors = {"GBP (1:1)": "#14868c", "lookup rate": "#8e44c9", "no rate: left unconverted": "#ef4444"}
    findings_html = "".join(
        f'<div class="finding"><h4>{esc(f["title"])}</h4><p>{esc(f["detail"])}</p>'
        f'<div class="obs">observed: {esc(f["observed"])}</div></div>' for f in res["findings"])
    inputs = ", ".join(f"{Path(k).name} ({v} rows)" for k, v in res["synthetic_inputs"].items())
    env = res["env"]

    page = f"""<!doctype html><html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>GMRD Ab Initio → PySpark migration demo</title><style>{CSS}</style></head><body>
<header>
  <h1>GMRD Ab Initio → PySpark: trade enrichment &amp; Teradata staging load</h1>
  <p>Two production graphs from this repo — <code style="background:rgba(255,255,255,.15);color:#fff">txfm_trade_enrichment.mp</code> and
  <code style="background:rgba(255,255,255,.15);color:#fff">ld_trades_tdw.mp</code> — ported to PySpark and executed end to end on
  synthetic Calypso / Murex / Summit extracts generated from the repo's DML layouts.</p>
  <div class="badges"><span class="badge warn">SYNTHETIC DATA</span><span class="badge">PySpark {esc(env['pyspark'])} · {esc(env['master'])}</span>
  <span class="badge">Python {esc(env['python'])} · Java {esc(env['java'])}</span><span class="badge">business date {esc(res['business_date'])} · batch {esc(res['batch_id'])}</span>
  <span class="badge">run {esc(res['generated_at'])}</span></div>
</header>
<main>
  <div class="kpis">
    <div class="kpi"><b>2</b><span>Ab Initio graphs ported</span></div>
    <div class="kpi"><b>{n_components - n_stub}/{n_components}</b><span>components mapped to PySpark ({n_stub} stub)</span></div>
    <div class="kpi"><b>{trades}</b><span>synthetic trades enriched &amp; staged</span></div>
    <div class="kpi good"><b>{passed}/{len(res['checks'])}</b><span>reconciliation checks passed</span></div>
    <div class="kpi good"><b>{int(cells):,}</b><span>cells identical to independent Python rules</span></div>
    <div class="kpi"><b>{secs:.1f}s</b><span>PySpark wall time (both graphs, local)</span></div>
  </div>

  <section>
    <h2>1 · Graph → DAG, side by side</h2>
    <p class="lead">Left: the legacy graph drawn straight from the <code>.mp</code> file (phases, components, flows; dashed purple = lookup port, dotted = phase break).
    Right: the PySpark steps that replace each component, with real row counts on every edge. Hover any node to highlight its counterpart.</p>
    {dag_html}
    <div class="legend">{''.join(f'<span class="chip" style="background:{f};border-color:{s};color:{s}">{g} {t}</span>' for t, (f, s, g) in TYPE_STYLE.items())}</div>
  </section>

  <section>
    <h2>2 · Component → PySpark mapping</h2>
    <p class="lead">Every component in both graphs, the construct that replaces it, where it lives, and the rows it produced in this run.</p>
    {mapping_table(res, graphs)}
  </section>

  <section>
    <h2>3 · Transform logic, side by side</h2>
    <p class="lead">Left panels are extracted verbatim from the legacy sources; right panels are the PySpark functions as committed.</p>
    {code_html}
  </section>

  <section>
    <h2>4 · Data: before → after</h2>
    <p class="lead">Raw extract rows (red dots = blank padding from fixed-width feeds) and the enriched rows PySpark wrote for them. Highlighted fields are derived or converted.</p>
    {samples_html(res)}
  </section>

  <section>
    <h2>5 · Reconciliation</h2>
    <p class="lead">Row-count and control-total checks from this run. The reference side is an independent, Spark-free Python implementation of the graph's rules
    (<code>gmrd_spark/reference_rules.py</code>) — not the Ab Initio runtime, which is not available here.</p>
    <div class="charts">
      <div class="panel"><h4>Rows per source at each stage</h4>{bar_chart_rows(res)}</div>
      <div class="panel"><h4>fx_lookup branch taken (per trade)</h4>{donut(res['fx_branch'], fx_colors)}
        <h4 style="margin-top:4px">Notional (GBP) by source &amp; asset_class</h4>{stacked_notional(res)}</div>
    </div>
    <div style="margin-top:14px">{checks_table(res)}</div>
  </section>

  <section>
    <h2>6 · Legacy behaviours surfaced by the port</h2>
    <p class="lead">The PySpark code reproduces these as-is (so outputs line up); each is a decision for the migration owner. Observations are from this run.</p>
    <div class="findings">{findings_html}</div>
  </section>

  <section>
    <h2>7 · Spark physical plan (txfm_trade_enrichment)</h2>
    <details><summary>Show <code>explain()</code> output — the lookup becomes a BroadcastHashJoin, the gather a Union</summary>
    <pre>{esc(compact_plan(res['spark_plan']))}</pre></details>
  </section>

  <section class="run">
    <h2>Run it</h2>
    <pre>cd pyspark_migration
python -m venv .venv &amp;&amp; . .venv/bin/activate &amp;&amp; pip install -r requirements.txt   # needs Java 17
python run_demo.py        # generate synthetic data, run both graphs, write work/results/run_results.json
python build_report.py    # regenerate report/abinitio_to_pyspark_demo.html
pytest -q tests           # parity + row-count tests</pre>
  </section>

  <section class="honesty">
    <h2>What this does and does not show</h2>
    <p class="lead" style="color:#78350f">All inputs are synthetic ({esc(inputs)}), generated by <code>scripts/generate_synthetic_data.py</code> from the repo's DML.
    No Ab Initio Co&gt;Operating System or Teradata was available, so no output from the legacy runtime was observed: "parity" here means the PySpark port agrees with an
    independent re-implementation of the same rules, plus row-count / control-total reconciliation. Teradata staging tables are emulated as local Parquet, and the
    <code>SP_TRADE_LIFECYCLE</code> stored-procedure phase is not migrated.</p>
  </section>
</main>
<script>{JS}</script></body></html>"""
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(page)
    return out_path


if __name__ == "__main__":
    out = build(HERE / "work" / "results" / "run_results.json", HERE / "report" / "abinitio_to_pyspark_demo.html")
    print(f"report -> {out}")
