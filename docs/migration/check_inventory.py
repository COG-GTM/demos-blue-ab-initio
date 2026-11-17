#!/usr/bin/env python3
"""Check migration Markdown against source files without executing legacy code."""

import argparse
import copy
from pathlib import Path
import re
import sys
import unittest


ROOT = Path(__file__).resolve().parents[2]
DOC_DIR = "docs/migration/"
DOC_NAMES = ("inventory.md", "dag.md", "missing-assets-register.md", "waves.md")
GRAPH = re.compile(r"mp/[A-Za-z0-9_/-]+\.mp\b")
PATH = re.compile(
    r"(?<![\w/])(?:"
    r"(?:docs|mp|dml|xfr|scripts|sql|pset|conf)/[A-Za-z0-9_./-]+"
    r"\.(?:md|mp|dml|xfr|ksh|sql|pset|cfg|py)"
    r"|README\.md|Makefile|azure-pipelines\.yml|\.gitignore)"
    r"(?::([0-9]+))?"
)


def load(root):
    paths = {p.relative_to(root).as_posix() for p in root.rglob("*")
             if ".git" not in p.relative_to(root).parts and p.is_file()}
    # Do not load pset/conf values, execute shells, or invoke external commands.
    readable = {p for p in paths if p.startswith((DOC_DIR, "scripts/", "mp/"))
                and p.endswith((".md", ".ksh", ".mp"))}
    for name in ("README.md", "Makefile", "azure-pipelines.yml", ".gitignore"):
        if name in paths:
            readable.add(name)
    readable.update(p for p in paths if p.startswith(("dml/", "xfr/")))
    return paths, {p: (root / p).read_text() for p in readable}


def invocations(script):
    executable = "\n".join(line for line in script.splitlines()
                           if not line.lstrip().startswith("#"))
    return GRAPH.findall(executable)


def daily_edges(script):
    """Reconstruct run_graph groups separated by shell waits, including & jobs."""
    previous = set()
    background = set()
    edges = set()
    for line in script.splitlines():
        if line.lstrip().startswith("wait ") and background:
            previous = background
            background = set()
        match = re.match(r'\s*run_graph\s+"[^"]+"\s+"([^"]+\.mp)"(.*)', line)
        if not match:
            continue
        node = Path(match[1]).stem
        edges.update((parent, node) for parent in previous)
        if match[2].rstrip().endswith("&"):
            background.add(node)
        else:
            previous = {node}
    return edges


def mermaid(section):
    blocks = re.findall(r"```mermaid\n(.*?)```", section, re.S)
    if len(blocks) != 1:
        return set(), set()
    edges = set(re.findall(r"^\s*(\w+)\s*-->\s*(\w+)", blocks[0], re.M))
    nodes = {node for edge in edges for node in edge}
    nodes.update(re.findall(r"^\s*(\w+)(?:\[.*\])?\s*$", blocks[0], re.M))
    return nodes, edges


def check(paths, content):
    errors = []
    docs = {name: content.get(DOC_DIR + name, "") for name in DOC_NAMES}
    for name, body in docs.items():
        if not body:
            errors.append(f"missing or empty document: {name}")
    inventory = docs["inventory.md"]
    dag = docs["dag.md"]
    register = docs["missing-assets-register.md"]
    absent = set(re.findall(r"^\| `([^`]+\.mp)` \|", register, re.M))
    graphs = {p for p in paths if p.startswith("mp/") and p.endswith(".mp")}
    extracts = {p for p in graphs if p.startswith("mp/extract/")}
    scripts = {p: content.get(p, "") for p in sorted(paths)
               if p.startswith("scripts/") and p.endswith(".ksh")}
    invoked = {g for script in scripts.values() for g in invocations(script)}
    missing_extracts = {g for g in invoked if g.startswith("mp/extract/")
                        and g not in paths}
    if not graphs:
        errors.append("no checked-in graphs discovered")
    if not invoked:
        errors.append("no script graph invocations discovered")
    for name, body in docs.items():
        for label, expected in (
            ("Checked-in graph count", len(graphs)),
            ("Script-referenced absent extract graph count", len(missing_extracts)),
        ):
            counts = re.findall(re.escape(label) + r": (\d+)", body)
            if not counts or any(int(n) != expected for n in counts):
                errors.append(f"{name}: {label} must be {expected}")
        for count in re.findall(r"(?<!absent )Extract graph count: (\d+)", body):
            if int(count) != len(extracts):
                errors.append(f"{name}: Extract graph count must be {len(extracts)}")
        for match in PATH.finditer(body):
            ref = match[0]
            path = ref.rsplit(":", 1)[0] if match[1] else ref
            if path not in paths and path not in absent:
                errors.append(f"{name}: nonexistent cited path: {path}")
            if match[1] and path in content:
                if not 1 <= int(match[1]) <= len(content[path].splitlines()):
                    errors.append(f"{name}: invalid line citation: {ref}")
    for graph in sorted(graphs | invoked):
        for name in ("inventory.md", "dag.md"):
            if graph not in set(GRAPH.findall(docs[name])):
                errors.append(f"{name}: graph not covered: {graph}")
    for graph in sorted(missing_extracts - absent):
        errors.append(f"absent extract not registered: {graph}")
    for graph in sorted(absent & paths):
        errors.append(f"registered absent graph now exists: {graph}")
    for graph in sorted(graphs):
        source = content.get(graph, "")
        section = re.search(r"^### `" + re.escape(graph)
                            + r"`\n(.*?)(?=^### |^## |\Z)", inventory, re.M | re.S)
        for component in re.findall(r'<component name="([^"]+)"', source):
            if not section or f"| `{component}` (" not in section[1]:
                errors.append(f"inventory.md: component not covered: {graph}::{component}")
    for title, script_name in (("Daily", "run_daily_batch.ksh"),
                               ("Intraday", "run_intraday_refresh.ksh")):
        section = re.search(r"^## " + title + r"\n(.*?)(?=^## |\Z)", dag, re.M | re.S)
        script = scripts.get("scripts/" + script_name, "")
        if not script or not section:
            errors.append(f"missing {title} script or DAG section")
            continue
        sequence = invocations(script)
        if not sequence:
            errors.append(f"{title} script has no graph invocations")
            continue
        expected = {Path(p).stem for p in sequence}
        nodes, edges = mermaid(section[1])
        if title == "Daily":
            expected_edges = daily_edges(script)
            if "${SCRIPT_DIR}/post_load_validation.ksh" in script:
                expected.add("post_load_validation")
                expected_edges.add((Path(sequence[-1]).stem, "post_load_validation"))
        else:
            expected_edges = {(Path(a).stem, Path(b).stem)
                              for a, b in zip(sequence, sequence[1:])}
        if nodes != expected:
            errors.append(f"{title} DAG node mismatch: missing={sorted(expected - nodes)}, "
                          f"extra={sorted(nodes - expected)}")
        if edges != expected_edges:
            errors.append(f"{title} DAG edge mismatch: missing={sorted(expected_edges - edges)}, "
                          f"extra={sorted(edges - expected_edges)}")
        if set(GRAPH.findall(section[1])) != set(sequence):
            errors.append(f"{title} DAG table graph paths differ from script invocations")
    return sorted(set(errors))


class CheckerTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.paths, cls.content = load(ROOT)

    def test_normal(self):
        self.assertEqual(check(self.paths, self.content), [])

    def test_empty(self):
        self.assertTrue(check(set(), {}))

    def mutated(self, name, old, new):
        content = copy.copy(self.content)
        content[DOC_DIR + name] = content[DOC_DIR + name].replace(old, new)
        return check(self.paths, content)

    def test_bad_path(self):
        self.assertTrue(self.mutated("waves.md", "README.md:49", "docs/not_present.md:1"))

    def test_bad_count(self):
        self.assertTrue(self.mutated("inventory.md", "graph count: 8", "graph count: 999"))

    def test_unregistered_extract(self):
        self.assertTrue(self.mutated("missing-assets-register.md",
                                    "mp/extract/ext_counterparty.mp", "unlisted.mp"))

    def test_extra_dag_node(self):
        self.assertTrue(self.mutated("dag.md", "flowchart TD", "flowchart TD\n  bogus --> ext_market_data"))

    def test_standalone_dag_node(self):
        self.assertTrue(self.mutated("dag.md", "flowchart TD", "flowchart TD\n  bogus[uninvoked]"))

    def test_script_without_graphs(self):
        content = copy.copy(self.content)
        content["scripts/run_daily_batch.ksh"] = "#!/bin/ksh\n"
        self.assertTrue(check(self.paths, content))

    def test_wrong_edge(self):
        self.assertTrue(self.mutated("dag.md", "ext_positions_multi --> ext_market_data",
                                    "ext_market_data --> ext_positions_multi"))

    def test_missing_component(self):
        self.assertTrue(self.mutated("inventory.md", "| `read_calypso` (", "| `omitted` ("))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--self-test", action="store_true")
    args = parser.parse_args()
    if args.self_test:
        suite = unittest.defaultTestLoader.loadTestsFromTestCase(CheckerTests)
        return 0 if unittest.TextTestRunner(verbosity=2).run(suite).wasSuccessful() else 1
    paths, content = load(ROOT)
    errors = check(paths, content)
    if errors:
        for error in errors:
            print(f"FAIL: {error}")
        return 1
    graphs = {p for p in paths if p.startswith("mp/") and p.endswith(".mp")}
    invoked = {g for p, s in content.items() if p.startswith("scripts/") for g in invocations(s)}
    print(f"PASS: {len(graphs)} checked-in graphs; {len(invoked)} distinct script-invoked graphs; "
          f"{len(invoked - graphs)} absent extract graphs registered")
    print("PASS: cited paths/available line ranges, component coverage, graph counts, "
          "daily/intraday DAG nodes, edges and graph tables")
    print("PASS: read-only stdlib check; no legacy jobs or configuration sourcing")
    return 0


if __name__ == "__main__":
    sys.exit(main())
