"""Lightweight reader for the GMRD ``.mp`` graph files.

The files are XML-like but repeat the ``port`` attribute on ``<input>``
elements, so they are parsed with regular expressions instead of an XML parser.
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from pathlib import Path

_PHASE_RE = re.compile(r'<phase\s+name="([^"]+)">(.*?)</phase>', re.S)
_COMP_RE = re.compile(r'<component\s+name="([^"]+)"\s+type="([^"]+)">(.*?)</component>', re.S)
_INPUT_RE = re.compile(r'<input\s+port="([^"]+)"\s+component="([^"]+)"')
_PROP_RE = re.compile(r'<property\s+name="([^"]+)">(.*?)</property>', re.S)
_CDATA_RE = re.compile(r"<!\[CDATA\[(.*?)\]\]>", re.S)


@dataclass
class Component:
    name: str
    type: str
    phase: str
    inputs: list[tuple[str, str]] = field(default_factory=list)  # (port, upstream component)
    props: dict[str, str] = field(default_factory=dict)


@dataclass
class Graph:
    name: str
    path: Path
    phases: list[str]
    components: list[Component]

    def component(self, name: str) -> Component:
        return next(c for c in self.components if c.name == name)

    @property
    def edges(self) -> list[tuple[str, str, str]]:
        return [(up, c.name, port) for c in self.components for port, up in c.inputs]


def _clean(value: str) -> str:
    m = _CDATA_RE.search(value)
    return (m.group(1) if m else value).strip("\n")


def parse_mp(path: str | Path) -> Graph:
    path = Path(path)
    text = path.read_text()
    name = re.search(r"<name>([^<]+)</name>", text).group(1)
    phases, comps = [], []
    for phase, body in _PHASE_RE.findall(text):
        phases.append(phase)
        for cname, ctype, cbody in _COMP_RE.findall(body):
            comps.append(
                Component(
                    name=cname,
                    type=ctype,
                    phase=phase,
                    inputs=[(p, up) for p, up in _INPUT_RE.findall(cbody)],
                    props={k: _clean(v) for k, v in _PROP_RE.findall(cbody)},
                )
            )
    return Graph(name=name, path=path, phases=phases, components=comps)
