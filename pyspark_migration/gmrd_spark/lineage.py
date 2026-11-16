"""Records each PySpark step against the Ab Initio component it replaces.

The recorded steps drive the PySpark DAG and the component mapping in the
HTML report, and their row counts come from real ``count()`` calls.
"""
from __future__ import annotations

import inspect
import time
from dataclasses import dataclass, field
from typing import Callable

from pyspark.sql import DataFrame


@dataclass
class Step:
    graph: str
    name: str
    ab_component: str | None
    ab_type: str | None
    construct: str
    parents: list[str] = field(default_factory=list)
    fn: Callable | None = None
    status: str = "migrated"  # migrated | stub
    note: str = ""
    df: DataFrame | None = None
    rows: int | None = None
    seconds: float | None = None

    @property
    def source_ref(self) -> str | None:
        if self.fn is None:
            return None
        lines, start = inspect.getsourcelines(self.fn)
        path = inspect.getsourcefile(self.fn).split("pyspark_migration/")[-1]
        return f"{path}:{start}"


class Lineage:
    def __init__(self) -> None:
        self.steps: list[Step] = []

    def track(self, df: DataFrame | None, **kw) -> DataFrame | None:
        step = Step(df=df.cache() if df is not None else None, **kw)
        if step.df is not None:
            t0 = time.perf_counter()
            step.rows = step.df.count()
            step.seconds = round(time.perf_counter() - t0, 3)
        self.steps.append(step)
        return step.df

    def as_records(self) -> list[dict]:
        return [
            {
                "graph": s.graph, "name": s.name, "ab_component": s.ab_component, "ab_type": s.ab_type,
                "construct": s.construct, "parents": s.parents, "status": s.status, "note": s.note,
                "rows": s.rows, "seconds": s.seconds, "source_ref": s.source_ref,
                "fn": s.fn.__name__ if s.fn else None,
            }
            for s in self.steps
        ]
