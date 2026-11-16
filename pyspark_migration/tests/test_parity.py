"""End-to-end test: run both PySpark graphs on synthetic data and check the results."""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import run_demo  # noqa: E402


@pytest.fixture(scope="module")
def results(tmp_path_factory):
    return run_demo.main(["--work", str(tmp_path_factory.mktemp("work"))])


def test_all_reconciliation_checks_pass(results):
    failed = [c for c in results["checks"] if c["status"] != "PASS"]
    assert not failed, failed


def test_pyspark_matches_independent_reference_cell_by_cell(results):
    assert results["mismatches"] == []


def test_every_component_of_both_graphs_is_accounted_for(results):
    from gmrd_spark.dml import REPO_ROOT
    from gmrd_spark.mp_graph import parse_mp

    for name, rel in [("txfm_trade_enrichment", "mp/transform/txfm_trade_enrichment.mp"),
                      ("ld_trades_tdw", "mp/load/ld_trades_tdw.mp")]:
        legacy = {c.name for c in parse_mp(REPO_ROOT / rel).components}
        ported = {s["ab_component"] for s in results["steps"] if s["graph"] == name}
        assert legacy == ported


def test_rerun_of_same_batch_duplicates_rows_like_the_legacy_pre_sql(results):
    run1, run2 = sum(results["staged_run1"].values()), sum(results["staged_run2"].values())
    assert run2 > run1
