"""Run the Ab Initio -> PySpark migration demo end to end and write results JSON.

Steps:
  1. generate SYNTHETIC source extracts from the DML layouts
  2. run the PySpark port of txfm_trade_enrichment.mp
  3. run the PySpark port of ld_trades_tdw.mp (twice, to observe rerun behaviour)
  4. cross-check against an independent pure-Python implementation of the rules
  5. write work/results/run_results.json (consumed by build_report.py)
"""
from __future__ import annotations

import argparse
import csv
import gzip
import json
import platform
import shutil
import sys
import time
from collections import Counter, defaultdict
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

import pyspark  # noqa: E402
from pyspark.sql import SparkSession  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

from gmrd_spark import ld_trades_tdw, reference_rules, txfm_trade_enrichment  # noqa: E402
from gmrd_spark.lineage import Lineage  # noqa: E402
from scripts.generate_synthetic_data import generate  # noqa: E402

SAMPLE_IDS = {"CALYPSO": 2, "MUREX": 1, "SUMMIT": 1}


def spark_session() -> SparkSession:
    return (
        SparkSession.builder.master("local[4]").appName("gmrd-abinitio-to-pyspark-demo")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )


def explain_uncached(spark: SparkSession, data: Path, bd: str, batch: str) -> str:
    """Physical plan of the enrichment flow built without the demo's per-step caching."""
    m = txfm_trade_enrichment
    ext, lk = data / "extract", data / "lookup"
    df = m.fx_lookup(
        m.merge_trades(
            m.normalise_calypso(m.read_extract(spark, ext / f"calypso_trades_{bd}.dat",
                                               "dml/trade_record.dml::trade_calypso_raw_t"), bd, batch),
            m.normalise_murex(m.read_extract(spark, ext / f"murex_trades_{bd}.dat",
                                             "dml/trade_record.dml::trade_murex_raw_t"), bd, batch),
            m.normalise_summit(m.read_extract(spark, ext / f"summit_trades_{bd}.dat",
                                              "dml/trade_record.dml::trade_summit_raw_t"), bd, batch),
        ),
        m.read_extract(spark, lk / f"fx_rates_{bd}.dat", "dml/market_data_record.dml::fx_rate_t"),
    )
    return spark.sparkContext._jvm.PythonSQLUtils.explainString(df._jdf.queryExecution(), "simple")


def read_dat(path: Path) -> list[dict]:
    with path.open() as fh:
        return list(csv.DictReader(fh, delimiter="|"))


def check(name, expected, actual, detail="") -> dict:
    return {"name": name, "expected": expected, "actual": actual,
            "status": "PASS" if expected == actual else "FAIL", "detail": detail}


def main(argv=None) -> dict:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--work", default=str(HERE / "work"))
    ap.add_argument("--business-date", default="2026-11-13")
    ap.add_argument("--batch-id", default="20261113001")
    args = ap.parse_args(argv)

    work = Path(args.work)
    bd, batch = args.business_date, args.batch_id
    for sub in ("data", "output", "teradata_stub", "archive"):
        shutil.rmtree(work / sub, ignore_errors=True)
    data = work / "data"
    started = datetime.now(timezone.utc)
    generated = generate(data, bd)

    spark = spark_session()
    spark.sparkContext.setLogLevel("ERROR")
    lineage = Lineage()

    t0 = time.perf_counter()
    enriched_path = txfm_trade_enrichment.run(
        spark, business_date=bd, batch_id=batch, extract_dir=data / "extract", lookup_dir=data / "lookup",
        output_dir=work / "output", lineage=lineage)
    t_txfm = time.perf_counter() - t0

    t0 = time.perf_counter()
    load1 = ld_trades_tdw.run(spark, business_date=bd, batch_id=batch, input_dir=work / "output",
                              staging_root=work / "teradata_stub", archive_dir=work / "archive", lineage=lineage)
    t_load = time.perf_counter() - t0
    load2 = ld_trades_tdw.run(spark, business_date=bd, batch_id=batch, input_dir=work / "output",
                              staging_root=work / "teradata_stub", archive_dir=work / "archive", lineage=Lineage())

    steps = {s.name: s for s in lineage.steps}
    spark.catalog.clearCache()
    plan = explain_uncached(spark, data, bd, batch)

    # ---- independent cross-check ----------------------------------------------------------
    spark_rows = read_dat(enriched_path)
    ref_rows = reference_rules.enrich(data / "extract", data / "lookup", bd, batch)
    columns = [c for c in spark_rows[0].keys() if c != "load_timestamp"]
    spark_by_key = {(r["source_system"], r["trade_id"]): r for r in spark_rows}
    mismatches, compared = [], 0
    for ref in ref_rows:
        key = (ref["source_system"], ref["trade_id"])
        got = spark_by_key.get(key)
        for col in columns:
            compared += 1
            exp = reference_rules.render(ref.get(col))
            act = "" if got is None else got[col]
            if exp != act:
                mismatches.append({"key": "/".join(key), "column": col, "reference": exp, "pyspark": act})

    def total(rows, col):
        return sum((Decimal(r[col]) for r in rows if r[col] not in ("", None)), Decimal(0))

    ref_total_n = sum((r["notional_amount_gbp"] or Decimal(0)) for r in ref_rows)
    ref_total_mv = sum((r["market_value_gbp"] or Decimal(0)) for r in ref_rows)

    raw_counts = {s: steps[f"read_{s.lower()}"].rows for s in ("CALYPSO", "MUREX", "SUMMIT")}
    norm_counts = {s: steps[f"normalise_{s.lower()}"].rows for s in raw_counts}
    split_counts = {s: steps[f"split_by_source:{s}"].rows for s in raw_counts}
    with gzip.open(load1["archive"], "rt") as fh:
        archived_lines = sum(1 for _ in fh) - 1

    checks = [
        *[check(f"{s}: extract rows = normalised rows", raw_counts[s], norm_counts[s]) for s in raw_counts],
        check("gather: rows out = sum of sources", sum(norm_counts.values()), steps["merge_trades"].rows),
        check("fx lookup: no row fan-out (left join)", steps["merge_trades"].rows, steps["fx_lookup"].rows),
        check("enriched file rows = fx_lookup rows", steps["fx_lookup"].rows, len(spark_rows)),
        check("row-level diff vs independent Python rules (cells)", 0, len(mismatches),
              f"{compared} cells compared across {len(ref_rows)} trades x {len(columns)} columns"),
        check("control total notional_amount_gbp (PySpark vs reference)", format(ref_total_n, "f"),
              format(total(spark_rows, "notional_amount_gbp"), "f")),
        check("control total market_value_gbp (PySpark vs reference)", format(ref_total_mv, "f"),
              format(total(spark_rows, "market_value_gbp"), "f")),
        *[check(f"{t}: staged rows = split rows", split_counts[s], load1["staged"][t])
          for s, t in ld_trades_tdw.SOURCES.items()],
        check("archive: gzip line count = enriched rows", len(spark_rows), archived_lines),
    ]

    # ---- findings observed in this run (legacy behaviour preserved as-is) -------------------
    fx_ccys = {r["base_ccy"] for r in read_dat(data / "lookup" / f"fx_rates_{bd}.dat")}
    branch = Counter(
        "GBP (1:1)" if r["notional_ccy"] == "GBP" else ("lookup rate" if r["notional_ccy"] in fx_ccys
                                                         else "no rate: left unconverted")
        for r in spark_rows)
    null_ac = Counter(r["source_system"] for r in spark_rows if r["asset_class"] == "")
    mv_ccy_diff = sum(1 for r in spark_rows if r["market_value_ccy"] != r["notional_ccy"])
    td_eq_bd = sum(1 for r in spark_rows if r["trade_date"] == bd)
    staged_feed_null = spark.read.parquet(
        str(work / "teradata_stub" / "GMRD_STG_DB" / "STG_TRADES_CALYPSO")).filter(F.col("FEED_ID").isNull()).count()
    findings = [
        {"title": "asset_class only derived for Calypso",
         "detail": "normalise_murex and normalise_summit never assign out.asset_class, so it is NULL for those feeds.",
         "observed": f"{sum(null_ac.values())} enriched rows with empty asset_class "
                     f"({', '.join(f'{k} {v}' for k, v in sorted(null_ac.items()))})"},
        {"title": "Currencies without an FX rate pass through unconverted",
         "detail": "fx_lookup's else-branch copies the local amount into *_gbp when no rate is found.",
         "observed": f"{branch.get('no rate: left unconverted', 0)} rows took the no-rate branch "
                     f"(synthetic SGD trades)"},
        {"title": "pre_sql DELETE is not rerun-safe",
         "detail": "The staging DELETE filters on TRADE_DATE = business date, but most trades have earlier "
                   "trade dates, so a rerun of the same batch re-inserts them.",
         "observed": f"staged rows after run 1: {sum(load1['staged'].values())}; after rerun of the same "
                     f"batch: {sum(load2['staged'].values())} ({td_eq_bd} of {len(spark_rows)} trades have "
                     f"trade_date = {bd})"},
        {"title": "FEED_ID mapped from a field that does not exist",
         "detail": "load_calypso_stg maps FEED_ID = CAST(feed_id AS BIGINT) but trade_record_t has no feed_id.",
         "observed": f"{staged_feed_null} STG_TRADES_CALYPSO rows with NULL FEED_ID (all rows in the table after the same-batch rerun)"},
        {"title": "Counterparty lookup is read but unused",
         "detail": "read_cpty_lookup has no downstream flow in txfm_trade_enrichment.mp.",
         "observed": f"{steps['read_cpty_lookup'].rows} rows read, 0 consumed"},
        {"title": "market_value converted at the notional currency rate",
         "detail": "The lookup key is notional_ccy, so market_value_gbp uses that rate even if "
                   "market_value_ccy differs.",
         "observed": f"{mv_ccy_diff} rows where market_value_ccy differs from notional_ccy "
                     f"(the synthetic sample does not exercise this case)" if mv_ccy_diff == 0 else
                     f"{mv_ccy_diff} rows where market_value_ccy differs from notional_ccy"},
    ]

    # ---- samples & aggregates for the visuals -----------------------------------------------
    raw_files = {"CALYPSO": "calypso", "MUREX": "murex", "SUMMIT": "summit"}
    raw_rows = {s: read_dat(data / "extract" / f"{p}_trades_{bd}.dat") for s, p in raw_files.items()}
    id_col = {"CALYPSO": "trade_ref", "MUREX": "mx_trade_id", "SUMMIT": "smt_deal_no"}
    samples = []
    picks = {"CALYPSO": [0, 1], "MUREX": [4], "SUMMIT": [6]}
    for s, idxs in picks.items():
        for i in idxs:
            raw = raw_rows[s][i]
            samples.append({"source": s, "raw": raw, "enriched": spark_by_key[(s, raw[id_col[s]])]})

    by_src_ac = defaultdict(Decimal)
    for r in spark_rows:
        by_src_ac[(r["source_system"], r["asset_class"] or "NULL")] += Decimal(r["notional_amount_gbp"])

    results = {
        "generated_at": started.isoformat(timespec="seconds"),
        "business_date": bd, "batch_id": batch,
        "env": {"pyspark": pyspark.__version__, "python": platform.python_version(),
                "java": spark.sparkContext._jvm.System.getProperty("java.version"), "master": "local[4]"},
        "synthetic_inputs": generated,
        "timings_sec": {"txfm_trade_enrichment": round(t_txfm, 2), "ld_trades_tdw": round(t_load, 2)},
        "steps": lineage.as_records(),
        "checks": checks,
        "mismatches": mismatches[:50],
        "findings": findings,
        "fx_branch": dict(branch),
        "notional_gbp_by_source_asset_class": [
            {"source": s, "asset_class": a, "notional_gbp": format(v, "f")} for (s, a), v in sorted(by_src_ac.items())],
        "staged_run1": load1["staged"], "staged_run2": load2["staged"],
        "samples": samples,
        "spark_plan": plan,
        "enriched_columns": list(spark_rows[0].keys()),
    }
    out = work / "results" / "run_results.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(results, indent=2, default=str))
    spark.stop()

    passed = sum(c["status"] == "PASS" for c in checks)
    print(f"txfm_trade_enrichment: {steps['fx_lookup'].rows} enriched rows in {t_txfm:.1f}s")
    print(f"ld_trades_tdw: staged {load1['staged']} in {t_load:.1f}s")
    for c in checks:
        print(f"  [{c['status']}] {c['name']}: expected={c['expected']} actual={c['actual']}")
    print(f"checks passed: {passed}/{len(checks)}")
    print(f"results -> {out}")
    return results


if __name__ == "__main__":
    res = main()
    sys.exit(0 if all(c["status"] == "PASS" for c in res["checks"]) else 1)
