"""PySpark port of the stage_load and archive phases of mp/load/ld_trades_tdw.mp.

There is no Teradata in the demo environment, so each GMRD_STG_DB staging
table is a local Parquet directory. The pre_sql DELETE + stream INSERT pair is
reproduced as "keep rows not matching the DELETE predicate, then append".
The conformed_load phase (BTEQ CALL GMRD_CORE_DB.SP_TRADE_LIFECYCLE) is a
Teradata stored procedure and is recorded as a stub, not migrated.
"""
from __future__ import annotations

import gzip
import shutil
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType, IntegerType, LongType, TimestampType

from .lineage import Lineage
from .txfm_trade_enrichment import TRADE_REF, read_extract

GRAPH = "ld_trades_tdw"
SOURCES = {"CALYPSO": "STG_TRADES_CALYPSO", "MUREX": "STG_TRADES_MUREX", "SUMMIT": "STG_TRADES_SUMMIT"}


def read_enriched_trades(spark: SparkSession, path: Path) -> DataFrame:
    return read_extract(spark, path, TRADE_REF)


def split_by_source(df: DataFrame) -> dict[str, DataFrame]:
    """partition_by_expression: one output flow per source_system expression."""
    return {src: df.filter(F.col("source_system") == src) for src in SOURCES}


def staging_columns(df: DataFrame, batch_id: str) -> DataFrame:
    """column_mapping of load_calypso_stg (TPT stream insert)."""
    c = F.col
    has_feed_id = "feed_id" in df.columns  # trade_record_t has no feed_id field
    return df.select(
        (c("feed_id").cast(LongType()) if has_feed_id else F.lit(None).cast(LongType())).alias("FEED_ID"),
        c("load_timestamp").cast(TimestampType()).alias("FEED_TIMESTAMP"),
        F.lit(int(batch_id)).cast(LongType()).alias("BATCH_ID"),
        c("trade_id").alias("TRADE_REF"),
        c("trade_version").cast(IntegerType()).alias("TRADE_VERSION"),
        c("trade_status").alias("TRADE_STATUS"),
        c("trade_type").alias("TRADE_TYPE"),
        c("product_type").alias("PRODUCT_TYPE"),
        c("product_subtype").alias("PRODUCT_SUBTYPE"),
        c("book_id").alias("BOOK_ID"),
        c("desk_id").alias("DESK_ID"),
        c("trader_id").alias("TRADER_ID"),
        c("counterparty_id").alias("COUNTERPARTY_ID"),
        c("counterparty_name").alias("COUNTERPARTY_NAME"),
        F.to_date("trade_date", "yyyy-MM-dd").alias("TRADE_DATE"),
        F.to_date("settlement_date", "yyyy-MM-dd").alias("SETTLEMENT_DATE"),
        F.to_date("maturity_date", "yyyy-MM-dd").alias("MATURITY_DATE"),
        c("notional_amount").cast(DecimalType(25, 6)).alias("NOTIONAL_AMOUNT"),
        c("notional_ccy").alias("NOTIONAL_CCY"),
        c("settlement_amount").cast(DecimalType(25, 6)).alias("SETTLEMENT_AMOUNT"),
        c("settlement_ccy").alias("SETTLEMENT_CCY"),
        c("trade_price").cast(DecimalType(18, 8)).alias("TRADE_PRICE"),
        c("market_value").cast(DecimalType(25, 6)).alias("MARKET_VALUE"),
        c("market_value_ccy").alias("MARKET_VALUE_CCY"),
        c("underlying_asset_id").alias("UNDERLYING_ASSET"),
        c("strike_price").cast(DecimalType(18, 8)).alias("STRIKE_PRICE"),
        c("option_type").alias("OPTION_TYPE"),
        c("buy_sell_ind").alias("BUY_SELL_IND"),
        c("long_short_ind").alias("LONG_SHORT_IND"),
        c("source_system").alias("SOURCE_SYSTEM"),
    )


def load_staging_table(spark: SparkSession, df: DataFrame, table_dir: Path, business_date: str,
                       batch_id: str) -> int:
    """pre_sql DELETE ... WHERE TRADE_DATE = :business_date AND BATCH_ID = :batch_id, then INSERT."""
    new_rows = staging_columns(df, batch_id)
    if table_dir.exists():
        deleted = (F.col("TRADE_DATE") == F.to_date(F.lit(business_date))) & (F.col("BATCH_ID") == int(batch_id))
        kept = spark.read.parquet(str(table_dir)).filter(~deleted)
        combined = kept.unionByName(new_rows)
    else:
        combined = new_rows
    tmp = table_dir.with_name(table_dir.name + ".tmp")
    shutil.rmtree(tmp, ignore_errors=True)
    combined.write.parquet(str(tmp), mode="overwrite")
    shutil.rmtree(table_dir, ignore_errors=True)
    tmp.rename(table_dir)
    spark.catalog.refreshByPath(str(table_dir))
    return spark.read.parquet(str(table_dir)).count()


def archive_file(src: Path, archive_dir: Path) -> Path:
    archive_dir.mkdir(parents=True, exist_ok=True)
    target = archive_dir / (src.name + ".gz")
    with src.open("rb") as fin, gzip.open(target, "wb") as fout:
        shutil.copyfileobj(fin, fout)
    return target


def run(spark: SparkSession, *, business_date: str, batch_id: str, input_dir: Path, staging_root: Path,
        archive_dir: Path, lineage: Lineage) -> dict:
    t = lineage.track
    common = {"graph": GRAPH}
    src_file = input_dir / f"trades_enriched_{business_date}.dat"
    enriched = t(read_enriched_trades(spark, src_file), name="read_enriched_trades",
                 ab_component="read_enriched_trades", ab_type="input_file",
                 construct="spark.read.csv(sep='|', schema=DML)", fn=read_enriched_trades, **common)
    parts = split_by_source(enriched)
    staged = {}
    for src, table in SOURCES.items():
        comp = f"load_{src.lower()}_stg"
        part = t(parts[src], name=f"split_by_source:{src}", ab_component="split_by_source",
                 ab_type="partition_by_expression", construct=f"filter(source_system == '{src}')",
                 parents=["read_enriched_trades"], fn=split_by_source, **common)
        table_dir = staging_root / "GMRD_STG_DB" / table
        staged[table] = load_staging_table(spark, part, table_dir, business_date, batch_id)
        t(spark.read.parquet(str(table_dir)), name=comp, ab_component=comp, ab_type="output_table",
          construct="pre_sql DELETE -> filter; INSERT -> union + write.parquet",
          parents=[f"split_by_source:{src}"], fn=load_staging_table,
          note=f"GMRD_STG_DB.{table} emulated as Parquet (no Teradata in demo env).", **common)
    lineage.track(None, graph=GRAPH, name="call_trade_lifecycle", ab_component="call_trade_lifecycle",
                  ab_type="run_program", construct="(not migrated) BTEQ CALL SP_TRADE_LIFECYCLE",
                  parents=[f"load_{s.lower()}_stg" for s in SOURCES], status="stub",
                  note="Teradata stored procedure; stays in the warehouse or is ported separately.")
    gz = archive_file(src_file, archive_dir)
    lineage.track(None, graph=GRAPH, name="archive_file", ab_component="archive_file", ab_type="run_program",
                  construct="gzip.open + shutil.copyfileobj", parents=["call_trade_lifecycle"], fn=archive_file)
    return {"staged": staged, "archive": gz}
