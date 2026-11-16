"""PySpark port of mp/transform/txfm_trade_enrichment.mp.

Ab Initio -> PySpark:
  input_file (x5)   -> spark.read.csv(sep="|", header=True, schema=<from DML>)
  reformat (x3)     -> DataFrame.select(...) per source system
  gather            -> unionByName
  lookup            -> broadcast left join on notional_ccy = base_ccy
  output_file       -> pipe-delimited file with header, trade_record_t column order
"""
from __future__ import annotations

import shutil
from pathlib import Path

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType, StringType

from .dml import DECIMAL_AMOUNT, field_names, raw_schema
from .lineage import Lineage

GRAPH = "txfm_trade_enrichment"
TRADE_REF = "dml/trade_record.dml::trade_record_t"
FX_RATE = DecimalType(18, 8)

ASSET_CLASS_RULES = [
    ("IR", ["IRS", "FRA", "SWAP", "SWAPTION", "CAP", "FLOOR"]),
    ("FX", ["FX_SPOT", "FX_FWD", "FX_SWAP", "FX_OPTION"]),
    ("EQ", ["EQUITY_SWAP", "EQUITY_OPTION", "CFD"]),
    ("CR", ["CDS", "CDO", "CLN", "TRS"]),
    ("CO", ["COMMODITY_SWAP", "COMMODITY_OPTION"]),
]


def decimal_strip(col: str) -> Column:
    """Ab Initio decimal_strip(): drop surrounding blanks / leading zeros, return a decimal."""
    return F.trim(F.col(col)).cast(DECIMAL_AMOUNT)


def string_substring(col: str, start: int, length: int) -> Column:
    """Ab Initio string_substring() is 1-based, as is Spark's substring()."""
    return F.substring(F.col(col), start, length)


def read_extract(spark: SparkSession, path: str | Path, dml_ref: str) -> DataFrame:
    return spark.read.csv(str(path), sep="|", header=True, schema=raw_schema(dml_ref))


def conform(df: DataFrame, mapping: dict[str, Column]) -> DataFrame:
    """Project onto trade_record_t: unmapped output fields stay NULL, as in a reformat."""
    cols = []
    for name in field_names(TRADE_REF):
        expr = mapping.get(name, F.lit(None).cast(StringType()))
        cols.append(expr.alias(name))
    return df.select(*cols)


def _audit(business_date: str, batch_id: str) -> dict[str, Column]:
    return {
        "business_date": F.lit(business_date),
        "batch_id": F.lit(batch_id),
        "load_timestamp": F.date_format(F.current_timestamp(), "yyyy-MM-dd HH:mm:ss"),
    }


def derive_asset_class(product_col: str) -> Column:
    expr = None
    for asset_class, products in ASSET_CLASS_RULES:
        cond = F.col(product_col).isin(products)
        expr = F.when(cond, asset_class) if expr is None else expr.when(cond, asset_class)
    return expr.otherwise("OTHER")


def normalise_calypso(df: DataFrame, business_date: str, batch_id: str) -> DataFrame:
    c = F.col
    return conform(df, {
        "trade_id": c("trade_ref"), "trade_version": c("trade_version"), "source_system": F.lit("CALYPSO"),
        "source_trade_ref": c("trade_ref"), "trade_status": c("trade_status"), "trade_type": c("trade_type"),
        "product_type": c("product_type"), "product_subtype": c("product_subtype"), "book_id": c("book_id"),
        "desk_id": c("desk_id"), "trader_id": c("trader_id"), "counterparty_id": c("counterparty_id"),
        "counterparty_name": c("counterparty_name"), "trade_date": c("trade_date"),
        "settlement_date": c("settlement_date"), "maturity_date": c("maturity_date"),
        "notional_amount": decimal_strip("notional_amount"), "notional_ccy": c("notional_ccy"),
        "settlement_amount": decimal_strip("settlement_amount"), "settlement_ccy": c("settlement_ccy"),
        "trade_price": decimal_strip("trade_price"), "market_value": decimal_strip("market_value"),
        "market_value_ccy": c("market_value_ccy"), "underlying_asset_id": c("underlying_asset"),
        "strike_price": decimal_strip("strike_price"), "option_type": c("option_type"),
        "buy_sell_ind": c("buy_sell_ind"), "long_short_ind": c("long_short_ind"),
        "asset_class": derive_asset_class("product_type"),
        "entity_code": string_substring("book_id", 1, 4),
        **_audit(business_date, batch_id),
    })


def normalise_murex(df: DataFrame, business_date: str, batch_id: str) -> DataFrame:
    c = F.col
    return conform(df, {
        "trade_id": c("mx_trade_id"), "trade_version": c("mx_version"), "source_system": F.lit("MUREX"),
        "source_trade_ref": c("mx_trade_id"), "trade_status": c("mx_status"), "trade_type": c("mx_trade_type"),
        "product_type": c("mx_product_type"), "product_subtype": c("mx_product_family"), "book_id": c("mx_book"),
        "desk_id": c("mx_desk"), "trader_id": c("mx_trader"), "counterparty_id": c("mx_cpty_code"),
        "counterparty_name": c("mx_cpty_long_name"), "trade_date": c("mx_trade_date"),
        "settlement_date": c("mx_value_date"), "maturity_date": c("mx_maturity_date"),
        "notional_amount": decimal_strip("mx_nominal"), "notional_ccy": c("mx_nominal_ccy"),
        "settlement_amount": decimal_strip("mx_settle_amount"), "settlement_ccy": c("mx_settle_ccy"),
        "trade_price": decimal_strip("mx_price"), "market_value": decimal_strip("mx_mkt_value"),
        "market_value_ccy": c("mx_mkt_value_ccy"), "underlying_asset_id": c("mx_underlying"),
        "strike_price": decimal_strip("mx_strike"), "buy_sell_ind": c("mx_buy_sell"),
        "entity_code": string_substring("mx_book", 1, 4),
        **_audit(business_date, batch_id),
    })


def normalise_summit(df: DataFrame, business_date: str, batch_id: str) -> DataFrame:
    c = F.col
    return conform(df, {
        "trade_id": c("smt_deal_no"), "trade_version": c("smt_version_no"), "source_system": F.lit("SUMMIT"),
        "source_trade_ref": c("smt_deal_no"), "trade_status": c("smt_deal_status"), "trade_type": c("smt_deal_type"),
        "product_type": c("smt_product_group"), "product_subtype": c("smt_product_class"), "book_id": c("smt_book"),
        "desk_id": c("smt_desk"), "entity_code": c("smt_entity"), "counterparty_id": c("smt_cpty_id"),
        "counterparty_name": c("smt_cpty_name"), "trade_date": c("smt_trade_date"),
        "settlement_date": c("smt_settle_date"), "maturity_date": c("smt_maturity_date"),
        "notional_amount": decimal_strip("smt_notional"), "notional_ccy": c("smt_notional_ccy"),
        "settlement_amount": decimal_strip("smt_principal"), "settlement_ccy": c("smt_principal_ccy"),
        "trade_price": decimal_strip("smt_rate"), "market_value": decimal_strip("smt_mkt_value"),
        "market_value_ccy": c("smt_mkt_value_ccy"), "underlying_asset_id": c("smt_underlying_id"),
        "strike_price": decimal_strip("smt_strike_rate"), "buy_sell_ind": c("smt_direction"),
        **_audit(business_date, batch_id),
    })


def merge_trades(*frames: DataFrame) -> DataFrame:
    out = frames[0]
    for f in frames[1:]:
        out = out.unionByName(f)
    return out


def fx_lookup(trades: DataFrame, fx_rates: DataFrame) -> DataFrame:
    """lookup on notional_ccy = base_ccy, then the graph's three-way GBP rule."""
    dupes = fx_rates.groupBy("base_ccy").count().filter("count > 1").count()
    if dupes:
        raise ValueError(f"fx lookup key base_ccy is not unique ({dupes} duplicate keys)")
    rates = fx_rates.select(
        F.col("base_ccy").alias("_lk_ccy"), F.trim("spot_rate").cast(FX_RATE).alias("_lk_rate")
    )
    j = trades.join(F.broadcast(rates), trades.notional_ccy == rates._lk_ccy, "left")
    is_gbp = F.col("notional_ccy") == "GBP"
    has_rate = F.col("_lk_rate").isNotNull()

    def to_gbp(amount: str) -> Column:
        return (
            F.when(is_gbp, F.col(amount))
            .when(has_rate, (F.col(amount) * F.col("_lk_rate")).cast(DECIMAL_AMOUNT))
            .otherwise(F.col(amount))
        )

    j = j.withColumn("notional_amount_gbp", to_gbp("notional_amount")).withColumn(
        "market_value_gbp", to_gbp("market_value")
    )
    return j.select(*field_names(TRADE_REF))


def write_delimited(df: DataFrame, target: Path) -> Path:
    """Write a single pipe-delimited file with header (Ab Initio output_file)."""
    tmp = target.with_suffix(".spark_tmp")
    shutil.rmtree(tmp, ignore_errors=True)
    df.coalesce(1).write.csv(str(tmp), sep="|", header=True, mode="overwrite", emptyValue="")
    part = next(tmp.glob("part-*.csv"))
    target.parent.mkdir(parents=True, exist_ok=True)
    shutil.move(str(part), target)
    shutil.rmtree(tmp)
    return target


def run(spark: SparkSession, *, business_date: str, batch_id: str, extract_dir: Path, lookup_dir: Path,
        output_dir: Path, lineage: Lineage) -> Path:
    t = lineage.track
    common = {"graph": GRAPH}
    calypso = t(read_extract(spark, extract_dir / f"calypso_trades_{business_date}.dat",
                             "dml/trade_record.dml::trade_calypso_raw_t"),
                name="read_calypso", ab_component="read_calypso", ab_type="input_file",
                construct="spark.read.csv(sep='|', schema=DML)", fn=read_extract, **common)
    murex = t(read_extract(spark, extract_dir / f"murex_trades_{business_date}.dat",
                           "dml/trade_record.dml::trade_murex_raw_t"),
              name="read_murex", ab_component="read_murex", ab_type="input_file",
              construct="spark.read.csv(sep='|', schema=DML)", fn=read_extract, **common)
    summit = t(read_extract(spark, extract_dir / f"summit_trades_{business_date}.dat",
                            "dml/trade_record.dml::trade_summit_raw_t"),
               name="read_summit", ab_component="read_summit", ab_type="input_file",
               construct="spark.read.csv(sep='|', schema=DML)", fn=read_extract, **common)
    fx = t(read_extract(spark, lookup_dir / f"fx_rates_{business_date}.dat", "dml/market_data_record.dml::fx_rate_t"),
           name="read_fx_rates", ab_component="read_fx_rates", ab_type="input_file",
           construct="spark.read.csv(sep='|', schema=DML)", fn=read_extract, **common)
    t(read_extract(spark, lookup_dir / "counterparty_master.dat",
                   "dml/counterparty_record.dml::counterparty_conformed_t"),
      name="read_cpty_lookup", ab_component="read_cpty_lookup", ab_type="input_file",
      construct="spark.read.csv(sep='|', schema=DML)", fn=read_extract,
      note="Read by the legacy graph but never wired to a downstream component; kept for parity.", **common)

    n_c = t(normalise_calypso(calypso, business_date, batch_id), name="normalise_calypso",
            ab_component="normalise_calypso", ab_type="reformat", construct="DataFrame.select(mapping)",
            parents=["read_calypso"], fn=normalise_calypso, **common)
    n_m = t(normalise_murex(murex, business_date, batch_id), name="normalise_murex",
            ab_component="normalise_murex", ab_type="reformat", construct="DataFrame.select(mapping)",
            parents=["read_murex"], fn=normalise_murex, **common)
    n_s = t(normalise_summit(summit, business_date, batch_id), name="normalise_summit",
            ab_component="normalise_summit", ab_type="reformat", construct="DataFrame.select(mapping)",
            parents=["read_summit"], fn=normalise_summit, **common)
    merged = t(merge_trades(n_c, n_m, n_s), name="merge_trades", ab_component="merge_trades", ab_type="gather",
               construct="unionByName", parents=["normalise_calypso", "normalise_murex", "normalise_summit"],
               fn=merge_trades, **common)
    enriched = t(fx_lookup(merged, fx), name="fx_lookup", ab_component="fx_lookup", ab_type="lookup",
                 construct="broadcast left join + when()", parents=["merge_trades", "read_fx_rates"],
                 fn=fx_lookup, **common)
    target = write_delimited(enriched, output_dir / f"trades_enriched_{business_date}.dat")
    t(enriched, name="write_enriched", ab_component="write_enriched", ab_type="output_file",
      construct="DataFrame.write.csv(sep='|')", parents=["fx_lookup"], fn=write_delimited, **common)
    return target
