"""Independent, Spark-free re-implementation of the txfm_trade_enrichment rules.

Written directly from the transform blocks in the .mp file using only the
Python standard library. It is NOT the Ab Initio runtime (none is available);
it is a second implementation used to cross-check the PySpark output.
"""
from __future__ import annotations

import csv
from decimal import ROUND_HALF_UP, Decimal
from pathlib import Path

DECIMAL_DP = Decimal("0.00000001")
ASSET_CLASS = {
    **dict.fromkeys(["IRS", "FRA", "SWAP", "SWAPTION", "CAP", "FLOOR"], "IR"),
    **dict.fromkeys(["FX_SPOT", "FX_FWD", "FX_SWAP", "FX_OPTION"], "FX"),
    **dict.fromkeys(["EQUITY_SWAP", "EQUITY_OPTION", "CFD"], "EQ"),
    **dict.fromkeys(["CDS", "CDO", "CLN", "TRS"], "CR"),
    **dict.fromkeys(["COMMODITY_SWAP", "COMMODITY_OPTION"], "CO"),
}


def decimal_strip(value: str | None) -> Decimal | None:
    value = (value or "").strip()
    return Decimal(value).quantize(DECIMAL_DP, ROUND_HALF_UP) if value else None


def _read(path: Path) -> list[dict]:
    with path.open() as fh:
        return list(csv.DictReader(fh, delimiter="|"))


def _common(bd: str, batch: str) -> dict:
    return {"business_date": bd, "batch_id": batch}


def _calypso(r: dict, bd: str, batch: str) -> dict:
    return {
        "trade_id": r["trade_ref"], "trade_version": r["trade_version"], "source_system": "CALYPSO",
        "source_trade_ref": r["trade_ref"], "trade_status": r["trade_status"], "trade_type": r["trade_type"],
        "product_type": r["product_type"], "product_subtype": r["product_subtype"], "book_id": r["book_id"],
        "desk_id": r["desk_id"], "trader_id": r["trader_id"], "counterparty_id": r["counterparty_id"],
        "counterparty_name": r["counterparty_name"], "trade_date": r["trade_date"],
        "settlement_date": r["settlement_date"], "maturity_date": r["maturity_date"],
        "notional_amount": decimal_strip(r["notional_amount"]), "notional_ccy": r["notional_ccy"],
        "settlement_amount": decimal_strip(r["settlement_amount"]), "settlement_ccy": r["settlement_ccy"],
        "trade_price": decimal_strip(r["trade_price"]), "market_value": decimal_strip(r["market_value"]),
        "market_value_ccy": r["market_value_ccy"], "underlying_asset_id": r["underlying_asset"],
        "strike_price": decimal_strip(r["strike_price"]), "option_type": r["option_type"],
        "buy_sell_ind": r["buy_sell_ind"], "long_short_ind": r["long_short_ind"],
        "asset_class": ASSET_CLASS.get(r["product_type"], "OTHER"), "entity_code": r["book_id"][0:4],
        **_common(bd, batch),
    }


def _murex(r: dict, bd: str, batch: str) -> dict:
    return {
        "trade_id": r["mx_trade_id"], "trade_version": r["mx_version"], "source_system": "MUREX",
        "source_trade_ref": r["mx_trade_id"], "trade_status": r["mx_status"], "trade_type": r["mx_trade_type"],
        "product_type": r["mx_product_type"], "product_subtype": r["mx_product_family"], "book_id": r["mx_book"],
        "desk_id": r["mx_desk"], "trader_id": r["mx_trader"], "counterparty_id": r["mx_cpty_code"],
        "counterparty_name": r["mx_cpty_long_name"], "trade_date": r["mx_trade_date"],
        "settlement_date": r["mx_value_date"], "maturity_date": r["mx_maturity_date"],
        "notional_amount": decimal_strip(r["mx_nominal"]), "notional_ccy": r["mx_nominal_ccy"],
        "settlement_amount": decimal_strip(r["mx_settle_amount"]), "settlement_ccy": r["mx_settle_ccy"],
        "trade_price": decimal_strip(r["mx_price"]), "market_value": decimal_strip(r["mx_mkt_value"]),
        "market_value_ccy": r["mx_mkt_value_ccy"], "underlying_asset_id": r["mx_underlying"],
        "strike_price": decimal_strip(r["mx_strike"]), "buy_sell_ind": r["mx_buy_sell"],
        "entity_code": r["mx_book"][0:4], **_common(bd, batch),
    }


def _summit(r: dict, bd: str, batch: str) -> dict:
    return {
        "trade_id": r["smt_deal_no"], "trade_version": r["smt_version_no"], "source_system": "SUMMIT",
        "source_trade_ref": r["smt_deal_no"], "trade_status": r["smt_deal_status"], "trade_type": r["smt_deal_type"],
        "product_type": r["smt_product_group"], "product_subtype": r["smt_product_class"], "book_id": r["smt_book"],
        "desk_id": r["smt_desk"], "entity_code": r["smt_entity"], "counterparty_id": r["smt_cpty_id"],
        "counterparty_name": r["smt_cpty_name"], "trade_date": r["smt_trade_date"],
        "settlement_date": r["smt_settle_date"], "maturity_date": r["smt_maturity_date"],
        "notional_amount": decimal_strip(r["smt_notional"]), "notional_ccy": r["smt_notional_ccy"],
        "settlement_amount": decimal_strip(r["smt_principal"]), "settlement_ccy": r["smt_principal_ccy"],
        "trade_price": decimal_strip(r["smt_rate"]), "market_value": decimal_strip(r["smt_mkt_value"]),
        "market_value_ccy": r["smt_mkt_value_ccy"], "underlying_asset_id": r["smt_underlying_id"],
        "strike_price": decimal_strip(r["smt_strike_rate"]), "buy_sell_ind": r["smt_direction"],
        **_common(bd, batch),
    }


def _to_gbp(amount: Decimal | None, ccy: str, rate: Decimal | None) -> Decimal | None:
    if ccy == "GBP" or rate is None or amount is None:
        return amount
    return (amount * rate).quantize(DECIMAL_DP, ROUND_HALF_UP)


def enrich(extract_dir: Path, lookup_dir: Path, business_date: str, batch_id: str) -> list[dict]:
    rates = {r["base_ccy"]: Decimal(r["spot_rate"].strip()) for r in _read(lookup_dir / f"fx_rates_{business_date}.dat")}
    out = []
    for prefix, fn in (("calypso", _calypso), ("murex", _murex), ("summit", _summit)):
        for r in _read(extract_dir / f"{prefix}_trades_{business_date}.dat"):
            rec = fn(r, business_date, batch_id)
            rate = rates.get(rec["notional_ccy"])
            rec["notional_amount_gbp"] = _to_gbp(rec["notional_amount"], rec["notional_ccy"], rate)
            rec["market_value_gbp"] = _to_gbp(rec["market_value"], rec["notional_ccy"], rate)
            out.append(rec)
    return out


def render(value) -> str:
    """Format a value the way the enriched .dat file stores it."""
    if value is None:
        return ""
    if isinstance(value, Decimal):
        return format(value, "f")
    return str(value)
