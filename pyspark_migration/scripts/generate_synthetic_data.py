"""Generate SYNTHETIC source extracts for the trade-enrichment demo.

Every file is laid out from the repo's DML record types (pipe-delimited with a
header row, as the graphs expect). Values are random but seeded, so reruns are
reproducible. No real trade, counterparty or market data is used.
"""
from __future__ import annotations

import argparse
import random
import sys
from datetime import date, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from gmrd_spark.dml import field_names  # noqa: E402

CALYPSO_REF = "dml/trade_record.dml::trade_calypso_raw_t"
MUREX_REF = "dml/trade_record.dml::trade_murex_raw_t"
SUMMIT_REF = "dml/trade_record.dml::trade_summit_raw_t"
FX_REF = "dml/market_data_record.dml::fx_rate_t"
CPTY_REF = "dml/counterparty_record.dml::counterparty_conformed_t"

# GBP is converted 1:1 by the graph; SGD deliberately has no rate so the
# "no FX rate" fallback branch of fx_lookup is exercised.
FX_RATES = {"USD": "0.787400", "EUR": "0.856100", "JPY": "0.005210", "CHF": "0.893300", "AUD": "0.512800"}
CCYS = ["GBP", "USD", "EUR", "JPY", "CHF", "AUD", "SGD"]
PRODUCTS = ["IRS", "FRA", "SWAPTION", "CAP", "FX_SPOT", "FX_FWD", "FX_OPTION", "EQUITY_SWAP",
            "EQUITY_OPTION", "CDS", "TRS", "COMMODITY_SWAP", "REPO", "BOND_FUT"]
CPTYS = [(f"CP{n:05d}", name) for n, name in enumerate(
    ["Synthetic Bank A", "Synthetic Fund B", "Synthetic Insurer C", "Synthetic Corp D",
     "Synthetic Pension E", "Synthetic Broker F", "Synthetic Sovereign G", "Synthetic CCP H",
     "Synthetic Bank I", "Synthetic Fund J", "Synthetic Corp K", "Synthetic Bank L"], start=1)]
ENTITIES = ["UKBK", "USBK", "EUBK"]
STATUSES = ["ACTIVE", "ACTIVE", "ACTIVE", "AMENDED", "CANCELLED"]


def _padded(rng: random.Random, value: float) -> str:
    """Mimic fixed-width upstream numerics: leading zeros and blank padding."""
    txt = f"{value:.2f}"
    neg = txt.startswith("-")
    body = txt.lstrip("-").rjust(rng.choice([12, 14, 16]), "0")
    return (" " * rng.randint(0, 2)) + ("-" if neg else "") + body + (" " * rng.randint(0, 2))


def _trade_core(rng: random.Random, bdate: date, i: int) -> dict:
    tdate = bdate - timedelta(days=rng.choice([0, 0, 1, 2, 5, 30, 90]))
    product = rng.choice(PRODUCTS)
    notional = rng.choice([1, 2.5, 5, 10, 25, 50, 100]) * 1_000_000
    ccy = CCYS[i % len(CCYS)]
    mv = round(notional * rng.uniform(-0.04, 0.04), 2)
    cpty = rng.choice(CPTYS)
    is_opt = "OPTION" in product or product in ("SWAPTION", "CAP")
    return {
        "status": rng.choice(STATUSES),
        "product": product,
        "book": f"{rng.choice(ENTITIES)}-{rng.choice(['RATES', 'FX', 'CRED', 'EQD'])}-{rng.randint(1, 9):02d}",
        "desk": rng.choice(["RATES_LDN", "FX_LDN", "CREDIT_NY", "EQD_LDN"]),
        "trader": f"TR{rng.randint(100, 199)}",
        "cpty": cpty,
        "trade_date": tdate.isoformat(),
        "settle_date": (tdate + timedelta(days=2)).isoformat(),
        "maturity_date": (tdate + timedelta(days=365 * rng.choice([1, 2, 5, 10]))).isoformat(),
        "notional": _padded(rng, notional),
        "ccy": ccy,
        "settle": _padded(rng, notional * rng.choice([0, 0.01, 1])),
        "price": f"{rng.uniform(0.5, 150):.8f}",
        "mv": _padded(rng, mv),
        "underlying": rng.choice(["GBP-SONIA", "USD-SOFR", "EURUSD", "FTSE100", "ITRAXX-MAIN", ""]),
        "strike": f"{rng.uniform(0.5, 5):.4f}" if is_opt else "",
        "buy_sell": rng.choice(["B", "S"]),
    }


def _write(path: Path, header: list[str], rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w") as fh:
        fh.write("|".join(header) + "\n")
        for r in rows:
            fh.write("|".join(str(r.get(h, "")) for h in header) + "\n")


def generate(out_dir: Path, business_date: str, n_calypso=40, n_murex=35, n_summit=25, seed=20261113) -> dict:
    rng = random.Random(seed)
    bdate = date.fromisoformat(business_date)
    stamp = f"{business_date} 18:30:00"
    tag = business_date.replace("-", "")
    extract, lookup = out_dir / "extract", out_dir / "lookup"

    calypso = []
    for i in range(n_calypso):
        t = _trade_core(rng, bdate, i)
        calypso.append({
            "feed_id": 7001, "feed_timestamp": stamp, "trade_ref": f"CLY{tag}{i:05d}", "trade_version": rng.randint(1, 3),
            "trade_status": t["status"], "trade_type": "NEW", "product_type": t["product"], "product_subtype": "VANILLA",
            "book_id": t["book"], "desk_id": t["desk"], "trader_id": t["trader"], "counterparty_id": t["cpty"][0],
            "counterparty_name": t["cpty"][1], "trade_date": t["trade_date"], "settlement_date": t["settle_date"],
            "maturity_date": t["maturity_date"], "notional_amount": t["notional"], "notional_ccy": t["ccy"],
            "settlement_amount": t["settle"], "settlement_ccy": t["ccy"], "trade_price": t["price"],
            "market_value": t["mv"], "market_value_ccy": t["ccy"], "underlying_asset": t["underlying"],
            "strike_price": t["strike"], "option_type": rng.choice(["CALL", "PUT"]) if t["strike"] else "",
            "buy_sell_ind": t["buy_sell"], "long_short_ind": "L" if t["buy_sell"] == "B" else "S", "raw_payload": "{}",
        })

    murex = []
    for i in range(n_murex):
        t = _trade_core(rng, bdate, i + 3)
        murex.append({
            "feed_id": 7002, "feed_timestamp": stamp, "mx_trade_id": f"MX{tag}{i:05d}", "mx_version": rng.randint(1, 3),
            "mx_status": t["status"], "mx_trade_type": "NEW", "mx_product_family": "DERIV", "mx_product_type": t["product"],
            "mx_portfolio": "PF_" + t["desk"], "mx_book": t["book"], "mx_desk": t["desk"], "mx_trader": t["trader"],
            "mx_cpty_code": t["cpty"][0], "mx_cpty_long_name": t["cpty"][1], "mx_trade_date": t["trade_date"],
            "mx_value_date": t["settle_date"], "mx_maturity_date": t["maturity_date"], "mx_nominal": t["notional"],
            "mx_nominal_ccy": t["ccy"], "mx_settle_amount": t["settle"], "mx_settle_ccy": t["ccy"], "mx_price": t["price"],
            "mx_mkt_value": t["mv"], "mx_mkt_value_ccy": t["ccy"], "mx_underlying": t["underlying"],
            "mx_strike": t["strike"], "mx_buy_sell": t["buy_sell"], "raw_payload": "{}",
        })

    summit = []
    for i in range(n_summit):
        t = _trade_core(rng, bdate, i + 5)
        summit.append({
            "feed_id": 7003, "feed_timestamp": stamp, "smt_deal_no": f"SMT{tag}{i:05d}", "smt_version_no": rng.randint(1, 3),
            "smt_deal_status": t["status"], "smt_deal_type": "NEW", "smt_product_class": "VANILLA",
            "smt_product_group": t["product"], "smt_folder": "FLD_" + t["desk"], "smt_book": t["book"], "smt_desk": t["desk"],
            "smt_entity": t["book"][:4], "smt_cpty_id": t["cpty"][0], "smt_cpty_name": t["cpty"][1],
            "smt_trade_date": t["trade_date"], "smt_settle_date": t["settle_date"], "smt_maturity_date": t["maturity_date"],
            "smt_notional": t["notional"], "smt_notional_ccy": t["ccy"], "smt_principal": t["settle"],
            "smt_principal_ccy": t["ccy"], "smt_rate": t["price"], "smt_mkt_value": t["mv"], "smt_mkt_value_ccy": t["ccy"],
            "smt_underlying_id": t["underlying"], "smt_strike_rate": t["strike"], "smt_direction": t["buy_sell"],
            "raw_payload": "{}",
        })

    fx = [{
        "ccy_pair": f"{c}GBP", "base_ccy": c, "quote_ccy": "GBP", "business_date": business_date, "spot_rate": r,
        "spot_bid": r, "spot_ask": r, "fwd_1m": r, "fwd_3m": r, "fwd_6m": r, "fwd_1y": r,
        "data_source": "SYNTHETIC", "fixing_type": "WMR_4PM", "batch_id": "", "load_timestamp": stamp,
    } for c, r in FX_RATES.items()]

    cpty = [{
        "counterparty_id": cid, "legal_name": name, "short_name": name.split()[-1], "counterparty_type": "FI",
        "country_code": "GB", "region": "EMEA", "internal_rating": "BBB", "status": "ACTIVE", "is_current": "1",
        "valid_from_date": "2020-01-01", "valid_to_date": "9999-12-31",
    } for cid, name in CPTYS]

    files = {
        extract / f"calypso_trades_{business_date}.dat": (field_names(CALYPSO_REF), calypso),
        extract / f"murex_trades_{business_date}.dat": (field_names(MUREX_REF), murex),
        extract / f"summit_trades_{business_date}.dat": (field_names(SUMMIT_REF), summit),
        lookup / f"fx_rates_{business_date}.dat": (field_names(FX_REF), fx),
        lookup / "counterparty_master.dat": (field_names(CPTY_REF), cpty),
    }
    for path, (header, rows) in files.items():
        _write(path, header, rows)
    return {str(p.relative_to(out_dir)): len(rows) for p, (_, rows) in files.items()}


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--out", default=str(Path(__file__).resolve().parents[1] / "work" / "data"))
    ap.add_argument("--business-date", default="2026-11-13")
    args = ap.parse_args()
    for f, n in generate(Path(args.out), args.business_date).items():
        print(f"{n:>4} rows  {f}")
