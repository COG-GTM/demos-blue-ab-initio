# GMRD ETL Graph Dependencies

## Daily EOD Batch Execution Order

The following diagram shows the execution order and dependencies for the GMRD daily EOD batch. Graphs at the same level can run in parallel.

```
Level 0 (Independent):
  ext_counterparty.mp ──────────────────────────────────┐
                                                        │
Level 1 (After counterparty):                           │
  ext_trades_calypso.mp  ──┐                            │
  ext_trades_murex.mp    ──┼── (parallel)               │
  ext_trades_summit.mp   ──┘                            │
                                                        │
Level 2 (After trade extracts):                         │
  ext_positions_multi.mp                                │
  ext_market_data.mp                                    │
                                                        │
Level 3 (Transform - parallel where possible):          │
  txfm_counterparty_hierarchy.mp  ◄─────────────────────┘
  txfm_market_data_validation.mp
                                                        
Level 4 (Transform - depends on L3):
  txfm_trade_enrichment.mp     (needs: counterparty, market data, trade extracts)
  txfm_position_aggregation.mp (needs: market data, position extracts)

Level 5 (Load - ordered):
  ld_market_data_tdw.mp     (first - needed for position FX conversion)
  ld_counterparty_tdw.mp    (second - needed for trade FK resolution)

Level 6 (Load - after market data & counterparty):
  ld_trades_tdw.mp          (depends on: counterparty load)
  ld_positions_tdw.mp       (depends on: market data load)
```

## Data Flow

```
Source Systems              Ab Initio ETL                    Teradata EDW
==============             ===============                  =============

Calypso (Oracle)   ──ext──> calypso_trades.dat
                            │
Murex (Sybase)     ──ext──> murex_trades.dat
                            │                      ┌──> STG_TRADES_CALYPSO
                            ├──txfm──> trades_enriched.dat ──ld──> STG_TRADES_MUREX
Summit (Oracle)    ──ext──> summit_trades.dat      │       STG_TRADES_SUMMIT
                            │                      │       TRD_TRADES (via SP)
                            │                      │
All Sources        ──ext──> positions_all.dat      │
                            │                      │
                            ├──txfm──> positions_enriched.dat ──ld──> STG_POSITIONS_RAW
                            │                      │       POS_DAILY_POSITIONS (via SP)
                            │                      │
Bloomberg/Reuters  ──ext──> eod_prices.dat         │
                            fx_rates.dat           │
                            yield_curves.dat       ├──ld──> MKT_EOD_PRICES
                            vol_surfaces.dat       │       MKT_FX_RATES
                            │                      │       MKT_YIELD_CURVES
                            ├──txfm──> validated   │       MKT_VOL_SURFACES
                            │                      │
All Sources        ──ext──> counterparty_all.dat   │
                            │                      │
                            └──txfm──> counterparty_conformed.dat ──ld──> CTR_COUNTERPARTIES
                                                                         CTR_HIERARCHY
```

## SLA Targets

| Phase | DEV | SIT | UAT | PROD |
|-------|-----|-----|-----|------|
| Extract (all sources) | 60 min | 60 min | 45 min | 45 min |
| Transform | 60 min | 45 min | 30 min | 30 min |
| Load | 60 min | 45 min | 45 min | 45 min |
| Post-load validation | 30 min | 30 min | 15 min | 15 min |
| **Total batch** | **300 min** | **240 min** | **210 min** | **180 min** |

## Error Handling

- Each graph has reject ports that capture failed records
- Reject files are written to `${REJECT_DIR}` with date-stamped filenames
- Batch control entries track start/end times and status
- Data lineage records track source-to-target row counts
- Reconciliation queries compare staging vs core counts post-load
- SLA breaches trigger alerts to the monitoring email distribution list
