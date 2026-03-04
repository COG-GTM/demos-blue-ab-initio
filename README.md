# GMRD Ab Initio ETL Platform

Global Markets Risk Data (GMRD) — Ab Initio Co>Operating System repository for the Group's market risk ETL pipelines. This repository contains all graph definitions, DML record types, parameter sets, database connectors, and orchestration scripts for the GMRD data integration layer.

## Architecture Overview

The GMRD ETL platform extracts data from three upstream trading systems and loads it into the Teradata Enterprise Data Warehouse:

```
┌──────────────┐     ┌─────────────────────────┐     ┌──────────────────┐
│  Calypso     │────>│                         │────>│ GMRD_STG_DB      │
│  (IR, FX)    │     │   Ab Initio             │     │ (Staging)        │
├──────────────┤     │   Co>Operating System    │     ├──────────────────┤
│  Murex MX.3  │────>│                         │────>│ GMRD_CORE_DB     │
│  (Cross-asset│     │   Extract > Transform   │     │ (Conformed)      │
├──────────────┤     │   > Load                │     ├──────────────────┤
│  Summit FT   │────>│                         │────>│ GMRD_RISK_DB     │
│  (FI, FX)    │     │   GDE 4.2 / Co>Op 3.6  │     │ (Risk Results)   │
└──────────────┘     └─────────────────────────┘     └──────────────────┘
     Source               ETL Layer                    Teradata EDW
```

## Repository Structure

```
├── mp/                  Graph definitions (.mp files)
│   ├── extract/         Source system extraction graphs
│   ├── transform/       Data transformation and enrichment
│   └── load/            Teradata load graphs
├── dml/                 Data Manipulation Language record types
├── pset/                Parameter sets per environment
│   ├── dev/
│   ├── sit/
│   ├── uat/
│   └── prod/
├── xfr/                 Transfer/connector definitions
├── sql/                 SQL scripts for pre/post-load validation
├── scripts/             Shell scripts for batch orchestration
├── conf/                Environment configuration
│   ├── dev/
│   ├── sit/
│   ├── uat/
│   └── prod/
└── docs/                Design documentation
```

## Graph Inventory

### Extract Graphs

| Graph | Source | Schedule | Description |
|-------|--------|----------|-------------|
| `ext_trades_calypso.mp` | Calypso | EOD T+0 | Extract trades from Calypso via JDBC |
| `ext_trades_murex.mp` | Murex MX.3 | EOD T+0 | Extract trades from Murex datamart |
| `ext_trades_summit.mp` | Summit FT | EOD T+0 | Extract trades from Summit database |
| `ext_positions_multi.mp` | All | EOD T+0 | Extract positions from all source systems |
| `ext_market_data.mp` | Bloomberg/Reuters | EOD + Intraday | Market data feed handler |
| `ext_counterparty.mp` | All | Daily | Counterparty reference data extract |

### Transform Graphs

| Graph | Description |
|-------|-------------|
| `txfm_trade_enrichment.mp` | Trade normalisation, counterparty lookup, FX conversion |
| `txfm_position_aggregation.mp` | Position aggregation across sources, Greek calculations |
| `txfm_counterparty_hierarchy.mp` | Counterparty golden source resolution and hierarchy build |
| `txfm_market_data_validation.mp` | Stale price detection, curve interpolation, data quality |

### Load Graphs

| Graph | Target | Description |
|-------|--------|-------------|
| `ld_trades_tdw.mp` | GMRD_STG_DB / GMRD_CORE_DB | Trade staging and conformed load |
| `ld_positions_tdw.mp` | GMRD_STG_DB / GMRD_CORE_DB | Position staging and snapshot load |
| `ld_market_data_tdw.mp` | GMRD_STG_DB / GMRD_CORE_DB | Market data load with validation |
| `ld_counterparty_tdw.mp` | GMRD_CORE_DB | Counterparty master SCD Type 2 load |

## Environment Configuration

| Environment | Ab Initio Sandbox | Teradata Target | Co>Op Version |
|-------------|-------------------|-----------------|---------------|
| DEV | `/data/gmrd/dev` | TDDEV01 | 3.6.1.2 |
| SIT | `/data/gmrd/sit` | TDSIT01 | 3.6.1.2 |
| UAT | `/data/gmrd/uat` | TDUAT01 | 3.6.1.2 |
| PROD | `/data/gmrd/prod` | TDPRD01 | 3.6.1.2 |

## Build & Deploy

```bash
# Build all graphs
make all ENV=dev

# Build specific graph
make build GRAPH=mp/extract/ext_trades_calypso.mp ENV=dev

# Run daily batch
scripts/run_daily_batch.ksh dev 2024-07-19

# Run intraday refresh
scripts/run_intraday_refresh.ksh dev 2024-07-19
```

## Dependencies

- **Ab Initio Co>Operating System 3.6** — Runtime environment
- **Ab Initio GDE 4.2** — Graph development
- **Teradata JDBC 17.20** — Database connectivity
- **Oracle JDBC 19c** — Source system connectivity (Calypso, Summit)
- **Sybase jConnect 16.0** — Source system connectivity (Murex)
- **GMRD Teradata EDW** — Target data warehouse (see `demos-blue-terradata`)

## Team

- **GMRD Data Engineering** — Graph development and ETL operations
- **GMRD DBA Team** — Target database management
- **GMRD Risk Analytics** — Downstream consumers (SAS)

## Support

For issues, raise a JIRA ticket in the **GMRD** project with component `Ab-Initio-ETL`.
For batch failures, check the Ab Initio Operations Console or contact the GMRD ETL on-call.
