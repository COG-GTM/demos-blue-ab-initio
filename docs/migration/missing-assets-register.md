# Missing-assets register

Base commit: `2db2fb408132693d2f2dbc3680c451ce80215294` (main).
Checked-in graph count: 8. Script-referenced absent extract graph count: 6.

“Absent” means no checked-in definition/data/export at this base, not that production lacks it. No CODEOWNERS or verified asset-owner file is present. **All owners below are PRESUMED**, not verified: Data Engineering (DE) for graphs/files/ETL operations, DBA for warehouse definitions per README.md:114 and README.md:115; Risk Analytics (RA) for downstream parity sign-off per README.md:116. Source-system/feed custodians are PRESUMED with basis README.md:7 and README.md:53. Request confirmation rather than treating presumptions as assignments.

## Absent repository graph paths

| Absent asset | Exact reference(s) | Needed / closure evidence | Owner (all PRESUMED) |
|---|---|---|---|
| `mp/extract/ext_counterparty.mp` | scripts/run_daily_batch.ksh:101 | Graph source, source SQL/tables, extraction filters/as-of/watermarks, DML/XFR binding, reject and output contracts, sanitized input/output fixtures; close with reviewed graph and reproducible export manifest | DE + respective upstream custodian |
| `mp/extract/ext_market_data.mp` | scripts/run_intraday_refresh.ksh:45; scripts/run_daily_batch.ksh:127 | Graph source, source SQL/tables, extraction filters/as-of/watermarks, DML/XFR binding, reject and output contracts, sanitized input/output fixtures; close with reviewed graph and reproducible export manifest | DE + respective upstream custodian |
| `mp/extract/ext_positions_multi.mp` | scripts/run_daily_batch.ksh:124 | Graph source, source SQL/tables, extraction filters/as-of/watermarks, DML/XFR binding, reject and output contracts, sanitized input/output fixtures; close with reviewed graph and reproducible export manifest | DE + respective upstream custodian |
| `mp/extract/ext_trades_calypso.mp` | scripts/run_daily_batch.ksh:105 | Graph source, source SQL/tables, extraction filters/as-of/watermarks, DML/XFR binding, reject and output contracts, sanitized input/output fixtures; close with reviewed graph and reproducible export manifest | DE + respective upstream custodian |
| `mp/extract/ext_trades_murex.mp` | scripts/run_daily_batch.ksh:107 | Graph source, source SQL/tables, extraction filters/as-of/watermarks, DML/XFR binding, reject and output contracts, sanitized input/output fixtures; close with reviewed graph and reproducible export manifest | DE + respective upstream custodian |
| `mp/extract/ext_trades_summit.mp` | scripts/run_daily_batch.ksh:109 | Graph source, source SQL/tables, extraction filters/as-of/watermarks, DML/XFR binding, reject and output contracts, sanitized input/output fixtures; close with reviewed graph and reproducible export manifest | DE + respective upstream custodian |

Exactly **six distinct** extract graphs are referenced by daily; intraday reuses ext_market_data, not a seventh graph. README.md:53 through README.md:58 list the same six. No extract graph is checked in.

## Warehouse routines: all executable CALL / EXEC references

| Absent definition | Exact reference(s) | Needed / closure evidence | Owner (PRESUMED) |
|---|---|---|---|
| `GMRD_CORE_DB.MCR_LOAD_AUDIT` | mp/load/ld_market_data_tdw.mp:127 | Full deployed procedure/macro DDL, transitive routines/tables, transaction/error behavior, version/hash, grants and representative before/after exports. Do not infer implementation from call name | DBA + DE |
| `GMRD_CORE_DB.SP_DAILY_POSITION_SNAPSHOT` | mp/load/ld_positions_tdw.mp:64 | Full deployed procedure/macro DDL, transitive routines/tables, transaction/error behavior, version/hash, grants and representative before/after exports. Do not infer implementation from call name | DBA + DE |
| `GMRD_CORE_DB.SP_TRADE_LIFECYCLE` | mp/load/ld_trades_tdw.mp:144; mp/load/ld_trades_tdw.mp:150; mp/load/ld_trades_tdw.mp:156 | Full deployed procedure/macro DDL, transitive routines/tables, transaction/error behavior, version/hash, grants and representative before/after exports. Do not infer implementation from call name | DBA + DE |

MCR_LOAD_AUDIT is invoked with EXEC: obtain its actual object type, not an invented procedure body. The three lifecycle CALLs differ by CALYPSO/MUREX/SUMMIT arguments. No additional executable CALL/EXEC targets were found in graphs, scripts, SQL or XFR. DML option_type CALL/PUT is data, not a procedure reference (dml/trade_record.dml:44).

## Warehouse DDL absent

No CREATE TABLE/VIEW/MACRO/PROCEDURE definitions are checked in. Each object below requires deployed DDL including keys, column order/types/defaults, nullability, constraints, indexes/partitioning, temporal/history rules, grants and dependencies; closure is versioned DDL plus a signed-off PostgreSQL contract and sample exports. Owners: **PRESUMED DBA + DE** for every row. Repeated SQL references are collapsed to the first reference in each file (all files retained).

| Object with absent DDL | Exact reference(s) | Required special evidence |
|---|---|---|
| `GMRD_CORE_DB.AUD_BATCH_CONTROL` | sql/pre_load_cleanup.sql:11 | Audit semantics, retention and writer ownership |
| `GMRD_CORE_DB.AUD_DATA_QUALITY_LOG` | scripts/post_load_validation.ksh:95 | Audit semantics, retention and writer ownership |
| `GMRD_CORE_DB.AUD_RECONCILIATION` | sql/post_load_reconciliation.sql:10 | Audit semantics, retention and writer ownership |
| `GMRD_CORE_DB.CTR_COUNTERPARTIES` | mp/load/ld_counterparty_tdw.mp:48 | SCD2/version boundaries and natural keys |
| `GMRD_CORE_DB.MKT_EOD_PRICES` | mp/load/ld_market_data_tdw.mp:41; scripts/post_load_validation.ksh:81 | Market-data natural key, batch/date history and precision |
| `GMRD_CORE_DB.MKT_FX_RATES` | mp/load/ld_market_data_tdw.mp:66; scripts/post_load_validation.ksh:84 | Market-data natural key, batch/date history and precision |
| `GMRD_CORE_DB.MKT_VOL_SURFACES` | mp/load/ld_market_data_tdw.mp:110 | Market-data natural key, batch/date history and precision |
| `GMRD_CORE_DB.MKT_YIELD_CURVES` | mp/load/ld_market_data_tdw.mp:88; scripts/post_load_validation.ksh:87 | Market-data natural key, batch/date history and precision |
| `GMRD_CORE_DB.POS_DAILY_POSITIONS` | scripts/post_load_validation.ksh:74; sql/post_load_reconciliation.sql:90 | Snapshot/PnL calculation and business-date grain |
| `GMRD_CORE_DB.TRD_TRADES` | scripts/post_load_validation.ksh:59; sql/post_load_reconciliation.sql:34 | Trade lifecycle/current-version behavior |
| `GMRD_STG_DB.STG_COUNTERPARTY_RAW` | mp/load/ld_counterparty_tdw.mp:115; sql/pre_load_cleanup.sql:36 | Source/staging vs conformed field mapping and replay semantics |
| `GMRD_STG_DB.STG_MARKET_DATA_RAW` | sql/pre_load_cleanup.sql:33 | Source/staging vs conformed field mapping and replay semantics |
| `GMRD_STG_DB.STG_POSITIONS_RAW` | mp/load/ld_positions_tdw.mp:40; scripts/post_load_validation.ksh:73; sql/pre_load_cleanup.sql:30; sql/post_load_reconciliation.sql:88 | Source/staging vs conformed field mapping and replay semantics |
| `GMRD_STG_DB.STG_TRADES_CALYPSO` | mp/load/ld_trades_tdw.mp:54; scripts/post_load_validation.ksh:49; sql/pre_load_cleanup.sql:21; sql/post_load_reconciliation.sql:32 | Source/staging vs conformed field mapping and replay semantics |
| `GMRD_STG_DB.STG_TRADES_MUREX` | mp/load/ld_trades_tdw.mp:106; scripts/post_load_validation.ksh:52; sql/pre_load_cleanup.sql:24; sql/post_load_reconciliation.sql:60 | Source/staging vs conformed field mapping and replay semantics |
| `GMRD_STG_DB.STG_TRADES_SUMMIT` | mp/load/ld_trades_tdw.mp:125; scripts/post_load_validation.ksh:55; sql/pre_load_cleanup.sql:27 | Source/staging vs conformed field mapping and replay semantics |

Additional declared target without a checked-in writer: **GMRD_CORE_DB.CTR_HIERARCHY**, dml/counterparty_record.dml:7 and docs/graph_dependency.md:66. Need deployed existence, DDL, writer and hierarchy history grain; PRESUMED DBA + DE. The DML comment names the schema/table but does not prove a deployed table or table load exists.

## File/data contracts and parity exports

All runtime files below are absent from git (data files are ignored, .gitignore:26). Rows distinguish truly unprovided inputs from intermediate/side-effect outputs whose producers are present: absence of an intermediate sample is a **fixture gap**, not a missing implementation. Dates/parameter roots are templates, not repository-relative citations. Need actual sanitized byte-level samples, record counts/checksums, provenance, timing and schema manifests; headers, delimiters, escaping, encoding, decimal precision and null/blank/invalid records must be represented. Owners **PRESUMED DE + source custodian** for external inputs; **PRESUMED DE + RA** for intermediate/expected outputs, based on README.md:114 and README.md:116.

| Missing runtime template / sample | Read references | Checked-in producer / gap | Evidence to close |
|---|---|---|---|
| `${ARCHIVE_DIR}/counterparty_missing_${BUSINESS_DATE}.dat` | No checked-in reader | mp/load/ld_counterparty_tdw.mp:135 | Sanitized expected output and provenance, including empty/error cases |
| `${EXTRACT_DIR}/calypso_trades_${BUSINESS_DATE}.dat` | mp/transform/txfm_trade_enrichment.mp:28 | No checked-in producer; obtain source/lookup generation contract | Upstream/lookup supply contract, sanitized feed snapshots and freshness/availability policy |
| `${EXTRACT_DIR}/counterparty_all_${BUSINESS_DATE}.dat` | mp/transform/txfm_counterparty_hierarchy.mp:27 | No checked-in producer; obtain source/lookup generation contract | Upstream/lookup supply contract, sanitized feed snapshots and freshness/availability policy |
| `${INPUT_DIR}/counterparty_conformed_${BUSINESS_DATE}.dat` = `${OUTPUT_DIR}/counterparty_conformed_${BUSINESS_DATE}.dat` | mp/load/ld_counterparty_tdw.mp:29 | mp/transform/txfm_counterparty_hierarchy.mp:130 | Sanitized expected output and provenance, including empty/error cases |
| `${LOOKUP_DIR}/counterparty_master.dat` | mp/transform/txfm_trade_enrichment.mp:60 | No checked-in producer; obtain source/lookup generation contract | Upstream/lookup supply contract, sanitized feed snapshots and freshness/availability policy |
| `${EXTRACT_DIR}/eod_prices_${BUSINESS_DATE}.dat` = `${LOOKUP_DIR}/eod_prices_${BUSINESS_DATE}.dat` | mp/transform/txfm_market_data_validation.mp:30; mp/transform/txfm_position_aggregation.mp:40 | No checked-in producer; obtain source/lookup generation contract | Upstream/lookup supply contract, sanitized feed snapshots and freshness/availability policy |
| `${EXTRACT_DIR}/eod_prices_${PREV_BUSINESS_DATE}.dat` | mp/transform/txfm_market_data_validation.mp:38 | No checked-in producer; obtain source/lookup generation contract | Upstream/lookup supply contract, sanitized feed snapshots and freshness/availability policy |
| `${INPUT_DIR}/eod_prices_validated_${BUSINESS_DATE}.dat` = `${OUTPUT_DIR}/eod_prices_validated_${BUSINESS_DATE}.dat` | mp/load/ld_market_data_tdw.mp:29 | mp/transform/txfm_market_data_validation.mp:89 | Sanitized expected output and provenance, including empty/error cases |
| `${INPUT_DIR}/fx_rates_${BUSINESS_DATE}.dat` = `${LOOKUP_DIR}/fx_rates_${BUSINESS_DATE}.dat` | mp/transform/txfm_position_aggregation.mp:33; mp/transform/txfm_trade_enrichment.mp:52; mp/load/ld_market_data_tdw.mp:54 | No checked-in producer; obtain source/lookup generation contract | Upstream/lookup supply contract, sanitized feed snapshots and freshness/availability policy |
| `${EXTRACT_DIR}/murex_trades_${BUSINESS_DATE}.dat` | mp/transform/txfm_trade_enrichment.mp:36 | No checked-in producer; obtain source/lookup generation contract | Upstream/lookup supply contract, sanitized feed snapshots and freshness/availability policy |
| `${OUTPUT_DIR}/positions_aggregated_${BUSINESS_DATE}.dat` | No checked-in reader | mp/transform/txfm_position_aggregation.mp:143 | Sanitized expected output and provenance, including empty/error cases |
| `${EXTRACT_DIR}/positions_all_${BUSINESS_DATE}.dat` | mp/transform/txfm_position_aggregation.mp:26 | No checked-in producer; obtain source/lookup generation contract | Upstream/lookup supply contract, sanitized feed snapshots and freshness/availability policy |
| `${INPUT_DIR}/positions_enriched_${BUSINESS_DATE}.dat` = `${OUTPUT_DIR}/positions_enriched_${BUSINESS_DATE}.dat` | mp/load/ld_positions_tdw.mp:27 | mp/transform/txfm_position_aggregation.mp:134 | Sanitized expected output and provenance, including empty/error cases |
| `${EXTRACT_DIR}/summit_trades_${BUSINESS_DATE}.dat` | mp/transform/txfm_trade_enrichment.mp:44 | No checked-in producer; obtain source/lookup generation contract | Upstream/lookup supply contract, sanitized feed snapshots and freshness/availability policy |
| `${INPUT_DIR}/trades_enriched_${BUSINESS_DATE}.dat` = `${OUTPUT_DIR}/trades_enriched_${BUSINESS_DATE}.dat` | mp/load/ld_trades_tdw.mp:31 | mp/transform/txfm_trade_enrichment.mp:233 | Sanitized expected output and provenance, including empty/error cases |
| `${INPUT_DIR}/vol_surfaces_${BUSINESS_DATE}.dat` | mp/load/ld_market_data_tdw.mp:98 | No checked-in producer; obtain source/lookup generation contract | Upstream/lookup supply contract, sanitized feed snapshots and freshness/availability policy |
| `${INPUT_DIR}/yield_curves_${BUSINESS_DATE}.dat` | mp/load/ld_market_data_tdw.mp:76 | No checked-in producer; obtain source/lookup generation contract | Upstream/lookup supply contract, sanitized feed snapshots and freshness/availability policy |
| `${HIERARCHY_FILE}` | mp/transform/txfm_counterparty_hierarchy.mp:34 | No checked-in producer; obtain source/lookup generation contract | Upstream/lookup supply contract, sanitized feed snapshots and freshness/availability policy |
| `${REJECT_DIR}/eod_prices_flagged_${BUSINESS_DATE}.dat` | No checked-in reader | mp/transform/txfm_market_data_validation.mp:96 | Sanitized expected output and provenance, including empty/error cases |
| `${ARCHIVE_DIR}/trades_enriched_${BUSINESS_DATE}.dat.gz` | No checked-in reader | mp/load/ld_trades_tdw.mp:175 | Archived payload/checksum parity, compression/retention and overwrite behavior |
| `${ARCHIVE_DIR}/positions_enriched_${BUSINESS_DATE}.dat.gz` | No checked-in reader | mp/load/ld_positions_tdw.mp:81 | Archived payload/checksum parity, compression/retention and overwrite behavior |

INPUT_DIR / OUTPUT_DIR / EXTRACT_DIR / LOOKUP_DIR aliases resolve to the same extract directory in every pset (pset/dev/gmrd_daily.pset:39); matching basenames therefore establish the intended local handoff, not runtime success. HIERARCHY_FILE resolves to corporate_hierarchy.dat under data/reference (pset/dev/gmrd_daily.pset:45), which Make setup does not create (Makefile:54).

## Further missing operational / parity evidence

| Asset / evidence absent | Exact reference | What closes it | Owner (PRESUMED) |
|---|---|---|---|
| Upstream physical table DDL, extraction SQL and snapshots for Calypso/Murex/Summit, plus Bloomberg/Reuters feeds | README.md:53; README.md:57; xfr/oracle_calypso.xfr:30; xfr/sybase_murex.xfr:29; xfr/oracle_summit.xfr:29 | Source schema/keys/timezone/cutoff and change/delete contracts, approved masked samples; actual physical table names are unresolved | DE + upstream custodians |
| Database before/after exports for all staging/core/audit objects listed above; procedure-derived histories/PnL; downstream aggregates | scripts/post_load_validation.ksh:36; sql/post_load_reconciliation.sql:94; README.md:116 | Same business date/as-of and batch across baseline outputs, source snapshots, counts, totals, precision/tolerance rules and downstream RA sign-off | DBA + DE + RA |
| Edge-case fixtures and replay exports | mp/transform/txfm_market_data_validation.mp:46; mp/transform/txfm_counterparty_hierarchy.mp:41; mp/load/ld_counterparty_tdw.mp:55 | Empty feeds, duplicates/ties, blank LEI, missing FX/price/hierarchy, stale/outlier thresholds, changed/unchanged/missing counterparties, cancelled/amended trades, bad decimals/dates, partial failure/restart, repeated intraday batches and business-date rollover | DE + RA |
| Control>Center daily/intraday job/calendar/alert exports | scripts/run_daily_batch.ksh:12; scripts/run_intraday_refresh.ksh:10; docs/graph_dependency.md:86 | Installed schedule, London DST/business-day calendar, cutoffs, overlap/retry/SLA escalation policies and logs | DE operations |
| Ab Initio setup/runtime, JDBC drivers, BTEQ/TPT binaries and dependency versions | scripts/run_daily_batch.ksh:44; scripts/run_daily_batch.ksh:46; README.md:105; mp/load/ld_trades_tdw.mp:142 | Runtime/package manifests and environment bindings; do not commit credentials or proprietary binaries | DE operations + DBA |
| External Azure variable groups, agent pool and environment approvals | azure-pipelines.yml:20; azure-pipelines.yml:23; azure-pipelines.yml:132 | Redacted definitions/access model and deployment logs, not secret values | DE operations |
| Actual credential bindings and external feeds/sandbox provisioning | pset/dev/gmrd_daily.pset:15; conf/dev/abinitio.cfg:33; Makefile:54 | Secret-manager reference names/access approval, filesystem/permissions manifest and availability checks; **never values** | DE operations + DBA |
| Deployment script distribution / production approval wiring | Makefile:91; azure-pipelines.yml:137 | Evidence how scripts reach sandbox and how GMRD_PROD_APPROVED and environment approvals are provided | DE operations |

No sample source/expected-output data, tests, DDL exports or Control>Center manifests are included at the base. References to batch audit, lineage and alerts do not prove writers/monitoring jobs exist; obtain their implementations if parity requires them (docs/graph_dependency.md:81).

## Related warehouse repo search

README.md:110 references `demos-blue-terradata` (spelling retained). **Placeholder — lead is checking connected repositories separately and will fill this section.** No warehouse-repo contents were inspected; no ownership or missing asset is considered resolved by that reference.
