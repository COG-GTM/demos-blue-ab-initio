#!/bin/ksh
#=============================================================================
# Script: post_load_validation.ksh
# JIRA: GMRD-375
# Author: GMRD Data Engineering
# Description: Post-load validation and reconciliation. Checks row counts,
#              cross-system reconciliation, and data quality metrics.
#              Called by run_daily_batch.ksh after all load phases complete.
# Usage: post_load_validation.ksh <environment> <business_date> <batch_id>
#=============================================================================

set -e

ENV=${1:?"Usage: $0 <env> <business_date> <batch_id>"}
BUSINESS_DATE=${2:?"Usage: $0 <env> <business_date> <batch_id>"}
BATCH_ID=${3:?"Usage: $0 <env> <business_date> <batch_id>"}

SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
PROJECT_ROOT=$(dirname "${SCRIPT_DIR}")

# Load environment parameters
. "${PROJECT_ROOT}/pset/${ENV}/gmrd_daily.pset"

VALIDATION_FAILED=0

echo "=== Post-Load Validation: ${BUSINESS_DATE} (Batch: ${BATCH_ID}) ==="

# --- Row count reconciliation ---
echo ""
echo "--- Row Count Reconciliation ---"

bteq <<EOF
.LOGON ${TD_JDBC_URL}/${TD_USER},${TD_PASSWORD};
.SET WIDTH 200;

/* Staging vs Core trade counts */
SELECT
    'TRADE_RECON' AS CHECK_NAME,
    stg.SOURCE_SYSTEM,
    stg.STG_COUNT,
    core.CORE_COUNT,
    stg.STG_COUNT - COALESCE(core.CORE_COUNT, 0) AS DIFFERENCE,
    CASE WHEN stg.STG_COUNT = COALESCE(core.CORE_COUNT, 0) THEN 'PASS'
         WHEN ABS(stg.STG_COUNT - COALESCE(core.CORE_COUNT, 0)) <= 10 THEN 'WARNING'
         ELSE 'FAIL'
    END AS STATUS
FROM (
    SELECT 'CALYPSO' AS SOURCE_SYSTEM, COUNT(*) AS STG_COUNT
    FROM GMRD_STG_DB.STG_TRADES_CALYPSO WHERE BATCH_ID = ${BATCH_ID}
    UNION ALL
    SELECT 'MUREX', COUNT(*)
    FROM GMRD_STG_DB.STG_TRADES_MUREX WHERE BATCH_ID = ${BATCH_ID}
    UNION ALL
    SELECT 'SUMMIT', COUNT(*)
    FROM GMRD_STG_DB.STG_TRADES_SUMMIT WHERE BATCH_ID = ${BATCH_ID}
) stg
LEFT JOIN (
    SELECT SOURCE_SYSTEM, COUNT(*) AS CORE_COUNT
    FROM GMRD_CORE_DB.TRD_TRADES
    WHERE BATCH_ID = ${BATCH_ID} AND IS_CURRENT = 1
    GROUP BY SOURCE_SYSTEM
) core ON stg.SOURCE_SYSTEM = core.SOURCE_SYSTEM;

/* Position count check */
SELECT
    'POSITION_RECON' AS CHECK_NAME,
    stg.STG_COUNT,
    core.CORE_COUNT,
    stg.STG_COUNT - COALESCE(core.CORE_COUNT, 0) AS DIFFERENCE,
    CASE WHEN ABS(stg.STG_COUNT - COALESCE(core.CORE_COUNT, 0)) <= 5 THEN 'PASS'
         ELSE 'FAIL'
    END AS STATUS
FROM (SELECT COUNT(*) AS STG_COUNT FROM GMRD_STG_DB.STG_POSITIONS_RAW WHERE BATCH_ID = ${BATCH_ID}) stg
CROSS JOIN (SELECT COUNT(*) AS CORE_COUNT FROM GMRD_CORE_DB.POS_DAILY_POSITIONS
            WHERE BUSINESS_DATE = CAST('${BUSINESS_DATE}' AS DATE FORMAT 'YYYY-MM-DD')
            AND BATCH_ID = ${BATCH_ID}) core;

/* Market data completeness */
SELECT
    'MARKET_DATA_CHECK' AS CHECK_NAME,
    (SELECT COUNT(*) FROM GMRD_CORE_DB.MKT_EOD_PRICES
     WHERE BUSINESS_DATE = CAST('${BUSINESS_DATE}' AS DATE FORMAT 'YYYY-MM-DD')
     AND BATCH_ID = ${BATCH_ID}) AS PRICE_COUNT,
    (SELECT COUNT(*) FROM GMRD_CORE_DB.MKT_FX_RATES
     WHERE BUSINESS_DATE = CAST('${BUSINESS_DATE}' AS DATE FORMAT 'YYYY-MM-DD')
     AND BATCH_ID = ${BATCH_ID}) AS FX_COUNT,
    (SELECT COUNT(*) FROM GMRD_CORE_DB.MKT_YIELD_CURVES
     WHERE BUSINESS_DATE = CAST('${BUSINESS_DATE}' AS DATE FORMAT 'YYYY-MM-DD')
     AND BATCH_ID = ${BATCH_ID}) AS CURVE_COUNT;

/* Data quality summary */
SELECT
    STATUS,
    COUNT(*) AS CHECK_COUNT
FROM GMRD_CORE_DB.AUD_DATA_QUALITY_LOG
WHERE BATCH_ID = ${BATCH_ID}
GROUP BY STATUS
ORDER BY STATUS;

.LOGOFF;
.QUIT 0;
EOF

RC=$?
if [[ ${RC} -ne 0 ]]; then
    echo "WARNING: Validation queries encountered errors (RC=${RC})"
    VALIDATION_FAILED=1
fi

echo ""
echo "=== Validation Complete ==="

exit ${VALIDATION_FAILED}
