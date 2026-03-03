#!/bin/ksh
#=============================================================================
# Script: run_intraday_refresh.ksh
# JIRA: GMRD-330
# Author: GMRD Data Engineering
# Description: Intraday market data and position refresh. Runs a subset of
#              the daily batch focusing on market data updates and position
#              re-valuation for intraday risk monitoring.
# Usage: run_intraday_refresh.ksh <environment> <business_date>
# Schedule: Every 2 hours during trading hours (07:00-18:00 London)
#=============================================================================

set -e

SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
PROJECT_ROOT=$(dirname "${SCRIPT_DIR}")
TIMESTAMP=$(date '+%Y%m%d_%H%M%S')

ENV=${1:?"Usage: $0 <dev|sit|uat|prod> <YYYY-MM-DD>"}
BUSINESS_DATE=${2:?"Usage: $0 <environment> <YYYY-MM-DD>"}

. "${PROJECT_ROOT}/pset/${ENV}/gmrd_daily.pset"

export AB_HOME=${AB_HOME:-/opt/abinitio/V3.6}
export PATH=${AB_HOME}/bin:${PATH}
source ${AB_HOME}/abinitio.setup 2>/dev/null || true

BATCH_ID="INTRA_$(date '+%Y%m%d%H%M%S')"
export BATCH_ID BUSINESS_DATE

LOG_DIR="${AB_SANDBOX}/logs/${BUSINESS_DATE}"
mkdir -p "${LOG_DIR}"
LOG_FILE="${LOG_DIR}/intraday_${TIMESTAMP}.log"

log_msg() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" | tee -a "${LOG_FILE}"
}

log_msg "=== Intraday Refresh: ${BUSINESS_DATE} (Batch: ${BATCH_ID}) ==="

# Step 1: Refresh market data
log_msg "Extracting intraday market data..."
air sandbox run \
    -basedir "${PROJECT_ROOT}/pset/${ENV}" \
    "${PROJECT_ROOT}/mp/extract/ext_market_data.mp" \
    BUSINESS_DATE="${BUSINESS_DATE}" \
    BATCH_ID="${BATCH_ID}" \
    2>&1 | tee -a "${LOG_FILE}"

# Step 2: Validate market data
log_msg "Validating market data..."
air sandbox run \
    -basedir "${PROJECT_ROOT}/pset/${ENV}" \
    "${PROJECT_ROOT}/mp/transform/txfm_market_data_validation.mp" \
    BUSINESS_DATE="${BUSINESS_DATE}" \
    BATCH_ID="${BATCH_ID}" \
    PREV_BUSINESS_DATE="${BUSINESS_DATE}" \
    2>&1 | tee -a "${LOG_FILE}"

# Step 3: Load to Teradata
log_msg "Loading intraday market data to Teradata..."
air sandbox run \
    -basedir "${PROJECT_ROOT}/pset/${ENV}" \
    "${PROJECT_ROOT}/mp/load/ld_market_data_tdw.mp" \
    BUSINESS_DATE="${BUSINESS_DATE}" \
    BATCH_ID="${BATCH_ID}" \
    2>&1 | tee -a "${LOG_FILE}"

log_msg "=== Intraday Refresh COMPLETED ==="

exit 0
