#!/bin/ksh
#=============================================================================
# Script: run_daily_batch.ksh
# JIRA: GMRD-213
# Author: GMRD Data Engineering
# Description: Master orchestration script for the GMRD daily EOD batch.
#              Executes extract, transform, and load phases in sequence
#              with dependency management and error handling.
# Usage: run_daily_batch.ksh <environment> <business_date>
#         environment: dev|sit|uat|prod
#         business_date: YYYY-MM-DD
# Schedule: Triggered by Control>Center at COB (17:30 London)
#=============================================================================

set -e

SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
PROJECT_ROOT=$(dirname "${SCRIPT_DIR}")
TIMESTAMP=$(date '+%Y%m%d_%H%M%S')

# --- Argument validation ---
ENV=${1:?"Usage: $0 <dev|sit|uat|prod> <YYYY-MM-DD>"}
BUSINESS_DATE=${2:?"Usage: $0 <environment> <YYYY-MM-DD>"}

# Validate date format
echo "${BUSINESS_DATE}" | grep -qE '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' || {
    echo "ERROR: Invalid date format. Expected YYYY-MM-DD, got: ${BUSINESS_DATE}" >&2
    exit 1
}

# --- Load environment config ---
PSET_FILE="${PROJECT_ROOT}/pset/${ENV}/gmrd_daily.pset"
CONF_FILE="${PROJECT_ROOT}/conf/${ENV}/abinitio.cfg"

if [[ ! -f "${PSET_FILE}" ]]; then
    echo "ERROR: Parameter set not found: ${PSET_FILE}" >&2
    exit 1
fi

# Source parameter set
. "${PSET_FILE}"

# --- Setup Ab Initio environment ---
export AB_HOME=${AB_HOME:-/opt/abinitio/V3.6}
export PATH=${AB_HOME}/bin:${PATH}
source ${AB_HOME}/abinitio.setup 2>/dev/null || true

# --- Generate batch ID ---
BATCH_ID=$(date '+%Y%m%d%H%M%S')
export BATCH_ID BUSINESS_DATE

LOG_DIR="${AB_SANDBOX}/logs/${BUSINESS_DATE}"
mkdir -p "${LOG_DIR}"
MASTER_LOG="${LOG_DIR}/daily_batch_${TIMESTAMP}.log"

log_msg() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" | tee -a "${MASTER_LOG}"
}

run_graph() {
    local GRAPH_NAME=$1
    local GRAPH_FILE=$2
    local GRAPH_LOG="${LOG_DIR}/${GRAPH_NAME}_${TIMESTAMP}.log"

    log_msg "START: ${GRAPH_NAME}"
    local START_TIME=$(date +%s)

    air sandbox run \
        -basedir "${PROJECT_ROOT}/pset/${ENV}" \
        -log "${GRAPH_LOG}" \
        "${PROJECT_ROOT}/${GRAPH_FILE}" \
        BUSINESS_DATE="${BUSINESS_DATE}" \
        BATCH_ID="${BATCH_ID}" \
        2>&1 | tee -a "${MASTER_LOG}"

    local RC=$?
    local END_TIME=$(date +%s)
    local DURATION=$((END_TIME - START_TIME))

    if [[ ${RC} -ne 0 ]]; then
        log_msg "FAILED: ${GRAPH_NAME} (RC=${RC}, Duration=${DURATION}s)"
        log_msg "Check log: ${GRAPH_LOG}"
        return ${RC}
    fi

    log_msg "COMPLETED: ${GRAPH_NAME} (Duration=${DURATION}s)"
    return 0
}

# ============================================================================
# PHASE 1: EXTRACT
# ============================================================================
log_msg "=========================================="
log_msg "GMRD Daily Batch - ${ENV} - ${BUSINESS_DATE}"
log_msg "Batch ID: ${BATCH_ID}"
log_msg "=========================================="
log_msg ""
log_msg "=== PHASE 1: EXTRACT ==="

# Counterparty extract (runs first as lookup for trade enrichment)
run_graph "ext_counterparty" "mp/extract/ext_counterparty.mp" || exit 1

# Trade extracts (run in parallel)
log_msg "Starting parallel trade extractions..."
run_graph "ext_trades_calypso" "mp/extract/ext_trades_calypso.mp" &
PID_CALYPSO=$!
run_graph "ext_trades_murex" "mp/extract/ext_trades_murex.mp" &
PID_MUREX=$!
run_graph "ext_trades_summit" "mp/extract/ext_trades_summit.mp" &
PID_SUMMIT=$!

# Wait for all trade extracts
EXTRACT_FAILED=0
wait ${PID_CALYPSO} || { log_msg "Calypso extract failed"; EXTRACT_FAILED=1; }
wait ${PID_MUREX}   || { log_msg "Murex extract failed"; EXTRACT_FAILED=1; }
wait ${PID_SUMMIT}  || { log_msg "Summit extract failed"; EXTRACT_FAILED=1; }

if [[ ${EXTRACT_FAILED} -ne 0 ]]; then
    log_msg "One or more trade extractions failed. Aborting batch."
    exit 1
fi

# Position extract (after trades, uses same source connections)
run_graph "ext_positions_multi" "mp/extract/ext_positions_multi.mp" || exit 1

# Market data extract
run_graph "ext_market_data" "mp/extract/ext_market_data.mp" || exit 1

log_msg "=== PHASE 1 COMPLETE ==="
log_msg ""

# ============================================================================
# PHASE 2: TRANSFORM
# ============================================================================
log_msg "=== PHASE 2: TRANSFORM ==="

# Counterparty hierarchy (independent)
run_graph "txfm_counterparty_hierarchy" "mp/transform/txfm_counterparty_hierarchy.mp" &
PID_CPTY=$!

# Market data validation (independent)
run_graph "txfm_market_data_validation" "mp/transform/txfm_market_data_validation.mp" &
PID_MKT=$!

wait ${PID_CPTY} || { log_msg "Counterparty transform failed"; exit 1; }
wait ${PID_MKT}  || { log_msg "Market data transform failed"; exit 1; }

# Trade enrichment (depends on counterparty and market data)
run_graph "txfm_trade_enrichment" "mp/transform/txfm_trade_enrichment.mp" || exit 1

# Position aggregation (depends on market data for price lookup)
run_graph "txfm_position_aggregation" "mp/transform/txfm_position_aggregation.mp" || exit 1

log_msg "=== PHASE 2 COMPLETE ==="
log_msg ""

# ============================================================================
# PHASE 3: LOAD
# ============================================================================
log_msg "=== PHASE 3: LOAD ==="

# Market data load first (needed for position snapshot FX conversion)
run_graph "ld_market_data_tdw" "mp/load/ld_market_data_tdw.mp" || exit 1

# Counterparty load (needed for trade lifecycle FK resolution)
run_graph "ld_counterparty_tdw" "mp/load/ld_counterparty_tdw.mp" || exit 1

# Trade load (depends on counterparty for FK)
run_graph "ld_trades_tdw" "mp/load/ld_trades_tdw.mp" || exit 1

# Position load (depends on market data for FX)
run_graph "ld_positions_tdw" "mp/load/ld_positions_tdw.mp" || exit 1

log_msg "=== PHASE 3 COMPLETE ==="
log_msg ""

# ============================================================================
# PHASE 4: POST-LOAD VALIDATION
# ============================================================================
log_msg "=== PHASE 4: POST-LOAD VALIDATION ==="

${SCRIPT_DIR}/post_load_validation.ksh "${ENV}" "${BUSINESS_DATE}" "${BATCH_ID}" 2>&1 | tee -a "${MASTER_LOG}"
RC=$?

if [[ ${RC} -ne 0 ]]; then
    log_msg "WARNING: Post-load validation reported issues (RC=${RC})"
    # Non-fatal - continue but flag for review
fi

log_msg "=== PHASE 4 COMPLETE ==="
log_msg ""

# ============================================================================
# COMPLETION
# ============================================================================
log_msg "=========================================="
log_msg "GMRD Daily Batch COMPLETED"
log_msg "Environment:   ${ENV}"
log_msg "Business Date: ${BUSINESS_DATE}"
log_msg "Batch ID:      ${BATCH_ID}"
log_msg "Master Log:    ${MASTER_LOG}"
log_msg "=========================================="

exit 0
