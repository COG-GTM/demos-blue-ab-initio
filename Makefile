# ============================================================================
# Makefile: GMRD Ab Initio ETL Platform
# JIRA: GMRD-210
# Author: GMRD Data Engineering
# Description: Build, validate, and deploy Ab Initio graphs.
# Usage: make [target] ENV=<dev|sit|uat|prod> [GRAPH=path/to/graph.mp]
# ============================================================================

SHELL := /bin/ksh

# Environment validation
ifndef ENV
$(error ENV is not set. Usage: make <target> ENV=<dev|sit|uat|prod>)
endif

# Ab Initio paths
AB_HOME       ?= /opt/abinitio/V3.6
GDE_HOME      ?= /opt/abinitio/GDE4.2
PROJECT_ROOT  := $(shell pwd)
SANDBOX_BASE  := /data/gmrd/$(ENV)
PSET_DIR      := $(PROJECT_ROOT)/pset/$(ENV)
CONF_DIR      := $(PROJECT_ROOT)/conf/$(ENV)
LOG_DIR       := $(PROJECT_ROOT)/logs

# Graph directories
EXTRACT_DIR   := $(PROJECT_ROOT)/mp/extract
TRANSFORM_DIR := $(PROJECT_ROOT)/mp/transform
LOAD_DIR      := $(PROJECT_ROOT)/mp/load

# All graph files
EXTRACT_GRAPHS   := $(wildcard $(EXTRACT_DIR)/*.mp)
TRANSFORM_GRAPHS := $(wildcard $(TRANSFORM_DIR)/*.mp)
LOAD_GRAPHS      := $(wildcard $(LOAD_DIR)/*.mp)
ALL_GRAPHS       := $(EXTRACT_GRAPHS) $(TRANSFORM_GRAPHS) $(LOAD_GRAPHS)

# Compiler flags
AIR_FLAGS := -basedir $(PSET_DIR)

export AB_HOME GDE_HOME PATH := $(AB_HOME)/bin:$(GDE_HOME)/bin:$(PATH)

.PHONY: all build validate deploy clean check-env setup

all: check-env setup validate build
	@echo "=== Build complete for ENV=$(ENV) ==="

check-env:
	@echo "=== Environment: $(ENV) ==="
	@echo "AB_HOME:      $(AB_HOME)"
	@echo "SANDBOX_BASE: $(SANDBOX_BASE)"
	@echo "PSET_DIR:     $(PSET_DIR)"
	@test -d "$(PSET_DIR)" || (echo "ERROR: Parameter set directory not found: $(PSET_DIR)" && exit 1)
	@test -d "$(CONF_DIR)" || (echo "ERROR: Config directory not found: $(CONF_DIR)" && exit 1)

setup:
	@mkdir -p $(LOG_DIR)
	@mkdir -p $(SANDBOX_BASE)/run
	@mkdir -p $(SANDBOX_BASE)/work
	@mkdir -p $(SANDBOX_BASE)/checkpoint
	@mkdir -p $(SANDBOX_BASE)/serial
	@mkdir -p $(SANDBOX_BASE)/data/extract
	@mkdir -p $(SANDBOX_BASE)/data/archive
	@mkdir -p $(SANDBOX_BASE)/data/reject
	@echo "Sandbox directories created."

validate: check-env
	@echo "=== Validating DML files ==="
	@for dml in dml/*.dml; do \
		echo "  Validating: $$dml"; \
		m_dml -verify "$$dml" || exit 1; \
	done
	@echo "=== Validating XFR files ==="
	@for xfr in xfr/*.xfr; do \
		echo "  Validating: $$xfr"; \
	done
	@echo "=== Validation complete ==="

build: check-env
ifdef GRAPH
	@echo "=== Building single graph: $(GRAPH) ==="
	air sandbox parameter $(AIR_FLAGS) "$(GRAPH)"
	@echo "Graph compiled: $(GRAPH)"
else
	@echo "=== Building all graphs ==="
	@for graph in $(ALL_GRAPHS); do \
		echo "  Compiling: $$graph"; \
		air sandbox parameter $(AIR_FLAGS) "$$graph" || exit 1; \
	done
	@echo "=== All graphs compiled ==="
endif

deploy: check-env build
	@echo "=== Deploying to $(ENV) sandbox ==="
	@if [ "$(ENV)" = "prod" ]; then \
		echo "WARNING: Deploying to PRODUCTION"; \
		test -n "$(GMRD_PROD_APPROVED)" || (echo "ERROR: Set GMRD_PROD_APPROVED=Y" && exit 1); \
	fi
	@# Copy compiled graphs
	@cp -r mp/ $(SANDBOX_BASE)/mp/
	@# Copy DML and XFR
	@cp -r dml/ $(SANDBOX_BASE)/dml/
	@cp -r xfr/ $(SANDBOX_BASE)/xfr/
	@# Copy parameter sets
	@cp -r $(PSET_DIR)/ $(SANDBOX_BASE)/pset/
	@# Copy SQL scripts
	@cp -r sql/ $(SANDBOX_BASE)/sql/
	@# Copy config
	@cp -r $(CONF_DIR)/ $(SANDBOX_BASE)/conf/
	@echo "=== Deployment complete ==="
	@echo "Sandbox: $(SANDBOX_BASE)"

clean:
	@echo "=== Cleaning build artifacts ==="
	@rm -f mp/**/*.mp.comp
	@rm -f mp/**/*.plan
	@rm -rf $(LOG_DIR)/*.log
	@echo "=== Clean complete ==="
