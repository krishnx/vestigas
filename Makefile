# Makefile for VESTIGAS Backend

# Variables
HOST ?= localhost
PORT ?= 8000
BASE_URL := http://$(HOST):$(PORT)/backend/deliveries
PYTHON := python3
PYTEST := $(PYTHON) -m pytest
FLAKE8 := $(PYTHON) -m flake8
BLACK := $(PYTHON) -m black

# ----------------------------
# Run backend locally with Docker Compose
# ----------------------------
up:
	docker-compose up --build

down:
	docker-compose down

# ----------------------------
# Health check
# ----------------------------
health:
	curl -s $(BASE_URL)/health | jq

# ----------------------------
# Fetch job
# ----------------------------
fetch-job:
	@echo "Starting a fetch job..."
	curl -s -X POST $(BASE_URL)/fetch \
	-H "Content-Type: application/json" \
	-d '{"siteId": "test-site-001", "date": "2025-10-27"}' | jq

# ----------------------------
# Get job status by ID
# ----------------------------
job-status:
ifndef JOB_ID
	$(error JOB_ID is not set. Usage: make job-status JOB_ID=<job_id>)
endif
	curl -s $(BASE_URL)/jobs/$(JOB_ID) | jq

# ----------------------------
# Get job results by ID
# ----------------------------
job-results:
ifndef JOB_ID
	$(error JOB_ID is not set. Usage: make job-results JOB_ID=<job_id>)
endif
	curl -s $(BASE_URL)/jobs/$(JOB_ID)/results | jq

# ----------------------------
# Search deliveries
# ----------------------------
search-deliveries:
	curl -s "$(BASE_URL)/deliveries?siteId=test-site-001&status=delivered&min_score=4.0&limit=10&offset=0" | jq

# ----------------------------
# Python tasks
# ----------------------------
test:
	$(PYTEST) backend/tests --maxfail=1 --disable-warnings -v

lint:
	$(FLAKE8) backend

format:
	$(BLACK) backend

# ----------------------------
# Dynamic test command
# ----------------------------
test-watch:
	$(PYTEST) backend/tests --maxfail=1 --disable-warnings --looponfail -v

.PHONY: up down health fetch-job job-status job-results search-deliveries test lint format test-watch
