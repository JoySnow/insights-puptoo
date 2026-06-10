# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Platform Upload PreProcessor II (PUPTOO) is a Kafka-based message processor that:
- Receives upload notifications from `platform.upload.announce` topic
- Downloads and processes archives for the `advisor` service using insights-core
- Forwards messages for `compliance` and `malware-detection` services without processing
- Extracts canonical facts and system profile data from archives
- Sends processed data to inventory via `platform.inventory.host-ingress-p1` topic

## Critical Development Rules

**NEVER run poetry, pip, pytest, or any Python development commands directly on the host machine.**

All Python development tasks (installing dependencies, running tests, linting, etc.) MUST be executed inside the development container. The host machine does not have the correct environment configured.

Always use:
1. `make build-dev` to build and enter the container
2. Run all commands inside the container shell

**When reading insights-core source code, always read from `../insights-core`, never from virtual environment directories like `.unit_test_venv`, `.venv`, or `venv`.**

This ensures you're reading the actual source repository rather than installed package copies, which is important for understanding the latest code and making coordinated changes.

## Development Commands

### Running Tests

**IMPORTANT**: Tests must be run inside the development container, not directly on the host machine.

#### Build and Enter Development Container

```bash
# Standard development container (uses insights-core from PyPI)
make build-dev

# With local insights-core for testing unreleased changes
make build-dev WITH_INSIGHTS_CORE=yes
```

The `build-dev` target will:
- Build a container image using `Dockerfile.dev`
- Mount the repository to `/app-root/insights-puptoo` inside the container
- Automatically run `poetry install` to install dependencies
- If `WITH_INSIGHTS_CORE=yes`: mount `../insights-core` and install it in editable mode
- Drop you into an interactive bash shell

#### Inside the Container

```bash
# Run all unit tests
INSIGHTS_FILTERS_ENABLED=false poetry run pytest ./tests

# Run a single test file
INSIGHTS_FILTERS_ENABLED=false poetry run pytest tests/test_app.py

# Run a specific test
INSIGHTS_FILTERS_ENABLED=false poetry run pytest tests/test_app.py::test_get_staletime

# Run linting
poetry run flake8 ./src/
poetry run flake8 ./tests/

# Run full test suite with schema validation
./unit_test_poetry_local.sh
```

**Note**: The `INSIGHTS_FILTERS_ENABLED=false` environment variable is required for tests to run correctly.

### Testing with Unreleased insights-core Changes

The puptoo tests depend on the insights-core library (typically at `~/Work/insights-core/`). To test with unreleased changes:

#### Option 1: Auto-mount with Makefile (Recommended)

```bash
# The Makefile will automatically mount ../insights-core and install it in editable mode
make build-dev WITH_INSIGHTS_CORE=yes

# Inside the container, poetry has already installed insights-core in editable mode
# Just run tests
INSIGHTS_FILTERS_ENABLED=false poetry run pytest ./tests
```

**Requirements**: insights-core must be cloned at `../insights-core` (sibling directory to insights-puptoo)

#### Option 2: Manual Installation Inside Container

```bash
# Start container without insights-core
make build-dev

# Inside the container, manually install insights-core in editable mode
pip install -e /path/to/insights-core

# Or clone inside container if needed
git clone -b <branch-name> https://github.com/RedHatInsights/insights-core /tmp/insights-core
pip install -e /tmp/insights-core

# Run tests
INSIGHTS_FILTERS_ENABLED=false poetry run pytest ./tests
```

### Testing System Profile Extraction

Test an archive locally to debug fact extraction issues. This should be run inside the development container:

```bash
# Inside the development container
poetry run insights-run -p src.puptoo ~/path/to/archive

# Output as JSON
poetry run insights-run -p src.puptoo -f json /path/to/archive.tar.gz
```

### Running Locally

```bash
# Create virtualenv and install
python3.11 -m venv .venv
source .venv/bin/activate
pip install .

# Run the service (requires Kafka)
puptoo
```

### Docker Compose

```bash
# Isolated puptoo with kafka and minio
cd dev && source .env && docker-compose up

# Full stack (requires ingress and inventory images)
cd dev && source .env && docker-compose -f full-stack.yml up
```

### Linting

```bash
flake8
```

### Hermetic Build Maintenance

When updating dependencies in `pyproject.toml` or `poetry.lock`, regenerate hermetic build files:

```bash
# Generate RPM lock files
make generate-repo-file
make generate-rpms-in-yaml
make generate-rpm-lockfile

# Generate Python requirements files
make generate-requirements-txt
make generate-requirements-dev-txt
make generate-requirements-build-in
make generate-requirements-build-txt
```

See `.hermetic_builds/README.md` for detailed instructions.

## Architecture

### Message Flow

1. **Consumer** (`src/puptoo/mq/consume.py`) subscribes to `platform.upload.announce`
2. **Main handler** (`src/puptoo/app.py:handle_message`) routes by service type:
   - **advisor**: Downloads archive → extracts facts → validates → sends to inventory
   - **compliance/malware-detection**: Forwards metadata → sends to inventory
3. **Producer** (`src/puptoo/mq/produce.py`) sends to:
   - `platform.inventory.host-ingress-p1` (processed facts)
   - `platform.upload.validation` (on failure)
   - `platform.payload-status` (tracking messages)

### Fact Extraction (Advisor Service)

- `src/puptoo/process/__init__.py:extract()` orchestrates:
  1. Download archive from S3 URL
  2. Unpack with `insights.extract`
  3. Extract system profile via `get_system_profile()`
  4. Validate extracted size against limits
  5. Postprocess facts

- `src/puptoo/process/profile.py` contains the core extraction logic:
  - Uses insights-core's rule system with `@rule()` decorator
  - `system_profile` rule gathers ~50+ system profile fields from various parsers
  - Parsers read archive files (e.g., `/sys/class/dmi/id/`, `/etc/os-release`, `rpm -qa`)
  - Returns structured facts including `metadata` (canonical facts) and `system_profile`

### Configuration

`src/puptoo/utils/config.py` supports two modes:

- **Clowder mode** (OpenShift): Reads from `app-common-python.LoadedConfig`
- **Legacy mode**: Uses environment variables with defaults for local dev

Key configs: Kafka brokers/topics, S3/Minio credentials, Redis, Prometheus port, log levels.

### Message Formats

`src/puptoo/mq/msgs.py` defines message schemas:
- `inv_message()`: Inventory host ingestion format
- `tracker_message()`: Payload status tracking
- `validation_message()`: Archive validation results

### Retry Logic

Redis tracks processing attempts per `request_id` (max 3 retries). Disabled with `DISABLE_REDIS=true`.

### Metrics

Prometheus metrics in `src/puptoo/utils/metrics.py` track:
- Message consumption/production counts
- Extraction success/failure rates
- Archive size distribution
- Processing time summaries

## Testing Conventions

- Tests use pytest with freezegun for time mocking
- Test archives in `dev/test-archives/core-base/` emulate real archive structure
- Each system profile field should have a test file in `tests/` (e.g., `test_cpuinfo.py`)
- When adding new system profile items, include corresponding test data in `dev/test-archives/core-base/`
- Schema validation runs against `insights-host-inventory/swagger/system_profile.spec.yaml`

## Important Notes

- **insights-core integration**: System profile fields are defined by the insights-core library. Changes to available facts must be coordinated with that project.
- **Archive size limits**: `MAX_EXTRACTED_SIZE` (default 1GB) rejects oversized payloads
- **Service types**: Only `advisor`, `compliance`, and `malware-detection` are supported
- **yum_updates handling**: Extracted from system profile and uploaded to S3 separately as `custom_metadata`
- **Owner ID extraction**: Parsed from `b64_identity` for system_profile when available
