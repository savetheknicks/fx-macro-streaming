# fx-macro-streaming

A local, Docker-based streaming data pipeline for free economic data (FX rates + FRED macro indicators) using Kafka, stream processing, and TimescaleDB.

> **Note:** This is a personal learning project designed to build hands-on understanding of Kafka-based streaming architectures. It is not a production system.

## Table of Contents

- [Project Overview](#project-overview)
- [Prerequisites](#prerequisites)
- [Local Setup](#local-setup)
- [Running the Application](#running-the-application)
- [Project Structure](#project-structure)
- [Testing](#testing)
- [Troubleshooting](#troubleshooting)

## Project Overview

This streaming pipeline ingests and processes free economic data:

- **Data Sources:** FX exchange rates (via live API) and macro indicators (via FRED API replay)
- **Streaming:** Redpanda (Kafka-compatible) with three topics:
  - `fx.rates` - Foreign exchange rates
  - `macro.history` - Macro economic indicators
  - `fx.anomalies` - Detected anomalies
- **Processing:** Stream processor with rolling averages and anomaly detection
- **Storage:** TimescaleDB for time-series data persistence

**Architecture:** FX Poller + FRED Replay Producers → Redpanda → Stream Processor → TimescaleDB

## Prerequisites

Before setting up the project locally, ensure you have:

- **Python:** 3.13.1 or higher
- **Docker & Docker Compose:** For running Redpanda, Redpanda Console, and TimescaleDB
- **uv:** Python package manager (install from [astral.sh/uv](https://astral.sh/uv))
- **Git:** For version control

Verify your setup:
```bash
python --version  # Should be 3.13.1+
docker --version
docker compose --version
uv --version
```

## Local Setup

### 1. Clone the Repository

```bash
git clone <repository-url>
cd fx-macro-streaming
```

### 2. Install Python Dependencies

```bash
uv sync
```

This command:
- Creates a virtual environment (`.venv`)
- Installs all project dependencies (Kafka client, TimescaleDB driver, etc.)
- Installs dev dependencies (pytest, test fixtures)

### 3. Start Infrastructure Services

Start Docker containers for Redpanda, Redpanda Console, and TimescaleDB:

```bash
docker compose up -d
```

This command:
- Starts a single-node Redpanda broker
- Starts Redpanda Console (web UI for monitoring)
- Starts TimescaleDB with initialized schema
- Creates required Kafka topics automatically

**Verify services are running:**
```bash
docker compose ps
```

All containers should show `healthy` or `running` status.

### 4. Verify Database Connection

Test connectivity to TimescaleDB:

```bash
psql -h localhost -U fxmacro -d fxmacro -c "SELECT version();"
```

Password: `fxmacro`

If successful, you'll see the PostgreSQL version information.

### 5. Access Monitoring Dashboards

- **Redpanda Console (Kafka UI):** http://localhost:8080
- **TimescaleDB:** Connect via `psql` or a database client at `localhost:5432`

## Running the Application

The pipeline consists of independent producer and processor components. Each can be run in separate terminal windows.

### 1. Start the FX Poller (Producer)

Polls live FX rates and publishes to `fx.rates` topic:

```bash
uv run python -m producers.fx_poller
```

### 2. Start the FRED Replay Producer

Replays historical macro indicators to `macro.history` topic:

```bash
uv run python -m producers.fred_replay
```

### 3. Start the Stream Processor

Consumes from `fx.rates` and `macro.history`, applies transformations, and publishes anomalies:

```bash
uv run python -m stream_processor.processor
```

### 4. Start the Sink Consumer (Optional)

Persists processed data to TimescaleDB:

```bash
uv run python -m consumers.sink_consumer
```

**Example Terminal Setup:**

```
Terminal 1: uv run python -m producers.fx_poller
Terminal 2: uv run python -m producers.fred_replay
Terminal 3: uv run python -m stream_processor.processor
Terminal 4: uv run python -m consumers.sink_consumer
```

Monitor data flow in Redpanda Console at http://localhost:8080.

## Project Structure

```
fx-macro-streaming/
├── producers/              # Data ingestion
│   ├── fx_poller.py       # Live FX rates from API
│   ├── fred_replay.py     # Historical macro data replay
│   ├── kafka_client.py    # Producer wrapper
│   └── config.py          # Producer configuration
├── stream_processor/       # Real-time processing
│   ├── processor.py       # Main processing logic
│   ├── windowing.py       # Time-window aggregations
│   ├── kafka_client.py    # Consumer wrapper
│   └── config.py          # Processor configuration
├── consumers/             # Data consumption
│   ├── sink_consumer.py   # Persists to TimescaleDB
│   ├── kafka_client.py    # Consumer wrapper
│   └── config.py          # Consumer configuration
├── db/                    # Database
│   ├── init.sql          # Schema initialization
│   ├── timescale.py      # Database operations
│   └── config.py         # Database configuration
├── tests/                # Test suite
│   ├── producers/
│   ├── stream_processor/
│   ├── consumers/
│   └── db/
├── docker-compose.yml     # Infrastructure definition
├── pyproject.toml        # Python project metadata
└── CLAUDE.md             # Developer guidelines
```

## Testing

### Run All Tests

```bash
uv run pytest
```

### Run a Specific Test File

```bash
uv run pytest tests/producers/test_fx_poller.py
```

### Run Integration Tests

Integration tests require a running TimescaleDB (via `docker compose up -d`):

```bash
uv run pytest -m integration
```

### Test Coverage

```bash
uv run pytest --cov=.
```

## Troubleshooting

### Services won't start

1. **Port conflicts:** Check if ports 19092, 8080, or 5432 are already in use:
   ```bash
   lsof -i :19092
   lsof -i :8080
   lsof -i :5432
   ```
   
2. **Docker daemon not running:** Ensure Docker Desktop is active (macOS).

3. **Container logs:** View detailed error messages:
   ```bash
   docker compose logs redpanda
   docker compose logs timescaledb
   ```

### Connection errors from producers/consumers

- Verify all containers are healthy: `docker compose ps`
- Check broker address in config files matches `localhost:19092`
- Verify firewall isn't blocking Docker network ports

### Database connection refused

- Ensure TimescaleDB is running: `docker compose ps timescaledb`
- Check credentials match in `db/config.py`: user `fxmacro`, password `fxmacro`
- Verify database exists: `docker compose exec timescaledb psql -U fxmacro -l`

### Clean Up & Restart

To reset everything:

```bash
# Stop and remove containers
docker compose down

# Remove data volumes (WARNING: deletes all data)
docker volume rm fx-macro-streaming_redpanda_data fx-macro-streaming_timescale_data

# Restart
docker compose up -d
```

## Additional Resources

- **CLAUDE.md** - Developer guidelines for the project
- **economic-streaming-pipeline-prd.md** - Detailed product requirements and design
- [Redpanda Documentation](https://docs.redpanda.com)
- [TimescaleDB Documentation](https://docs.timescale.com)
- [Confluent Kafka Python Client](https://docs.confluent.io/kafka-clients/python/current/overview.html)
