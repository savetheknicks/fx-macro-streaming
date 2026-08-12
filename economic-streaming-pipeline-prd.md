# Project requirements: local economic data streaming pipeline

## 1. Overview

**Goal:** build a locally-runnable streaming data pipeline using free economic data sources, to develop hands-on skill with Kafka-based streaming, stream processing, and time-series storage — without needing a cloud account.

**Learning objectives:**
- Understand streaming semantics in practice: offsets, consumer groups, at-least-once vs exactly-once delivery
- Build and reason about a CDC-style / event-driven producer architecture
- Implement stateful stream processing (windowed aggregations, anomaly detection)
- Model and query time-series data effectively
- Confront the gap between "true real-time" and "frequently polled" data sources, and design around it honestly

**Non-goals:** production-grade reliability, horizontal scaling, cloud deployment (this is a follow-on project once you're on AWS).

---

## 2. Data sources & strategy

A key design decision up front: **most public economic data is not truly real-time.** Macro indicators (GDP, CPI, unemployment) update monthly or quarterly. Even "live" data like FX rates from free tiers is polled, not pushed. This project uses two complementary sources so you get both realistic constraints and enough event volume to practice streaming mechanics properly.

| Source | What it provides | Update frequency | Auth | Role in project |
|---|---|---|---|---|
| **Alpha Vantage** (`CURRENCY_EXCHANGE_RATE`, `FX_INTRADAY`) | Forex quotes (e.g. USD/EUR, USD/JPY) | Polled, free tier ~25 req/day or 5/min | Free API key | Live producer — simulates a real-time feed via polling |
| **Frankfurter API** (frankfurter.app) | ECB daily reference exchange rates | Once per business day | None required | Secondary/backup live source, good for a second topic with different cadence |
| **FRED (Federal Reserve Economic Data)** | Historical macro series (unemployment, CPI, Fed funds rate, GDP) | Monthly/quarterly, decades of history | Free API key | Historical replay producer — streamed at accelerated pace to generate volume |

**Honest framing to keep in mind while building:** the FX poller gives you real streaming semantics (small, frequent, genuinely time-ordered events) but low volume due to rate limits. The FRED replay producer gives you volume and lets you practice things like backpressure and consumer lag, but it's synthetic — you're replaying history fast, not observing it live. Naming this distinction in your own documentation is itself a useful engineering habit.

---

## 3. Technical architecture

```
FX rate poller ──┐
                  ├──> Kafka ──> Stream processor ──> TimescaleDB/Postgres ──> (Grafana, optional)
FRED replay ──────┘              (Kafka UI for monitoring)
```

### 3.1 Producer layer
- **FX rate poller**: Python script polling Alpha Vantage on an interval respecting the free-tier rate limit, publishing to a `fx.rates` Kafka topic.
- **FRED replay producer**: Python script that pulls historical FRED series once, then "replays" them onto a `macro.history` topic at an accelerated rate (e.g. 1 event/second regardless of original monthly cadence) — this is your volume/ordering practice ground.

### 3.2 Message broker
- **Kafka** (Confluent's Docker images, or Redpanda as a lighter-weight Kafka-compatible alternative — worth trying both to compare operational feel)
- Topics: `fx.rates`, `macro.history`
- Use **Kafka UI** (`provectuslabs/kafka-ui`) or Redpanda Console for visual topic/consumer-group inspection — much easier than `kafka-console-consumer` while learning.

### 3.3 Stream processing layer
- Options, roughly in order of learning depth vs. setup cost:
  - **Faust** (Python, Kafka Streams-like API) — lowest friction given your Python background
  - **kafka-python** + manual windowing logic — more instructive, more code
  - **Kafka Streams** (Java/Scala) — most "correct" but adds a JVM learning curve; optional stretch
- Compute: rolling averages (e.g. 5-minute mean FX rate), and simple anomaly detection (z-score threshold on rate-of-change) — flag anomalous events to a separate `fx.anomalies` topic.

### 3.4 Storage layer
- **TimescaleDB** (Postgres extension — you already know Postgres, this is a natural extension into hypertables and time-bucket queries)
- Consumer process reads from Kafka topics and writes to hypertables.

### 3.5 Monitoring
- Kafka UI / Redpanda Console for topic and consumer-group visibility (required)
- Grafana + the Postgres/Timescale data source, for dashboards (stretch goal — not required to hit core learning objectives)

---

## 4. Data model

### Kafka topic schemas (JSON)

**`fx.rates`**
```json
{
  "event_id": "uuid",
  "pair": "USD/EUR",
  "rate": 0.9123,
  "source": "alpha_vantage",
  "observed_at": "2026-07-03T14:32:00Z",
  "ingested_at": "2026-07-03T14:32:01Z"
}
```

**`macro.history`**
```json
{
  "event_id": "uuid",
  "series_id": "UNRATE",
  "value": 4.1,
  "period": "2026-05-01",
  "source": "fred",
  "replayed_at": "2026-07-03T14:32:00Z"
}
```

**`fx.anomalies`**
```json
{
  "event_id": "uuid",
  "pair": "USD/EUR",
  "rate": 0.9123,
  "z_score": 3.4,
  "window_start": "2026-07-03T14:27:00Z",
  "window_end": "2026-07-03T14:32:00Z"
}
```

### TimescaleDB hypertable (example)

```sql
CREATE TABLE fx_rates (
  time TIMESTAMPTZ NOT NULL,
  pair TEXT NOT NULL,
  rate NUMERIC NOT NULL,
  source TEXT NOT NULL
);
SELECT create_hypertable('fx_rates', 'time');
```

---

## 5. Docker Compose services

- `kafka` (or `redpanda`)
- `kafka-ui` (or Redpanda Console)
- `timescaledb` (Postgres + Timescale extension)
- `fx-producer` (your polling script)
- `fred-replay-producer` (your replay script)
- `stream-processor` (Faust worker or equivalent)
- `sink-consumer` (Kafka → TimescaleDB writer)
- `grafana` (stretch)

---

## 6. Milestones

| Milestone | Deliverable | Learning focus |
|---|---|---|
| M1 | Docker Compose brings up Kafka + Kafka UI + TimescaleDB, topics created | Infra fluency, Docker networking |
| M2 | FX poller and FRED replay producers publishing to their topics | API integration, producer design, backoff/retry |
| M3 | Sink consumer writing raw events into TimescaleDB, verified via query | Consumer groups, at-least-once delivery, idempotent writes |
| M4 | Stream processor computing rolling averages + anomaly flags | Windowing, stateful stream processing |
| M5 | Deliberately break something (kill a broker mid-write, misconfigure a consumer group, drop the rate limit) and document what happens | Failure modes, backpressure, consumer lag |
| M6 (stretch) | Grafana dashboard on top of TimescaleDB | Observability, dashboard-as-code |

---

## 7. Success criteria

- Pipeline runs end-to-end via a single `docker compose up`
- Both producers publish without crashing on API rate-limit errors (graceful backoff, not silent failure)
- Consumer lag is visible and explainable via Kafka UI
- At least one anomaly detection run produces a defensible flagged event
- You can articulate, in your own words, the difference between what this pipeline does and what a "real" real-time economic data feed would require

---

## 8. Tech stack summary

| Layer | Tool |
|---|---|
| Broker | Kafka or Redpanda |
| Producers | Python (`requests`, `confluent-kafka` or `kafka-python`) |
| Stream processing | Faust or manual `kafka-python` windowing |
| Storage | TimescaleDB (Postgres) |
| Monitoring | Kafka UI / Redpanda Console |
| Orchestration | Docker Compose |
| Dashboard (stretch) | Grafana |

---

## 9. Reading material

- **"Designing Data-Intensive Applications" — Martin Kleppmann.** Chapters on replication, partitioning, and stream processing map directly onto this project's core mechanics.
- **"Fundamentals of Data Engineering" — Reis & Housley.** Good applied companion, especially the ingestion and serving chapters.
- **Confluent's "Kafka: The Definitive Guide"** (free ebook from Confluent) — practical reference for topic design, consumer groups, and delivery semantics as you build M2–M4.
- **Debezium docs, "How the Debezium connectors work"** — not used directly in this project, but useful background on CDC patterns if you extend this later to a database-sourced stream instead of API polling.
- **TimescaleDB docs, "Hypertables" and "Continuous aggregates"** — directly relevant to Stage 4/M4 windowed aggregation work, and shows a database-native alternative to doing windowing in the stream processor.

---

## 10. Stretch goals

- Swap the FRED replay producer's sink for a second, database-sourced CDC stream (Debezium + Postgres) to compare API-polling vs. log-based CDC as two different "producer philosophies"
- Add LocalStack to emulate S3 as a raw-event landing zone before the Kafka topics — a low-risk way to start getting AWS-shaped muscle memory before opening a real account
- Add a second anomaly detection strategy (e.g. moving average crossover) and compare false-positive rates against the z-score approach
- Add Schema registry
- Explore dead letter queue for error handling
