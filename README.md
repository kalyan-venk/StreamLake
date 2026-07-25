# StreamLake

**An end-to-end streaming lakehouse with enforced data contracts.**
NYC taxi trips, ingested twice — a nightly batch and a live Kafka stream — into an Apache
Iceberg lakehouse, modelled into a warehouse with dbt, served through a dashboard, orchestrated
by Airflow. Every hop asserts a contract and fails loudly when the data breaks it.

```
                    ┌──────────────────────────────────────────── contracts run here ─┐
                    ▼                ▼                ▼                ▼              │
  NYC TLC ──▶ bronze.trips_raw ─▶ silver.trips ─▶ gold.* ─▶ curated ─▶ DuckDB ─▶ dbt ─▶ dashboard
  parquet          (as landed)     (conformed,     (lake        parquet   /Snowflake   marts    + freshness
                                    quarantined,    serving)                                      monitor
                                    deduped)
                                        │
  synthetic ──▶ Kafka ──▶ Structured Streaming ──▶ stream.trip_metrics_1m
  producer            (1-min windows, 2-min watermark, dedup, MERGE)
```

Everything runs on a laptop for free: local filesystem Iceberg, single-container Kafka, DuckDB
as the warehouse. The same code points at S3, a real Kafka, and Snowflake by changing
environment variables — nothing in the pipeline is hardcoded to the local setup.

---

## What actually ran

Numbers from the run committed in this repo (`_reports/`, `dashboard/build/index.html`):

| | |
|---|---|
| Source | NYC yellow taxi, 2024-01 — **2,964,624 trips**, 48 MB parquet |
| Quarantined | **38,491 rows (1.30%)** — negative totals, backwards timestamps, out-of-period pickups |
| Silver | **2,926,133 trips**, one row per `trip_id`, 14 contract checks |
| Gold | 6,940 zone-days · 4,475 borough-hours · 155 payment-day rows |
| Warehouse | 5 staging views, 5 marts, **46 dbt tests green** |
| Contracts | **7 contracts, 47 checks** — 0 errors, 2 warnings |
| Streaming | 4,182 events produced → **4,000 counted**; dedup removed exactly the 182 redeliveries |
| Orchestration | `airflow dags test streamlake_batch` — 9 tasks, green, 66s |
| Kubernetes | consumer `Running 1/1` on kind, contract passing per micro-batch |

---

## Quick start

Requires Docker, a JDK 17, Python 3.12, and [uv](https://docs.astral.sh/uv/).

```bash
make setup          # virtualenv + dependencies
make batch          # Layer 1: ingest → bronze → silver → gold → export → warehouse → dbt
make stream         # Layer 2: Kafka up, produce events, consume them into Iceberg
make dashboard      # Layer 3: render the dashboard
open dashboard/build/index.html
```

`make help` lists every target. Each hop is runnable on its own (`make silver`, `make gold`, …)
and prints its contract results as it goes.

---

## The idea

Most pipelines fail silently. The job succeeds, the dashboard renders, and the number is wrong —
because a vendor changed a column, a join fanned out, or last night's load only got half the
rows. Nothing in the stack is looking.

StreamLake's answer is a **data contract at every hop**: a YAML file next to the pipeline that
states what the dataset must look like, checked before the next hop is allowed to read it.

```yaml
# conf/contracts/silver_trips.yml (excerpt)
checks:
  - type: unique
    columns: [trip_id]
    description: >
      The dedup guarantee. This is the assertion that makes a re-run safe and makes the Kafka
      arm's at-least-once delivery harmless.

  - type: expression
    expr: trip_duration_min > 0 AND trip_duration_min <= 1440

  - type: accepted_range
    column: avg_speed_mph
    min: 0
    max: 150
    severity: warn        # a data-quality signal, not a reason to stop the pipeline
```

A breach at `error` severity raises, the task fails, the DAG stops, and the warehouse keeps
serving yesterday's correct data instead of today's broken data. A breach at `warn` severity is
recorded and surfaced on the dashboard.

Three design decisions in the engine are worth stating outright:

**One pass, not N.** Every check compiles itself into Spark *aggregate expressions*; the engine
collects them into a single `df.agg(...)`. Validating silver's 14 assertions over 2.9M rows costs
one scan and about 2 seconds. Only checks that actually failed pay for a second, filtered pass to
collect example rows. There is a test that asserts this (`test_all_checks_share_one_aggregate_pass`).

**Quarantine is not the same as failing.** Individual bad trips are a fact of life in TLC data
(fares of −$300, dropoffs in 2098). Those rows are *moved* to `silver.trips_quarantine` with the
rule that rejected them, so "where did my 4,000 trips go?" has an answer you can query. The
contract is the dataset-level gate on top: if quarantine swallows more than 10% of the month, the
run fails even though every surviving row is individually clean.

**Freshness is measured against the logical run time, not the wall clock.** A backfill of January
2024 data is not stale because you ran it in 2026. What you actually want to assert is that the
data covers its own period.

---

## Layers

### Layer 1 — the batch spine
`src/streamlake/batch/` — ingest → bronze → silver → gold → export.

Bronze is a faithful copy of the source plus lineage columns (`trip_id`, `source_file`,
`batch_id`, `ingested_at`); nothing is filtered, because bronze is what you replay from when a
silver rule turns out to be wrong. Silver conforms names and types, quarantines invalid rows with
a reason, deduplicates on `trip_id`, and joins the zone dimension. Gold builds the aggregates the
lake serves directly. Tables are Iceberg, partitioned by day, written with dynamic partition
overwrite so a re-run replaces rather than appends.

### Layer 2 — the streaming arm
`src/streamlake/stream/` — Kafka → Structured Streaming → Iceberg.

The producer replays real curated trips with a fresh `event_ts`, and **misbehaves on purpose**:
5% of events are sent twice and 3% arrive late. A streaming pipeline that has only ever seen
well-behaved input has not been tested. The consumer uses a 2-minute watermark,
`dropDuplicatesWithinWatermark`, 1-minute windows, and a `MERGE INTO` sink — not an append,
because update mode re-emits a window every time it changes. Contracts run inside `foreachBatch`
*before* the merge, so a bad batch never advances the Kafka offsets.

### Layer 3 — serving
dbt marts on DuckDB (or Snowflake — same models, `DBT_TARGET` picks the engine), a static
self-contained dashboard, a Terraform module and Kubernetes manifests for the streaming consumer.

The dbt layer deliberately **recomputes** Spark's daily aggregate in warehouse SQL, and
`tests/assert_batch_spark_dbt_parity.sql` fails the build if the two engines disagree by more
than a cent. Duplicating the logic is the point: it turns silent drift into a red test.

---

## Repository map

| Path | What is in it |
|---|---|
| `conf/contracts/` | The contracts. Start here — they describe the data better than the code does. |
| `src/streamlake/contracts/` | The contract engine: spec parsing, checks, the runner, reports. |
| `src/streamlake/batch/` | Ingest, bronze, silver, gold, export. |
| `src/streamlake/stream/` | Kafka producer and the Structured Streaming consumer. |
| `src/streamlake/transforms.py` | Logic shared by both arms — one definition of "what a trip is". |
| `dbt/streamlake/` | Staging views, marts, generic tests, and the cross-engine parity test. |
| `airflow/dags/` | The nightly batch DAG and the streaming supervisor DAG. |
| `infra/` | Terraform module and kustomize manifests for the Kubernetes deployment. |
| `docker/` | Kafka/MinIO compose stack and the consumer image. |
| `docs/` | Architecture, runbook, and the contract reference. |
| `LEARNING.md` | A guided walkthrough of the whole pipeline, hop by hop. |

## Documentation

- **[docs/ARCHITECTURE.md](docs/ARCHITECTURE.md)** — why each component is there, and what it costs.
- **[docs/DATA_CONTRACTS.md](docs/DATA_CONTRACTS.md)** — every check type, with examples.
- **[docs/RUNBOOK.md](docs/RUNBOOK.md)** — running it, pointing it at S3/Snowflake, what breaks.
- **[LEARNING.md](LEARNING.md)** — the layered walkthrough.
- **[MISTAKES.md](MISTAKES.md)** — bugs found while building this, and what caused them.
