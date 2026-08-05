# StreamLake

**An end-to-end streaming lakehouse with enforced data contracts.**
Card transactions (Sparkov, a Faker-generated dataset shaped like a real card feed, Jan 2019 -
Dec 2020), ingested twice (a batch job and a live Kafka stream) into an Apache Iceberg
lakehouse, modelled into a warehouse with dbt, served through a dashboard, orchestrated by
Airflow. Every hop asserts a contract and fails loudly when the data breaks it.

```
                    ┌──────────────────────────────────────────── contracts run here ─┐
                    ▼                ▼                ▼                ▼              │
  Sparkov ──▶ bronze.transactions ─▶ silver.transactions ─▶ gold.* ─▶ curated ─▶ DuckDB ─▶ dbt ─▶ dashboard
  train+test        (as landed,       (conformed, PII-      (fraud       parquet   /Snowflake   marts    + freshness
  CSV                 raw PII)         masked, deduped)      KPIs)                                        monitor
                                        │
  synthetic ──▶ Kafka ──▶ Structured Streaming ──▶ stream.txn_metrics_1m
  producer            (1-min windows, 2-min watermark, dedup, MERGE)
```

Everything runs on a laptop for free: Iceberg on MinIO (S3-compatible, one Docker container),
single-container Kafka, DuckDB as the warehouse. The same code runs against real AWS S3, a real
Kafka, and Snowflake by changing environment variables. Nothing is hardcoded to the local setup,
and the MinIO and Snowflake swaps are not just written, both have actually run (numbers below).

---

## What actually ran

Numbers from the run committed in this repo (`_reports/`, `dashboard/build/index.html`):

| | |
|---|---|
| Source | Sparkov card transactions, train+test combined, **1,852,394 transactions**, ~506 MB CSV |
| Quarantined | **0 rows (0.0000%)**: Sparkov is a clean, Faker-generated feed; the quarantine rules run on every row and simply found nothing, unlike a real production card feed |
| Silver | **1,852,394 transactions**, one row per `trans_num`, PII masked, 14 contract checks |
| Gold | 172,753 category-hours · 627,828 state-hours · 554,286 card-days · 700 merchants (min-volume leaderboard) · 2 fraud/legit distance rows |
| Warehouse | 5 staging views, 5 marts, **44 dbt tests green on DuckDB, 44/44 green again on real Snowflake** |
| Contracts | **7 contracts, 49 checks**: 0 errors, 0 warnings |
| Unit tests | **55 passed** (`pytest -q`), real local Spark, no mocks |
| Streaming (dedup) | 20,985 events produced (985 duplicate redeliveries) → **20,000 counted**; watermark-scoped dedup removed exactly the 985 redeliveries |
| Streaming (late-arrival drop) | Dedicated interleaved demo (`scripts/demo_late_arrivals.py`), reconciling exactly and independently cross-checked against the sink table, see below |
| Fraud rate | **0.521%** overall (9,651 of 1,852,394) |
| Object storage | Iceberg on MinIO, S3-compatible, verified: bronze landed 1,852,394 rows as 739 real S3 objects (147 MiB), read back with `mc ls`/`mc du` |
| Snowflake | Verified against a live trial account: `warehouse-load` reconciled all 9 curated tables, `dbt build --target snowflake` ran all 44 tests green (10.7s) |

**Streaming note:** `make stream` runs the producer to completion, then starts the consumer.
Every event, on-time and deliberately-late alike, is already sitting in the Kafka topic by the
time the consumer subscribes, so the first micro-batch consumes the whole backlog before the
watermark has advanced past anything, and nothing is late relative to a watermark that has not
moved yet. That demo proves dedup only (985 produced, 985 removed, exact match every time).
Proving the *drop* path needs producer and consumer genuinely interleaved, which is what
`scripts/demo_late_arrivals.py` does, see "Proving the late-arrival drop" below for the real,
repeatable numbers and how the mechanism works.

---

## Quick start

Requires Docker, a JDK 17 (Spark 4 does not support JDK 25; `export JAVA_HOME=$(/usr/libexec/java_home -v 17)`),
Python 3.11 or 3.12 (the project uses `datetime.UTC`, and PySpark 4.0 does not support 3.14), and
[uv](https://docs.astral.sh/uv/).

```bash
make setup          # virtualenv + dependencies
make batch          # Layer 1: ingest → bronze → silver → gold → export → warehouse → dbt   (~3 min, first run downloads ~506 MB)
make stream         # Layer 2: Kafka up, produce events, consume them into Iceberg            (~2 min)
make dashboard      # Layer 3: render the dashboard
open dashboard/build/index.html
```

`make help` lists every target. Each hop is runnable on its own (`make silver`, `make gold`, …)
and prints its contract results as it goes.

```bash
make test       # unit tests (55, real local Spark)
make lint       # ruff + dbt parse
make contracts  # what every contract asserts
make clean      # delete generated data, keep the downloaded source
```

---

## The idea

Most pipelines fail silently. The job succeeds, the dashboard renders, and the number is quietly
wrong because a vendor renamed a column or last night's load only got half the rows. Worse, for a
fintech dataset: the job succeeds and a cardholder's name or card number is sitting in a table
nobody meant to put it in. No job in the stack notices either failure mode on its own, so the
first person to notice is a stakeholder, or an auditor.

StreamLake puts a **data contract at every hop**: a YAML file next to the pipeline that states
what the dataset must look like, checked before the next hop is allowed to read it. On
`silver_transactions`, the contract's strict schema doubles as a PII gate: any column that is not
on the declared list, `cc_num`, `first`, `last`, `street`, `dob` among them, fails the run.

```yaml
# conf/contracts/silver_transactions.yml (excerpt)
schema:
  strict: true      # cc_num, first, last, street, dob are not declared here: if any
                     # of them reappear, this contract fails the run
  columns:
    - name: cc_num_hash
      type: string
      nullable: false

checks:
  - type: unique
    columns: [trans_num]
    description: >
      The dedup guarantee. This is the assertion that makes a re-run safe and makes the Kafka
      arm's at-least-once delivery harmless.

  - type: accepted_range
    column: cardholder_age
    min: 0
    max: 100
    severity: warn        # a data-quality signal, not a reason to stop the pipeline
```

A breach at `error` severity raises, the task fails, the DAG stops, and the warehouse keeps
serving yesterday's correct data instead of today's broken data. A breach at `warn` severity is
recorded and surfaced on the dashboard.

A few decisions in the engine are worth calling out.

The engine makes one validation pass per hop. Every check compiles itself into Spark *aggregate
expressions*, and the engine collects them into a single `df.agg(...)`. Validating silver's 14
assertions over 1.85M rows took about 3.6 seconds in the committed run. Only checks that actually
flagged rows pay for a second, filtered pass to collect example rows, none did in this run,
Sparkov is clean enough that every silver check passed outright.

Quarantine and the contract work at different levels. A handful of malformed rows is a fact of
life in any real feed, so silver would move those rows to `silver.transactions_quarantine`
tagged with the rule that rejected them, and "where did my transactions go?" would have an answer
you can query. The contract sits on top as a dataset-level gate: if quarantine swallows more than
10% of the data, the run fails even though every surviving row is individually clean. On this
particular source that budget has never been approached; the rules exist for the day the source
is swapped for a messier one.

Freshness is measured against the logical run time. A backfill of 2019-2020 data run today should
still pass, because the assertion checks that the data covers the period it claims to rather than
that it landed recently.

---

## PII: where masking actually happens (and where it does not)

Bronze is a **faithful, unmodified copy** of the Sparkov source: `cc_num`, `first`, `last`,
`street`, `dob`, all in plaintext, for all 1,852,394 rows. That is standard medallion design, not
an oversight: bronze exists to be replayed from when a downstream rule turns out to be wrong, and
a bronze layer that already dropped a field cannot be replayed to fix a rule that needed it.
Sparkov is Faker-generated synthetic data, so no real person's information is in this table
either way, but the pipeline is written as if it were real, because that is the only way the
masking logic actually gets exercised and proven rather than merely asserted.

**Masking happens at the bronze-to-silver hop, once, in `src/streamlake/transforms.py`,** and
nothing past silver ever sees the raw fields again:

- `cc_num` is dropped entirely. `cc_num_last4` (display) and a salted `cc_num_hash` (the join key
  `gold.card_velocity` uses to count transactions per card) replace it.
- `first`, `last`, `street` are dropped outright, no KPI in this project needs them.
- `dob` is dropped and replaced by `cardholder_age`, a derived integer.
- `lat`/`long` (the cardholder's home coordinates) are consumed into `distance_km` and then
  dropped; `merch_lat`/`merch_long` (a business location, not a person's) are kept.

`silver_transactions`'s **strict** contract schema is the enforcement mechanism: `cc_num`,
`first`, `last`, `street`, `dob` are not declared columns, so if any of them reappeared in a
future change to the silver transform, the contract would fail the run rather than silently
letting a PII field through. The streaming arm never touches raw PII at all, the Kafka producer
replays from the *curated* export, which is already past this hop.

---

## Proving the late-arrival drop

`make stream`'s producer-then-consumer ordering can only prove dedup (see the streaming note
above): the watermark literally does not exist until the consumer has ingested something, so
nothing can arrive "late" relative to it in that demo. `scripts/demo_late_arrivals.py` proves the
drop path for real, by interleaving producer and consumer:

1. Delete and recreate a dedicated Kafka topic (`streamlake.transactions.latedemo`) and the
   sink table, so every run starts from a genuinely empty backlog. This step is required, not
   cosmetic: dropping the sink table and clearing the checkpoint alone is not enough, Kafka keeps
   every message a previous run sent, and without deleting the topic a second run replays that
   whole backlog on top of its own data and the reconciliation breaks. (This was caught by
   actually running the script twice in a row during review, see `MISTAKES.md` #16/#17.)
2. Start the consumer, subscribed against the now-empty topic.
3. Produce an on-time batch and wait for its micro-batch to commit, which is what advances the
   watermark to `max(event_ts seen) - 2 minutes`.
4. Produce a second batch stamped `--force-late-seconds 600` (ten minutes behind wall clock,
   deterministically, not the usual randomised 60-240s jitter), which arrives after the watermark
   has already passed it and gets dropped by Spark's own watermark logic before it ever reaches
   the aggregation.
5. Read back what the producer actually sent, what Spark's own state-operator metrics recorded,
   check the reconciliation identity, then **independently** query the sink table's own
   `sum(txns)` in a fresh Spark session, after the streaming query has stopped, a different code
   path than the metrics being checked against it, and confirm the two agree.

Run it yourself (uses Docker Kafka; `make kafka-up` first):

```bash
.venv/bin/python scripts/demo_late_arrivals.py
```

It exits non-zero if the reconciliation identity does not hold, if the independent sink query
disagrees with it, or if nothing was actually dropped as late. Designed to be run repeatedly:
each run resets its own topic and table first. Real numbers from two consecutive runs, both
reconciling:

```
produced = 1,568 (phase A) + 500 (phase B) = 2,068
consumer numInputRows (all batches)        = 2,068
  dedup_removed  (state operator)          = 68
  late_dropped   (state operator)          = 500
  => counted (input - dedup - late)        = 1,500
sink table sum(txns), queried independently after the run = 1,500   MATCHES
identity: 2,068 == 1,500 + 68 + 500   HOLDS: True
```

Getting the *counting* right took a second pass of its own: the first version of the
reconciliation script misread which JSON field on which Spark state operator meant "duplicate
removed" versus "dropped for lateness" (both live on the same operator, split by field name, not
operator name). Found by dumping the raw progress JSON from a real run instead of trusting memory
of the Spark docs, see `MISTAKES.md` #16.

---

## Layers

### Layer 1: the batch spine
`src/streamlake/batch/`, ingest → bronze → silver → gold → export.

Bronze is a faithful copy of the source plus lineage columns (`source_file`, `source_split`,
`batch_id`, `ingested_at`), raw PII included, see "PII" above; nothing is filtered, because
bronze is what you replay from when a silver rule turns out to be wrong. Silver conforms names
and types, **masks the card number to a salted hash plus last four digits, drops the
cardholder's name, street and date of birth outright, and consumes home coordinates into a
distance before dropping them too**, quarantines invalid rows with a reason, and deduplicates on
`trans_num`. Gold builds five fraud KPI aggregates: fraud rate by category and hour, transaction
volume by state and hour, card velocity (a rolling 7-day transaction count per card, a real fraud
signal), a minimum-volume-gated merchant risk leaderboard, and a geo-distance anomaly table
comparing fraud vs legitimate transaction distance from the cardholder's home. Tables are
Iceberg, partitioned by day, written with dynamic partition overwrite so a re-run replaces rather
than appends.

### Layer 2: the streaming arm
`src/streamlake/stream/`, Kafka → Structured Streaming → Iceberg.

The producer replays real curated transactions (already past silver's PII handling, so no raw
cardholder data ever reaches the topic) with a fresh `event_ts`, and misbehaves on purpose: about
5% of events are sent twice and about 3% arrive a little late in the default demo, which is what
gives the consumer's dedup and watermark something to handle (the dedicated late-arrival demo
above uses a much larger, deterministic lateness to force an actual drop). The consumer uses a
2-minute watermark, `dropDuplicatesWithinWatermark(["trans_num"])`, 1-minute windows, and a
`MERGE INTO` sink, not an append, because update mode re-emits a window every time it changes.
Contracts run inside `foreachBatch` *before* the merge, so a bad batch never advances the Kafka
offsets. The source is Kafka's usual at-least-once delivery; the watermark-scoped dedup is what
makes redelivery harmless, this is not exactly-once delivery, it is at-least-once plus a
watermark-bounded dedup that produces the same practical result as long as a redelivery arrives
within the 2-minute window.

### Layer 3: serving
dbt marts on DuckDB (or Snowflake, same models, `DBT_TARGET` picks the engine), a static
self-contained dashboard, a Terraform module and Kubernetes manifests for the streaming consumer.

The dbt layer deliberately **recomputes** Spark's category/hour fraud aggregate in warehouse SQL,
and `tests/assert_batch_spark_dbt_parity.sql` compares every shared column of the two rebuilds,
failing the build if any of them drift past a tolerance that only absorbs last-digit rounding.
Duplicating the logic is the point: it turns silent drift into a red test.

---

## Pointing it at real infrastructure

Every knob is an environment variable; copy `.env.example` to `.env` and source it
(`set -a; . ./.env; set +a`).

### MinIO, then real S3, instead of a local warehouse

MinIO is already wired into `docker/docker-compose.yml` behind the `s3` profile, so the default
demo never touches the network for storage. This path has actually been run end to end (bronze,
1,852,394 rows, both bronze contracts green, 739 real objects / 147 MiB landed in the bucket,
verified with `mc ls`/`mc du`):

```bash
make minio-up                                   # single container, S3-compatible, free
export ICEBERG_WAREHOUSE=s3a://streamlake/warehouse
export AWS_S3_ENDPOINT=http://localhost:9000
export AWS_S3_PATH_STYLE=true
export AWS_ACCESS_KEY_ID=minioadmin
export AWS_SECRET_ACCESS_KEY=minioadmin
export SPARK_EXTRA_PACKAGES="org.apache.iceberg:iceberg-aws-bundle:1.11.0,org.apache.hadoop:hadoop-aws:3.4.1"
make bronze   # or any other hop
```

Two jars, not one, turned out to be necessary. `iceberg-aws-bundle` gives Iceberg's own
`S3FileIO` (table *data* files) the AWS SDK v2 client; skip it and Spark fails immediately with
`UnsupportedFileSystemException`. `hadoop-aws` gives Hadoop's generic `FileSystem` abstraction an
S3A handler, a `hadoop`-type Iceberg catalog still uses **Hadoop's** filesystem, not Iceberg's
`S3FileIO`, for namespace and table-directory operations, a different client with its own
credentials (`spark.hadoop.fs.s3a.*`, set by `_hadoop_s3a_conf()` in `src/streamlake/spark.py`).
Skip either piece and the run gets partway before failing with a confusingly unrelated-looking
error, see `MISTAKES.md` #14 for the full story.

**The swap to real AWS S3 is the same variables, pointed differently:**

```bash
export ICEBERG_WAREHOUSE=s3a://<your-bucket>/streamlake/warehouse
unset AWS_S3_ENDPOINT                           # real S3 needs no endpoint override
export AWS_ACCESS_KEY_ID=...                    # or leave unset and rely on ~/.aws/credentials
export AWS_SECRET_ACCESS_KEY=...
export AWS_REGION=us-east-2
export SPARK_EXTRA_PACKAGES="org.apache.iceberg:iceberg-aws-bundle:1.11.0,org.apache.hadoop:hadoop-aws:3.4.1"
```

The code path does not change between MinIO and real S3, both go through the same S3A/S3FileIO
connectors; only the endpoint and credentials differ. Unsetting `AWS_S3_ENDPOINT` and pointing
`ICEBERG_WAREHOUSE` at a real bucket is the entire change, no code edit. **Not exercised in this
repo**: no run against a real S3 bucket has been recorded, to avoid the AWS bill; MinIO is what
has actually run, and it proved the connector configuration genuinely works rather than just
being present in the code.

### Snowflake instead of DuckDB

```bash
export WAREHOUSE_TARGET=snowflake DBT_TARGET=snowflake
export SNOWFLAKE_ACCOUNT=... SNOWFLAKE_USER=... SNOWFLAKE_PASSWORD=...
export SNOWFLAKE_DATABASE=STREAMLAKE SNOWFLAKE_SCHEMA=RAW
uv pip install -e ".[snowflake]"
make warehouse dbt
```

The loader stages the curated Parquet and `COPY INTO`s with `MATCH_BY_COLUMN_NAME`, so the table
is defined by the Parquet schema rather than a hand-maintained DDL that drifts from the lake.
**Verified against a live trial account:** `warehouse-load` staged and `COPY INTO`'d all 9
curated tables (the 1,852,394-row `transactions` table included) and reconciled every row count;
`dbt build --target snowflake` then ran all 44 tests green against that same account (10.7s). The
default DuckDB path is still what `make batch` uses day to day.

### Kubernetes and Airflow

```bash
make kind-up && make kind-load && make k8s-apply    # local kind cluster, kustomize manifests
make tf-init && make tf-plan && make tf-apply         # or the same objects via OpenTofu

export AIRFLOW_HOME=$PWD/airflow/home AIRFLOW__CORE__DAGS_FOLDER=$PWD/airflow/dags
.venv-airflow/bin/airflow dags test streamlake_batch   # whole batch DAG, no scheduler needed
```

Airflow lives in its own venv (`.venv-airflow`), its pins conflict with the pipeline's, and its
DAG tasks shell out to the same `streamlake` CLI rather than importing PySpark into the
scheduler's interpreter.

---

## When it breaks

**Wrong JDK / obscure reflection errors on startup**
`export JAVA_HOME=$(/usr/libexec/java_home -v 17)`. Spark 4 does not support JDK 25.

**`PYTHON_VERSION_MISMATCH: Python in worker has different version`**
Spark launched workers with the system `python3`. `build_spark()` pins `PYSPARK_PYTHON` to
`sys.executable`; if you create a SparkSession outside it, do the same.

**Streaming: `UnknownTopicOrPartitionException`**
The topic does not exist. Broker auto-create does not save you, Spark's offset reader asks the
admin API before any produce request. Create it explicitly first (`kafka-topics.sh --create`).

**`quarantine rate X% exceeds the 10% budget`**
Silver refused to promote. Query `lakehouse.silver.transactions_quarantine` grouped by
`reject_reason`. On the Sparkov source this has run at exactly 0% every time, it is a clean,
Faker-generated feed; the rules still run on every row, they simply have not found anything.

**Two consumers, one checkpoint**
Structured Streaming locks the checkpoint directory; the second query dies. `replicas: 1` in the
Kubernetes Deployment and `max_active_runs=1` in the Airflow DAG are both deliberate.

---

## Repository map

| Path | What is in it |
|---|---|
| `conf/contracts/` | The contracts. Start here, they describe the data better than the code does. |
| `conf/reference/category_channel.csv` | Small hand-authored reference: Sparkov's 14 categories mapped to a card-present/not-present channel from their own `_net`/`_pos` naming convention. |
| `src/streamlake/contracts/` | The contract engine: spec parsing, checks, the runner, reports. |
| `src/streamlake/batch/` | Ingest, bronze, silver, gold, export. |
| `src/streamlake/stream/` | Kafka producer and the Structured Streaming consumer. |
| `src/streamlake/transforms.py` | Logic shared by both arms: PII masking, distance, validity rules, one definition of "what a clean transaction is". |
| `scripts/demo_late_arrivals.py` | The interleaved producer/consumer demo that proves the watermark's drop path for real. |
| `dbt/streamlake/` | Staging views, marts, generic tests, and the cross-engine parity test. |
| `airflow/dags/` | The nightly batch DAG and the streaming supervisor DAG. |
| `infra/` | Terraform module and kustomize manifests for the Kubernetes deployment. |
| `docker/` | Kafka/MinIO compose stack and the consumer image. |
