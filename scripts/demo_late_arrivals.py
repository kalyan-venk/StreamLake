"""Prove the watermark's *drop* path with a real, non-zero late-arrival count.

``make stream`` runs the producer to completion and only then starts the consumer, so every
event, on-time and deliberately-late alike, is already sitting in Kafka before a watermark exists
to be behind. That is fine for proving dedup (it does, exactly: 985 duplicates in, 985 removed),
but it structurally cannot produce a late *drop*, nothing has advanced the watermark yet when the
first event arrives, so nothing can be behind it.

This script interleaves producer and consumer for real:

  1. Delete and recreate the demo's dedicated Kafka topic, and drop and recreate its sink table
     and checkpoint, so this run starts from a genuinely empty backlog. This is not optional
     housekeeping: dropping the sink table and clearing the checkpoint alone is not enough, the
     topic itself retains every message a previous run produced, and without deleting it a second
     run replays that whole backlog on top of its own data and the reconciliation breaks. See
     ``_reset_topic``'s docstring for the exact failure mode this was verified to cause.
  2. Start the consumer, subscribed and running against the now-empty topic.
  3. Produce an on-time batch. The consumer ingests it, and its watermark advances to
     ``max(event_ts seen) - watermark_delay`` (2 minutes) once that batch's micro-batch commits.
  4. Wait for that to happen, then produce a second batch stamped ``--force-late-seconds 600``
     (10 minutes behind wall clock, deterministically, not the usual 60-240s jitter), guaranteed
     older than the now-advanced watermark by a wide margin.
  5. Those events get dropped by Spark's own watermark logic before they ever reach the
     aggregation, which is what ``numRowsDroppedByWatermark`` counts (see
     ``streamlake.stream.consumer.summarize_state_ops``).
  6. Read back what the producer actually sent and what the consumer's own state-operator
     metrics recorded, check the identity
     ``produced == counted + duplicates_removed + late_dropped``, and then independently
     cross-check ``counted`` against a fresh query of the sink table's own ``sum(txns)``, a
     different code path than the metrics being checked, run after the streaming query has
     already stopped.

Run with the pipeline venv active and Kafka up:

    make kafka-up
    .venv/bin/python scripts/demo_late_arrivals.py

Exits non-zero if the reconciliation identity does not hold, if the independent sink query
disagrees with it, or if nothing was actually dropped as late, on the theory that a demo script
that can silently "pass" with mismatched numbers is worse than no demo at all. Designed to be run
repeatedly: each run resets its own topic and table first, so running it twice in a row is
expected to reconcile both times, not just the first.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PY = ROOT / ".venv" / "bin" / "python"
DEMO_TOPIC = "streamlake.transactions.latedemo"

# Timing. Trigger interval is 10s and the watermark only advances between committed micro-batches,
# so these are sized in multiples of that, with margin, not tuned to the second.
STARTUP_WAIT_S = 20
AFTER_PHASE_A_WAIT_S = 40
AFTER_PHASE_B_WAIT_S = 40
CONSUMER_RUN_SECONDS = 150


def _env() -> dict[str, str]:
    env = dict(os.environ)
    env.setdefault("PYTHONPATH", "src")
    env.setdefault("TZ", "UTC")
    if "JAVA_HOME" not in env:
        result = subprocess.run(
            ["/usr/libexec/java_home", "-v", "17"], capture_output=True, text=True, check=True
        )
        env["JAVA_HOME"] = result.stdout.strip()
    env["KAFKA_TOPIC"] = DEMO_TOPIC
    return env


def _kafka_topics(env: dict[str, str], *args: str) -> subprocess.CompletedProcess:
    return subprocess.run(
        [
            "docker",
            "exec",
            "streamlake-kafka",
            "/opt/kafka/bin/kafka-topics.sh",
            "--bootstrap-server",
            "localhost:9092",
            *args,
        ],
        env=env,
        capture_output=True,
        text=True,
    )


def _reset_topic(env: dict[str, str]) -> None:
    """Delete the demo topic if it exists, then recreate it empty.

    This is the fix for a real non-reproducibility bug: dropping the sink table and clearing the
    checkpoint is not enough on its own. With the checkpoint gone, the consumer's next run starts
    from ``startingOffsets=earliest`` again, and if the topic itself still has every message a
    *previous* run of this script produced (it does, Kafka retains them), that whole backlog gets
    re-ingested as if it were new, on top of whatever this run produces. The second run then
    reconciles against a mixture of two runs' data and the identity breaks. Verified failure mode:
    running the script twice in a row without this step gave ``2068 != 2000 + 1636 + 500``, a
    second run's phase A landing on top of the first run's already-committed 68 duplicates plus
    the first run's 1500 counted rows partially miscounted as new duplicates. Deleting and
    recreating the topic (not just the checkpoint) is what makes every run start from a genuinely
    empty backlog, not merely a consumer that has forgotten where it was.

    Broker auto-create (``KAFKA_AUTO_CREATE_TOPICS_ENABLE=true``) does not save you on the create
    side either: it fires on a produce/consume metadata request, but Spark's Kafka source asks
    the **admin API** for partition info before any of that happens, and gets "no such topic" if
    the topic is genuinely new (see ``MISTAKES.md`` #7). Creating it here, before the consumer
    ever starts, sidesteps that race.
    """
    print(f"--- deleting and recreating topic {DEMO_TOPIC} (makes the demo repeatable) ---")
    existing = _kafka_topics(env, "--list").stdout
    if DEMO_TOPIC in existing.splitlines():
        deleted = _kafka_topics(env, "--delete", "--topic", DEMO_TOPIC)
        if deleted.returncode != 0:
            raise RuntimeError(f"failed to delete topic {DEMO_TOPIC}: {deleted.stderr}")
        print(f"deleted existing topic {DEMO_TOPIC}")
        # Deletion is asynchronous on the broker; a create issued immediately after can race it
        # and land on a topic still marked for deletion. Poll until it is actually gone.
        for _ in range(30):
            if DEMO_TOPIC not in _kafka_topics(env, "--list").stdout.splitlines():
                break
            time.sleep(1)
        else:
            msg = f"topic {DEMO_TOPIC} still listed 30s after delete, refusing to proceed"
            raise RuntimeError(msg)
    else:
        print(f"topic {DEMO_TOPIC} did not exist yet")

    created = _kafka_topics(env, "--create", "--topic", DEMO_TOPIC, "--partitions", "3")
    if created.returncode != 0:
        raise RuntimeError(f"failed to create topic {DEMO_TOPIC}: {created.stderr}")
    print(f"created fresh topic {DEMO_TOPIC}")


def _reset_sink(env: dict[str, str]) -> None:
    """Drop the sink table and clear its checkpoint so this run's counts are not contaminated by
    whatever the ordinary `make stream` demo (or a previous run of this script) already wrote.

    Necessary but, on its own, not sufficient, see `_reset_topic` above for the other half of
    this fix.
    """
    print("--- resetting sink table and checkpoint ---")
    checkpoint = ROOT / "checkpoints" / "txn_metrics_1m"
    if checkpoint.exists():
        subprocess.run(["rm", "-rf", str(checkpoint)], check=True)
        print(f"cleared {checkpoint}")

    reports = ROOT / "_reports" / "stream"
    stale_reports = (
        "phaseA_ontime_latest.json",
        "phaseB_late_latest.json",
        "consumer_reconciliation.json",
    )
    for stale in stale_reports:
        (reports / stale).unlink(missing_ok=True)

    drop_script = (
        "from streamlake.config import get_config\n"
        "from streamlake.spark import build_spark\n"
        "cfg = get_config()\n"
        "spark = build_spark('demo-reset', cfg=cfg)\n"
        "table = cfg.table('stream', 'txn_metrics_1m')\n"
        "if spark.catalog.tableExists(table):\n"
        "    spark.sql(f'DROP TABLE {table}')\n"
        "    print(f'dropped {table}')\n"
        "else:\n"
        "    print(f'{table} did not exist yet')\n"
    )
    subprocess.run([str(PY), "-c", drop_script], cwd=ROOT, env=env, check=True)


def _start_consumer(env: dict[str, str]) -> subprocess.Popen:
    print(f"--- starting consumer, run_seconds={CONSUMER_RUN_SECONDS}, topic={DEMO_TOPIC} ---")
    log_path = ROOT / "_reports" / "stream" / "demo_consumer.log"
    log_path.parent.mkdir(parents=True, exist_ok=True)
    log_file = open(log_path, "w")
    proc = subprocess.Popen(
        [str(PY), "-m", "streamlake", "consume", "--run-seconds", str(CONSUMER_RUN_SECONDS)],
        cwd=ROOT,
        env=env,
        stdout=log_file,
        stderr=subprocess.STDOUT,
    )
    return proc


def _produce(env: dict[str, str], *args: str) -> None:
    cmd = [str(PY), "-m", "streamlake", "produce", *args]
    print(f"--- {' '.join(cmd[3:])} ---")
    subprocess.run(cmd, cwd=ROOT, env=env, check=True)


def _read_json(path: Path) -> dict:
    if not path.exists():
        raise RuntimeError(f"expected report at {path}, it was never written")
    return json.loads(path.read_text())


def _query_sink_sum_txns(env: dict[str, str]) -> int:
    """The actual, independent cross-check: query `sum(txns)` from the sink table itself, not
    from anything Spark's streaming query self-reported during the run.

    `reconciliation["input_rows"]` and the per-batch dedup/late-drop counts all come from the
    same running query's own progress metrics; comparing them to each other is arithmetic, not
    verification, they cannot disagree with themselves. This function opens a fresh, separate
    Spark session after the streaming query has already stopped and reads the materialized
    Iceberg table, a different code path than the one that produced the metrics being checked
    against it. If this number does not match `counted`, the metrics were wrong (or the merge
    was), not just the arithmetic.
    """
    query_script = (
        "from streamlake.config import get_config\n"
        "from streamlake.spark import build_spark\n"
        "cfg = get_config()\n"
        "spark = build_spark('demo-verify', cfg=cfg)\n"
        "table = cfg.table('stream', 'txn_metrics_1m')\n"
        "row = spark.table(table).selectExpr('sum(txns) as total').collect()[0]\n"
        "print('SINK_SUM_TXNS=' + str(row['total'] or 0))\n"
    )
    result = subprocess.run(
        [str(PY), "-c", query_script], cwd=ROOT, env=env, capture_output=True, text=True, check=True
    )
    for line in result.stdout.splitlines():
        if line.startswith("SINK_SUM_TXNS="):
            return int(line.split("=", 1)[1])
    raise RuntimeError(
        "did not find SINK_SUM_TXNS in the verification query's output:\n" + result.stdout
    )


def main() -> int:
    env = _env()
    _reset_topic(env)
    _reset_sink(env)

    consumer_proc = _start_consumer(env)
    print(f"waiting {STARTUP_WAIT_S}s for the consumer to subscribe and start its first trigger")
    time.sleep(STARTUP_WAIT_S)

    # Phase A: on-time batch. Real duplicate rate left on, so this phase also re-proves dedup in
    # the same run. skip_events=0 draws from the start of the curated file; this demo topic has
    # never seen these trans_num keys before, so there is nothing to collide with.
    _produce(
        env,
        "--max-events",
        "1500",
        "--skip-events",
        "0",
        "--label",
        "phaseA_ontime",
        "--late-rate",
        "0",
    )

    print(f"waiting {AFTER_PHASE_A_WAIT_S}s for phase A's micro-batch to commit and the "
          "watermark to advance")
    time.sleep(AFTER_PHASE_A_WAIT_S)

    # Phase B: forced 10 minutes behind wall clock, deterministically. By now the watermark has
    # advanced to roughly (phase A send time - 2 minutes), which is already comfortably behind
    # wall clock; 10 minutes behind is a wide safety margin against timing jitter, not a tight
    # bound. skip_events=1500 draws a disjoint slice of the curated file so these trans_num keys
    # were never sent in phase A, keeping the late-drop count unambiguous (no dedup interaction).
    _produce(
        env,
        "--max-events",
        "500",
        "--skip-events",
        "1500",
        "--label",
        "phaseB_late",
        "--force-late-seconds",
        "600",
        "--duplicate-rate",
        "0",
    )

    print(f"waiting {AFTER_PHASE_B_WAIT_S}s for phase B's micro-batch to commit and the drop to "
          "register in the state-operator metrics")
    time.sleep(AFTER_PHASE_B_WAIT_S)

    elapsed = STARTUP_WAIT_S + AFTER_PHASE_A_WAIT_S + AFTER_PHASE_B_WAIT_S
    remaining = CONSUMER_RUN_SECONDS - elapsed
    if remaining > 0:
        print(f"waiting up to {remaining}s more for the consumer's bounded run to finish")
    consumer_proc.wait(timeout=max(remaining, 0) + 60)

    reports = ROOT / "_reports" / "stream"
    phase_a = _read_json(reports / "phaseA_ontime_latest.json")
    phase_b = _read_json(reports / "phaseB_late_latest.json")
    reconciliation = _read_json(reports / "consumer_reconciliation.json")

    produced = phase_a["sent"] + phase_b["sent"]
    duplicates_removed = reconciliation["dedup_removed"]
    late_dropped = reconciliation["late_dropped"]
    input_rows = reconciliation["input_rows"]
    counted = input_rows - duplicates_removed - late_dropped

    print("--- querying the sink table directly for an independent cross-check ---")
    sink_sum_txns = _query_sink_sum_txns(env)

    a_sent, a_dup = phase_a["sent"], phase_a["duplicates"]
    b_sent = phase_b["sent"]
    print()
    print("=" * 78)
    print("LATE-ARRIVAL DROP DEMO: RECONCILIATION")
    print("=" * 78)
    print(f"phase A (on time)           sent={a_sent:>6}  duplicates_injected={a_dup}")
    print(f"phase B (forced 600s late)  sent={b_sent:>6}")
    print(f"produced total (phase A + phase B)        = {produced}")
    print(f"consumer numInputRows total (all batches) = {input_rows}")
    print(f"  dedup_removed (from state operator)     = {duplicates_removed}")
    print(f"  late_dropped  (from state operator)     = {late_dropped}")
    print(f"  => counted (input - dedup - late)       = {counted}")
    print(f"sink table sum(txns), queried independently after the run  = {sink_sum_txns}")
    print()

    identity_holds = produced == (counted + duplicates_removed + late_dropped)
    sink_matches = sink_sum_txns == counted
    print("identity: produced == counted + duplicates_removed + late_dropped")
    total = counted + duplicates_removed + late_dropped
    print(f"          {produced} == {counted} + {duplicates_removed} + {late_dropped}  ({total})")
    print(f"HOLDS: {identity_holds}")
    print(f"sink cross-check: sum(txns)={sink_sum_txns} == counted={counted}  "
          f"MATCHES: {sink_matches}")
    print(f"late_dropped > 0: {late_dropped > 0}")
    print("=" * 78)

    ok = identity_holds and sink_matches and late_dropped > 0
    if not ok:
        reasons = []
        if not identity_holds:
            reasons.append("the reconciliation identity did not hold")
        if not sink_matches:
            reasons.append(f"sink sum(txns)={sink_sum_txns} did not match counted={counted}")
        if late_dropped <= 0:
            reasons.append("nothing was actually dropped as late")
        log_path = reports / "demo_consumer.log"
        print(f"FAILED: {'; '.join(reasons)}. See {log_path} for the consumer log.")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
