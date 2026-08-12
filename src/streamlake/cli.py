"""``python -m streamlake <command>``. One entrypoint for every job.

Airflow calls these same functions directly (see airflow/dags), so anything you can run by hand
here is exactly what the DAG runs. There is no second code path that only the scheduler takes.
"""

from __future__ import annotations

import argparse
import sys
from collections.abc import Callable
from datetime import UTC

from streamlake.config import get_config
from streamlake.logging_utils import get_logger

log = get_logger("streamlake.cli")


def _batch(args: argparse.Namespace) -> None:
    from streamlake.batch import bronze, export, gold, ingest, silver

    ingest.run(force=args.force)
    bronze.run()
    silver.run()
    gold.run()
    export.run()


COMMANDS: dict[str, tuple[str, Callable[[argparse.Namespace], object]]] = {}


def command(name: str, help_text: str) -> Callable[[Callable], Callable]:
    def wrapper(fn: Callable) -> Callable:
        COMMANDS[name] = (help_text, fn)
        return fn

    return wrapper


@command("ingest", "download and verify the raw source files")
def _cmd_ingest(args: argparse.Namespace) -> object:
    from streamlake.batch import ingest

    return ingest.run(force=args.force)


@command("bronze", "land raw files into Iceberg bronze tables")
def _cmd_bronze(args: argparse.Namespace) -> object:
    from streamlake.batch import bronze

    return bronze.run()


@command("silver", "conform, quarantine, dedup into Iceberg silver")
def _cmd_silver(args: argparse.Namespace) -> object:
    from streamlake.batch import silver

    return silver.run()


@command("gold", "build the lake-side aggregate tables")
def _cmd_gold(args: argparse.Namespace) -> object:
    from streamlake.batch import gold

    return gold.run()


@command("export", "export the curated layer to parquet for the warehouse")
def _cmd_export(args: argparse.Namespace) -> object:
    from streamlake.batch import export

    return export.run()


@command("batch", "run the whole Layer 1 batch spine end to end")
def _cmd_batch(args: argparse.Namespace) -> object:
    return _batch(args)


@command("warehouse-load", "load the curated parquet into the warehouse (duckdb or snowflake)")
def _cmd_warehouse_load(args: argparse.Namespace) -> object:
    from streamlake.warehouse import load

    return load.run(target=args.target)


@command("produce", "replay transactions onto the Kafka topic")
def _cmd_produce(args: argparse.Namespace) -> object:
    from streamlake.stream import producer

    return producer.run(
        max_events=args.max_events,
        skip_events=args.skip_events,
        force_late_seconds=args.force_late_seconds,
        duplicate_rate=args.duplicate_rate,
        late_rate=args.late_rate,
        label=args.label,
    )


@command("consume", "run the Spark Structured Streaming consumer")
def _cmd_consume(args: argparse.Namespace) -> object:
    from streamlake.stream import consumer

    return consumer.run(run_seconds=args.run_seconds)


@command("score-stream", "score Kafka events through the fraud model into the decision table")
def _cmd_score_stream(args: argparse.Namespace) -> object:
    from streamlake.stream import scorer

    return scorer.run(run_seconds=args.run_seconds)


@command("dashboard", "render the static BI dashboard from the warehouse")
def _cmd_dashboard(args: argparse.Namespace) -> object:
    from streamlake.dashboard import build

    return build.run()


@command("stream-check", "fail if the streaming table's newest window is too far behind")
def _cmd_stream_check(args: argparse.Namespace) -> object:
    """A consumer that starts, processes nothing and exits cleanly leaves a green DAG."""
    from datetime import datetime

    from streamlake.spark import build_spark

    cfg = get_config()
    spark = build_spark("stream-check", cfg=cfg)
    table = cfg.table("stream", "txn_metrics_1m")
    if not spark.catalog.tableExists(table):
        raise RuntimeError(f"{table} does not exist, the stream has never run")

    latest = spark.sql(f"SELECT max(window_end) AS w FROM {table}").collect()[0]["w"]
    if latest is None:
        raise RuntimeError(f"{table} is empty, the stream has never produced a window")

    latest = latest if latest.tzinfo else latest.replace(tzinfo=UTC)
    lag_minutes = (datetime.now(UTC) - latest).total_seconds() / 60
    if lag_minutes > args.max_lag_minutes:
        raise RuntimeError(
            f"stream lag is {lag_minutes:.1f} minutes (limit {args.max_lag_minutes}), "
            "the consumer is not keeping up, or it is not running at all"
        )
    return {"latest_window_end": latest.isoformat(), "lag_minutes": round(lag_minutes, 2)}


@command("summary", "roll the latest contract reports into one summary")
def _cmd_summary(args: argparse.Namespace) -> object:
    from streamlake.contracts.summary import summarise

    return summarise()


@command("contracts", "list the contracts and what each one asserts")
def _cmd_contracts(args: argparse.Namespace) -> object:
    from streamlake.contracts import load_contracts

    cfg = get_config()
    contracts = load_contracts(cfg.root / str(cfg.require("contracts.dir")))
    for contract in contracts.values():
        print(f"\n{contract.name}  ->  {contract.dataset}")
        for check in contract.all_checks:
            marker = "!" if check.severity == "error" else "~"
            print(f"  {marker} {check.label}")
    return {"contracts": len(contracts)}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="streamlake", description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)

    for name, (help_text, _) in COMMANDS.items():
        sub = subparsers.add_parser(name, help=help_text)
        if name in ("ingest", "batch"):
            sub.add_argument("--force", action="store_true", help="re-download even if present")
        if name == "warehouse-load":
            sub.add_argument("--target", default=None, choices=["duckdb", "snowflake"])
        if name == "produce":
            sub.add_argument("--max-events", type=int, default=None, dest="max_events")
            sub.add_argument("--skip-events", type=int, default=0, dest="skip_events")
            sub.add_argument(
                "--force-late-seconds",
                type=int,
                default=None,
                dest="force_late_seconds",
                help="stamp every event this many seconds behind wall clock, deterministically "
                "(for the late-arrival-drop demo; see docs/RUNBOOK.md)",
            )
            sub.add_argument("--duplicate-rate", type=float, default=None, dest="duplicate_rate")
            sub.add_argument("--late-rate", type=float, default=None, dest="late_rate")
            sub.add_argument("--label", default="producer", dest="label")
        if name in ("consume", "score-stream"):
            sub.add_argument("--run-seconds", type=int, default=None, dest="run_seconds")
        if name == "stream-check":
            sub.add_argument(
                "--max-lag-minutes", type=float, default=30.0, dest="max_lag_minutes"
            )

    args = parser.parse_args(argv)
    _, handler = COMMANDS[args.command]
    try:
        result = handler(args)
    except Exception as exc:
        log.error("%s failed: %s", args.command, exc)
        raise
    if result is not None:
        log.info("%s -> %s", args.command, result)
    return 0


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
