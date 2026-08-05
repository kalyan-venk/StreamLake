"""Step 5, export the curated layer out of the lake, ready for the warehouse.

The lakehouse is the system of record; the warehouse is a serving copy. This step writes the
handful of tables the warehouse actually needs as plain Parquet, plus a manifest recording row
counts and watermarks. The manifest is what the loader checks against after loading, if
the warehouse ends up with fewer rows than the lake exported, the load failed silently and we
want to know before dbt runs.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime

from streamlake.config import Config, get_config
from streamlake.logging_utils import banner, get_logger
from streamlake.spark import build_spark

log = get_logger(__name__)

# lake table -> (curated name, optional watermark column)
EXPORTS: dict[str, tuple[str, str, str | None]] = {
    "transactions": ("silver", "transactions", "trans_time"),
    "dim_category": ("silver", "dim_category", None),
    "category_hourly_fraud": ("gold", "category_hourly_fraud", "trans_hour_ts"),
    "state_hourly_volume": ("gold", "state_hourly_volume", "trans_hour_ts"),
    "card_velocity": ("gold", "card_velocity", "trans_date"),
    "merchant_risk_leaderboard": ("gold", "merchant_risk_leaderboard", None),
    "geo_distance_anomaly": ("gold", "geo_distance_anomaly", None),
    "txn_metrics_1m": ("stream", "txn_metrics_1m", "window_end"),
}

# The streaming table only exists once Layer 2 has run; a batch-only checkout must still work.
OPTIONAL = {"txn_metrics_1m"}


def run(cfg: Config | None = None) -> dict[str, int]:
    from pyspark.sql import functions as F

    cfg = cfg or get_config()
    banner(log, "EXPORT | lakehouse -> curated parquet")

    spark = build_spark("export", cfg=cfg)
    curated_root = cfg.path("curated")
    curated_root.mkdir(parents=True, exist_ok=True)

    manifest: dict[str, dict] = {}
    counts: dict[str, int] = {}

    for name, (layer, table, watermark) in EXPORTS.items():
        identifier = cfg.table(layer, table)
        if not spark.catalog.tableExists(identifier):
            if name in OPTIONAL:
                log.info("skipping %s (not built yet)", identifier)
                continue
            raise RuntimeError(f"expected table {identifier} does not exist, run the batch DAG")

        df = spark.table(identifier)
        target = cfg.curated_dir(name)
        # coalesce(4) because the warehouse loader reads these files directly, and a few hundred
        # small parquet files turn a quick export into a slow load.
        df.coalesce(4).write.mode("overwrite").parquet(str(target))

        row_count = df.count()
        counts[name] = row_count
        entry: dict[str, object] = {
            "source_table": identifier,
            "path": str(target),
            "row_count": row_count,
            "columns": len(df.columns),
        }
        if watermark:
            latest = df.agg(F.max(watermark)).collect()[0][0]
            entry["watermark_column"] = watermark
            entry["watermark_value"] = str(latest)
        manifest[name] = entry
        log.info("exported %-24s %9d rows -> %s", name, row_count, target)

    # Quarantine goes out as a summary. The warehouse only has to answer "what did we reject and
    # why", storing every rejected transaction a second time buys nothing.
    quarantine = cfg.table("silver", "transactions_quarantine")
    if spark.catalog.tableExists(quarantine):
        reasons = (
            spark.table(quarantine)
            .groupBy("reject_reason")
            .agg(F.count(F.lit(1)).alias("rows"))
            .orderBy(F.desc("rows"))
        )
        target = cfg.curated_dir("quarantine_reasons")
        reasons.coalesce(1).write.mode("overwrite").parquet(str(target))
        row_count = reasons.count()
        counts["quarantine_reasons"] = row_count
        manifest["quarantine_reasons"] = {
            "source_table": quarantine,
            "path": str(target),
            "row_count": row_count,
            "columns": 2,
        }
        log.info("exported %-24s %9d rows -> %s", "quarantine_reasons", row_count, target)

    manifest_path = curated_root / "_export_manifest.json"
    manifest_path.write_text(
        json.dumps({"exported_at": datetime.now(UTC).isoformat(), "tables": manifest}, indent=2)
    )
    log.info("manifest: %s", manifest_path)
    return counts


if __name__ == "__main__":  # pragma: no cover
    run()
