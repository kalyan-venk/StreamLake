"""Step 5 — export the curated layer out of the lake, ready for the warehouse.

The lakehouse is the system of record; the warehouse is a serving copy. This step writes the
handful of tables the warehouse actually needs as plain Parquet, plus a manifest recording row
counts and watermarks. The manifest is what the loader checks against after loading — if
Snowflake ends up with fewer rows than the lake exported, the load failed silently and we want
to know before dbt runs.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone

from streamlake.config import Config, get_config
from streamlake.logging_utils import banner, get_logger
from streamlake.spark import build_spark

log = get_logger(__name__)

# lake table -> (curated name, optional watermark column)
EXPORTS: dict[str, tuple[str, str, str | None]] = {
    "trips": ("silver", "trips", "pickup_ts"),
    "dim_zone": ("silver", "dim_zone", None),
    "daily_zone_kpis": ("gold", "daily_zone_kpis", "pickup_date"),
    "hourly_demand": ("gold", "hourly_demand", "pickup_hour_ts"),
    "payment_mix": ("gold", "payment_mix", "pickup_date"),
    "trip_metrics_1m": ("stream", "trip_metrics_1m", "window_end"),
}

# The streaming table only exists once Layer 2 has run; a batch-only checkout must still work.
OPTIONAL = {"trip_metrics_1m"}


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
            raise RuntimeError(f"expected table {identifier} does not exist — run the batch DAG")

        df = spark.table(identifier)
        target = cfg.curated_dir(name)
        # A handful of files, not 200: the warehouse loader reads these directly and small-file
        # overhead is the classic way a "fast" export becomes a slow load.
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
        log.info("exported %-16s %9d rows -> %s", name, row_count, target)

    # The quarantine table is exported as a *summary*, not row by row: the warehouse needs to
    # answer "what did we reject and why", not to store every rejected trip a second time.
    quarantine = cfg.table("silver", "trips_quarantine")
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
        log.info("exported %-16s %9d rows -> %s", "quarantine_reasons", row_count, target)

    manifest_path = curated_root / "_export_manifest.json"
    manifest_path.write_text(
        json.dumps(
            {"exported_at": datetime.now(timezone.utc).isoformat(), "tables": manifest}, indent=2
        )
    )
    log.info("manifest: %s", manifest_path)
    return counts


if __name__ == "__main__":  # pragma: no cover
    run()
