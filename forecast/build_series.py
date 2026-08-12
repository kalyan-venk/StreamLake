"""Build the daily transaction-volume and fraud-rate series from the gold layer.

Reads the curated Parquet StreamLake already exports (data/curated/state_hourly_volume and
data/curated/category_hourly_fraud), rolls each up to one row per calendar day across the full
Jan 2019 to Dec 2020 span, and writes a single CSV that the backtest script and Tableau both read.

This does not need Spark. The curated exports are already plain Parquet on disk, and a daily
rollup of two files with ~628K and ~173K rows is a pandas-sized job. No PYTHONPATH or JAVA_HOME
needed to run this file on its own; `python forecast/build_series.py` is enough as long as the
project venv has pandas and pyarrow (both already project dependencies).
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parent.parent
CURATED = REPO_ROOT / "data" / "curated"
OUTPUT_DIR = Path(__file__).resolve().parent / "output"


def build_daily_series() -> pd.DataFrame:
    """Return one row per calendar day: total transaction volume and fraud rate.

    Cross-checked against each other: `state_hourly_volume` and `category_hourly_fraud` are two
    independent group-bys over the same silver table, so their daily txn totals should match
    exactly. The build asserts that rather than assuming it.
    """
    state_hourly = pd.read_parquet(
        CURATED / "state_hourly_volume", columns=["trans_hour_ts", "txns"]
    )
    category_hourly = pd.read_parquet(
        CURATED / "category_hourly_fraud", columns=["trans_hour_ts", "txns", "fraud_txns"]
    )

    volume_daily = (
        state_hourly.assign(day=state_hourly["trans_hour_ts"].dt.floor("D"))
        .groupby("day")["txns"]
        .sum()
        .rename("txn_volume")
    )

    fraud_daily = (
        category_hourly.assign(day=category_hourly["trans_hour_ts"].dt.floor("D"))
        .groupby("day")
        .agg(txns_check=("txns", "sum"), fraud_txns=("fraud_txns", "sum"))
    )

    merged = volume_daily.to_frame().join(fraud_daily, how="outer")
    mismatches = (merged["txn_volume"] - merged["txns_check"]).abs()
    if (mismatches > 0).any():
        bad = mismatches[mismatches > 0]
        raise AssertionError(
            f"state_hourly_volume and category_hourly_fraud disagree on daily txn count for "
            f"{len(bad)} day(s), largest gap {bad.max()}. They are two independent rollups of the "
            f"same silver table and must match exactly."
        )

    merged["fraud_rate"] = merged["fraud_txns"] / merged["txn_volume"]
    merged = merged.drop(columns="txns_check").sort_index()
    merged.index.name = "date"

    # Reindex onto the full calendar span rather than assume every day has rows. Verified against
    # the raw Sparkov CSVs directly (`grep -c "2020-02-29"` on both train and test files returns
    # 0): 2020-02-29 (leap day) has zero transactions in the *source*, not a pipeline artifact.
    # It gets filled with real zeros below, not dropped and not interpolated, because a forecast
    # backtest should see the same gap a live pipeline would.
    full_range = pd.date_range(merged.index.min(), merged.index.max(), freq="D")
    missing = full_range.difference(merged.index)
    if len(missing) > 0:
        print(f"note: {len(missing)} calendar day(s) had zero source rows: {list(missing)}")
    merged = merged.reindex(full_range)
    merged["txn_volume"] = merged["txn_volume"].fillna(0).astype(int)
    merged["fraud_txns"] = merged["fraud_txns"].fillna(0).astype(int)
    merged["fraud_rate"] = merged["fraud_rate"].fillna(0.0)
    merged.index.name = "date"

    return merged.reset_index()


def main() -> pd.DataFrame:
    df = build_daily_series()
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    out_path = OUTPUT_DIR / "daily_series.csv"
    df.to_csv(out_path, index=False)
    print(f"wrote {len(df)} daily rows -> {out_path}")
    print(df.describe(include="all"))
    return df


if __name__ == "__main__":
    main()
