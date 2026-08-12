"""Export gold-layer CSV extracts for the Tableau Public workbook (see BUILD-SPEC.md, local-only).

Reads `data/curated/transactions` (the full 1,852,394-row silver export StreamLake already
writes on `make batch`/`make export`), not a re-run of Spark; this is a single pandas pass over
the same curated Parquet the warehouse loader reads. Every number below is a real aggregate over
the actual Sparkov-derived silver table, not sampled or invented.

Run: `.venv/bin/python tableau/export_extracts.py`. Needs `data/curated/transactions` to exist,
i.e. `make batch` (or at least `make export`) must have run first.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parent.parent
CURATED = REPO_ROOT / "data" / "curated"
OUTPUT_DIR = Path(__file__).resolve().parent

DAY_NAMES = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]


def main() -> None:
    cols = ["trans_time", "trans_date", "trans_hour", "state", "category", "amt", "is_fraud"]
    df = pd.read_parquet(CURATED / "transactions", columns=cols)
    print(f"loaded {len(df)} silver transactions")

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # 1. fraud rate over time, daily
    daily = (
        df.groupby("trans_date")
        .agg(txns=("is_fraud", "size"), fraud_txns=("is_fraud", "sum"), total_amt=("amt", "sum"))
        .reset_index()
        .sort_values("trans_date")
    )
    daily["fraud_rate"] = daily["fraud_txns"] / daily["txns"]
    daily["total_amt"] = daily["total_amt"].round(2)
    daily.to_csv(OUTPUT_DIR / "fraud_rate_over_time_daily.csv", index=False)
    print(f"fraud_rate_over_time_daily.csv: {len(daily)} rows")

    # 2. fraud by category
    by_category = (
        df.groupby("category")
        .agg(txns=("is_fraud", "size"), fraud_txns=("is_fraud", "sum"), total_amt=("amt", "sum"))
        .reset_index()
        .sort_values("txns", ascending=False)
    )
    by_category["fraud_rate"] = by_category["fraud_txns"] / by_category["txns"]
    by_category["total_amt"] = by_category["total_amt"].round(2)
    by_category.to_csv(OUTPUT_DIR / "fraud_by_category.csv", index=False)
    print(f"fraud_by_category.csv: {len(by_category)} rows")

    # 3. fraud by state (2-letter USPS codes, Tableau geocodes these directly as State/Province)
    by_state = (
        df.groupby("state")
        .agg(txns=("is_fraud", "size"), fraud_txns=("is_fraud", "sum"), total_amt=("amt", "sum"))
        .reset_index()
        .sort_values("txns", ascending=False)
    )
    by_state["fraud_rate"] = by_state["fraud_txns"] / by_state["txns"]
    by_state["total_amt"] = by_state["total_amt"].round(2)
    by_state.to_csv(OUTPUT_DIR / "fraud_by_state.csv", index=False)
    print(
        f"fraud_by_state.csv: {len(by_state)} rows, "
        f"{by_state['state'].nunique()} distinct states"
    )

    # 4. transaction volume by hour of day (0-23, summed across all 731 days)
    by_hour = (
        df.groupby("trans_hour")
        .agg(txns=("is_fraud", "size"), fraud_txns=("is_fraud", "sum"), avg_amt=("amt", "mean"))
        .reset_index()
        .sort_values("trans_hour")
    )
    by_hour["fraud_rate"] = by_hour["fraud_txns"] / by_hour["txns"]
    by_hour["avg_amt"] = by_hour["avg_amt"].round(3)
    by_hour.to_csv(OUTPUT_DIR / "volume_by_hour_of_day.csv", index=False)
    print(f"volume_by_hour_of_day.csv: {len(by_hour)} rows")

    # 5. transaction volume by day of week
    dow = pd.to_datetime(df["trans_date"]).dt.dayofweek
    by_dow = (
        df.assign(day_of_week=dow.map(dict(enumerate(DAY_NAMES))), dow_index=dow)
        .groupby(["dow_index", "day_of_week"])
        .agg(txns=("is_fraud", "size"), fraud_txns=("is_fraud", "sum"), avg_amt=("amt", "mean"))
        .reset_index()
        .sort_values("dow_index")
        .drop(columns="dow_index")
    )
    by_dow["fraud_rate"] = by_dow["fraud_txns"] / by_dow["txns"]
    by_dow["avg_amt"] = by_dow["avg_amt"].round(3)
    by_dow.to_csv(OUTPUT_DIR / "volume_by_day_of_week.csv", index=False)
    print(f"volume_by_day_of_week.csv: {len(by_dow)} rows")

    print("\nall 5 extracts written to tableau/")


if __name__ == "__main__":
    main()
