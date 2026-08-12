"""Select the number of annual Fourier harmonics (K) for the SARIMA volume forecast, without ever
touching the real test holdout `backtest.py` reports against.

`backtest.py` hardcodes `FOURIER_K = 5`. Its own docstring describes how that number was chosen (an
internal validation split carved out of the training span), but until this script existed that
description had no runnable code behind it, only prose. This reproduces the selection end to end so
it can be re-run and checked independently, not just read.

Split (everything here is carved from the 675-day TRAINING span `backtest.py` fits on; the real
56-day test holdout, 2020-11-06 to 2020-12-31, is never read by this script):

  - inner-train: the 619 days before the inner-val window.
  - inner-val:   the 56 days immediately before the real holdout (2020-09-11 to 2020-11-05). This
    window predates the December volume ramp on purpose, so picking K here does not hand the model
    a peek at the exact pattern the real holdout needs.

For each K in {3,4,5,6,8,10}: fit SARIMA(1,1,1)x(1,1,1,7) + K annual Fourier harmonics on
inner-train, forecast the 56 inner-val days blind, score MAPE. The K with the lowest inner-val MAPE
is the one `backtest.py` should use (and does, as `FOURIER_K`) against the real holdout.

Run: `.venv/bin/python forecast/tune_fourier_k.py` (needs `forecast/output/daily_series.csv`, i.e.
`build_series.py` must have run first; `make forecast` runs both build_series.py and backtest.py
but not this script by default, see `make tune-fourier-k`).
"""

from __future__ import annotations

import sys
import warnings
from pathlib import Path

import numpy as np
import pandas as pd
from statsmodels.tools.sm_exceptions import ConvergenceWarning
from statsmodels.tsa.statespace.sarimax import SARIMAX

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

OUTPUT_DIR = Path(__file__).resolve().parent / "output"
SERIES_PATH = OUTPUT_DIR / "daily_series.csv"
SEASON = 7  # weekly seasonality on a daily series, same as backtest.py
REAL_HOLDOUT_DAYS = 56  # backtest.py's actual test holdout; carved out first and never touched
INNER_VAL_DAYS = 56  # the internal validation window, carved from training only
ANNUAL_PERIOD = 365.25
K_GRID = [3, 4, 5, 6, 8, 10]


def mape(actual: np.ndarray, predicted: np.ndarray) -> float:
    return float(np.mean(np.abs((actual - predicted) / actual)) * 100)


def fourier_terms(index: pd.DatetimeIndex, period: float, k: int, offset: int) -> pd.DataFrame:
    """K harmonics of a Fourier series over a continuous day counter, matching backtest.py."""
    t = np.arange(len(index)) + offset
    feats = {}
    for i in range(1, k + 1):
        feats[f"annual_sin{i}"] = np.sin(2 * np.pi * i * t / period)
        feats[f"annual_cos{i}"] = np.cos(2 * np.pi * i * t / period)
    return pd.DataFrame(feats, index=index)


def run() -> pd.DataFrame:
    df = pd.read_csv(SERIES_PATH, parse_dates=["date"]).set_index("date")
    series = df["txn_volume"].asfreq("D")

    # backtest.py's TRAINING span: everything except its real 56-day test holdout. That real
    # holdout is sliced off here and never referenced again in this script.
    training_span = series.iloc[:-REAL_HOLDOUT_DAYS]

    # Inner split, carved only from training_span.
    inner_train = training_span.iloc[:-INNER_VAL_DAYS]
    inner_val = training_span.iloc[-INNER_VAL_DAYS:]

    print(
        f"inner-train: {inner_train.index.min().date()} -> {inner_train.index.max().date()} "
        f"({len(inner_train)} days)"
    )
    print(
        f"inner-val:   {inner_val.index.min().date()} -> {inner_val.index.max().date()} "
        f"({len(inner_val)} days)"
    )
    print(f"(real test holdout, last {REAL_HOLDOUT_DAYS} days, is not read by this script)\n")

    rows = []
    for k in K_GRID:
        exog_train = fourier_terms(inner_train.index, ANNUAL_PERIOD, k, offset=0)
        exog_val = fourier_terms(inner_val.index, ANNUAL_PERIOD, k, offset=len(inner_train))

        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            model = SARIMAX(
                inner_train,
                exog=exog_train,
                order=(1, 1, 1),
                seasonal_order=(1, 1, 1, SEASON),
                enforce_stationarity=False,
                enforce_invertibility=False,
            )
            fit = model.fit(disp=False)
            converged = not any(issubclass(w.category, ConvergenceWarning) for w in caught)

        pred = fit.get_forecast(steps=INNER_VAL_DAYS, exog=exog_val).predicted_mean.to_numpy()
        val_mape = mape(inner_val.to_numpy(), pred)
        rows.append({"K": k, "val_mape_pct": round(val_mape, 1), "converged": converged})
        flag = "" if converged else "  <- convergence warning"
        print(f"K={k:<2d} val MAPE {val_mape:5.1f}%{flag}")

    result = pd.DataFrame(rows)
    best_row = result.loc[result["val_mape_pct"].idxmin()]
    best_k = int(best_row["K"])
    print(f"\nselected K={best_k} (lowest inner-val MAPE, {best_row['val_mape_pct']}%)")

    from forecast import backtest as backtest_module

    if best_k != backtest_module.FOURIER_K:
        print(
            f"\nWARNING: this sweep selected K={best_k}, but backtest.py hardcodes "
            f"FOURIER_K={backtest_module.FOURIER_K}. backtest.py's headline numbers no longer "
            f"match what this sweep would pick; FOURIER_K needs to be updated (or this sweep's "
            f"result needs to be reconciled) before the two are consistent."
        )
    else:
        print(f"\nmatches backtest.py's hardcoded FOURIER_K={backtest_module.FOURIER_K}.")

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    out_path = OUTPUT_DIR / "fourier_k_sweep.csv"
    result.to_csv(out_path, index=False)
    print(f"sweep table -> {out_path}")

    return result


if __name__ == "__main__":
    run()
