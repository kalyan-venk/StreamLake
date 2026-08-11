"""Build the small, deterministic Sparkov sample CI runs the batch pipeline against.

The full download is ~506 MB and the real contracts' row-count bounds assume something close to
its true size (1.85M rows), neither of which belongs in a CI job. This script takes a fixed
stride through each already-downloaded source file (run `make ingest` first) and writes a few
thousand rows to `tests/fixtures/ci_sample/`, which *is* committed, unlike everything under
`data/`.

Stride, not head/tail: both source files are sorted by `trans_date_trans_time`, so a fixed stride
spreads the sample across the whole 2019-2020 period instead of clustering on day one. The sample
is validated against `conf/contracts-ci/`, not `conf/contracts/`, since the real contracts' row
counts are calibrated to the full dataset, not a sample of it.

Re-run this and commit the result only if the sample needs to change (a different stride, a
different source). CI itself never runs this script, it reads the committed output.
"""

from __future__ import annotations

import csv
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
TRAIN_SRC = REPO_ROOT / "data" / "raw" / "credit_card_transaction_train.csv"
TEST_SRC = REPO_ROOT / "data" / "raw" / "credit_card_transaction_test.csv"
TRAIN_OUT = REPO_ROOT / "tests" / "fixtures" / "ci_sample" / "credit_card_transaction_train.csv"
TEST_OUT = REPO_ROOT / "tests" / "fixtures" / "ci_sample" / "credit_card_transaction_test.csv"
STRIDE = 430


def _sample(src: Path, out: Path, stride: int) -> int:
    out.parent.mkdir(parents=True, exist_ok=True)
    with open(src, newline="") as fh_in, open(out, "w", newline="") as fh_out:
        reader = csv.reader(fh_in)
        writer = csv.writer(fh_out)
        writer.writerow(next(reader))  # header
        n = 0
        for i, row in enumerate(reader):
            if i % stride == 0:
                writer.writerow(row)
                n += 1
    return n


if __name__ == "__main__":
    train_n = _sample(TRAIN_SRC, TRAIN_OUT, STRIDE)
    test_n = _sample(TEST_SRC, TEST_OUT, STRIDE)
    print(f"train sample: {train_n} rows -> {TRAIN_OUT}")
    print(f"test sample:  {test_n} rows -> {TEST_OUT}")
    print(f"total: {train_n + test_n} rows")
