"""Step 1, land the source data.

Fetch the bytes, verify them, write a manifest, stop. No parsing and no cleaning, so that a
corrupt or truncated download fails here (where the error is "the file is wrong") rather than
three hops later where it is "the fraud rate looks odd".
"""

from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path

import requests

from streamlake.config import Config, get_config
from streamlake.logging_utils import banner, get_logger

log = get_logger(__name__)

CHUNK = 1 << 20
MIN_TRAIN_BYTES = 100 * 1024 * 1024  # real split is ~354 MB; a tiny file means a failed fetch
MIN_TEST_BYTES = 40 * 1024 * 1024  # the real test split is ~152 MB


def _download(url: str, target: Path) -> Path:
    target.parent.mkdir(parents=True, exist_ok=True)
    tmp = target.with_suffix(target.suffix + ".part")
    log.info("downloading %s -> %s", url, target.name)
    with requests.get(url, stream=True, timeout=600) as response:
        response.raise_for_status()
        with open(tmp, "wb") as fh:
            for chunk in response.iter_content(chunk_size=CHUNK):
                fh.write(chunk)
    # Rename only after a complete write, so a half-finished download can never be mistaken
    # for a good file by the next run.
    tmp.replace(target)
    return target


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(CHUNK), b""):
            digest.update(chunk)
    return digest.hexdigest()


def run(cfg: Config | None = None, *, force: bool = False) -> dict[str, str]:
    cfg = cfg or get_config()
    banner(log, f"INGEST | dataset={cfg.get('dataset.name')}")

    train_url = str(cfg.require("dataset.train_url"))
    test_url = str(cfg.require("dataset.test_url"))
    train_path, test_path = cfg.raw_train_file(), cfg.raw_test_file()

    for url, path in ((train_url, train_path), (test_url, test_path)):
        if force or not path.exists():
            _download(url, path)
        else:
            log.info("already present, skipping download: %s", path.name)

    if train_path.stat().st_size < MIN_TRAIN_BYTES:
        size = train_path.stat().st_size
        raise RuntimeError(f"{train_path} is only {size} bytes, the download failed")
    if test_path.stat().st_size < MIN_TEST_BYTES:
        size = test_path.stat().st_size
        raise RuntimeError(f"{test_path} is only {size} bytes, the download failed")

    category_ref = cfg.category_ref_file()
    if not category_ref.exists():
        raise RuntimeError(
            f"{category_ref} is missing, it ships with the repo (conf/reference/), not downloaded"
        )

    manifest = {
        "dataset": cfg.get("dataset.name"),
        "ingested_at": datetime.now(UTC).isoformat(),
        "files": {
            name: {
                "path": str(path),
                "bytes": path.stat().st_size,
                "sha256": _sha256(path),
            }
            for name, path in (("train", train_path), ("test", test_path))
        },
    }
    manifest_path = cfg.path("raw") / "_manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2))
    log.info("manifest written: %s", manifest_path)
    for name, meta in manifest["files"].items():
        log.info("  %-6s %8.1f MB  sha256=%s…", name, meta["bytes"] / 1e6, meta["sha256"][:12])

    return {
        "train": str(train_path),
        "test": str(test_path),
        "category_ref": str(category_ref),
        "manifest": str(manifest_path),
    }


if __name__ == "__main__":  # pragma: no cover
    run()
