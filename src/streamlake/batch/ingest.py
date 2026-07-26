"""Step 1 — land the source data.

Fetch the bytes, verify them, write a manifest, stop. No parsing and no cleaning, so that a
corrupt or truncated download fails here — where the error is "the file is wrong" — rather than
three hops later where it is "the revenue number looks odd".
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
MIN_TRIPS_BYTES = 5 * 1024 * 1024  # a real month is ~50 MB; anything tiny is a failed fetch


def _download(url: str, target: Path) -> Path:
    target.parent.mkdir(parents=True, exist_ok=True)
    tmp = target.with_suffix(target.suffix + ".part")
    log.info("downloading %s -> %s", url, target.name)
    with requests.get(url, stream=True, timeout=300) as response:
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
    banner(log, f"INGEST | dataset={cfg.get('dataset.name')} month={cfg.month}")

    trips_url = str(cfg.require("dataset.trips_url")).format(month=cfg.month)
    zones_url = str(cfg.require("dataset.zones_url"))
    trips_path, zones_path = cfg.raw_trips_file(), cfg.raw_zones_file()

    for url, path in ((trips_url, trips_path), (zones_url, zones_path)):
        if force or not path.exists():
            _download(url, path)
        else:
            log.info("already present, skipping download: %s", path.name)

    if trips_path.stat().st_size < MIN_TRIPS_BYTES:
        raise RuntimeError(
            f"{trips_path} is only {trips_path.stat().st_size} bytes — the download failed"
        )

    manifest = {
        "dataset": cfg.get("dataset.name"),
        "month": cfg.month,
        "ingested_at": datetime.now(UTC).isoformat(),
        "files": {
            name: {
                "path": str(path),
                "bytes": path.stat().st_size,
                "sha256": _sha256(path),
            }
            for name, path in (("trips", trips_path), ("zones", zones_path))
        },
    }
    manifest_path = cfg.path("raw") / f"_manifest_{cfg.month}.json"
    manifest_path.write_text(json.dumps(manifest, indent=2))
    log.info("manifest written: %s", manifest_path)
    for name, meta in manifest["files"].items():
        log.info("  %-6s %8.1f MB  sha256=%s…", name, meta["bytes"] / 1e6, meta["sha256"][:12])

    return {"trips": str(trips_path), "zones": str(zones_path), "manifest": str(manifest_path)}


if __name__ == "__main__":  # pragma: no cover
    run()
