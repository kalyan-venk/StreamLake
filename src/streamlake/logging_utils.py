"""Consistent, greppable logging for every StreamLake job."""

from __future__ import annotations

import logging
import os
import sys

_CONFIGURED = False


def setup_logging(level: str | None = None) -> None:
    global _CONFIGURED
    if _CONFIGURED:
        return
    logging.basicConfig(
        level=(level or os.environ.get("STREAMLAKE_LOG_LEVEL", "INFO")).upper(),
        format="%(asctime)s %(levelname)-7s %(name)-28s %(message)s",
        datefmt="%H:%M:%S",
        stream=sys.stdout,
    )
    # Spark/py4j are extremely chatty at INFO; keep our own lines readable.
    for noisy in ("py4j", "pyspark", "org.apache.spark"):
        logging.getLogger(noisy).setLevel(logging.WARNING)
    _CONFIGURED = True


def get_logger(name: str) -> logging.Logger:
    setup_logging()
    return logging.getLogger(name)


def banner(logger: logging.Logger, text: str) -> None:
    logger.info("=" * 78)
    logger.info(text)
    logger.info("=" * 78)
