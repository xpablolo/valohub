"""Helper entrypoint for launching the analytical RQ worker.

Why it exists:
    RQ looks up job callables by importing their dotted path (e.g.
    ``jobs.analytical_report_job.run_analytical_report_job``). When the worker
    is launched from outside the project root, that module might not be on
    ``sys.path`` and the import fails with
    ``ValueError: Invalid attribute name: jobs.analytical_report_job...``.

    Running this script ensures the repository root is added to ``sys.path``
    before RQ starts, so the queue worker can always import our jobs package.

Usage:
    python rq_worker.py            # listens to the default queue
    python rq_worker.py queue_one queue_two
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Sequence

# Ensure the project root is in sys.path even when executed from elsewhere.
PROJECT_ROOT = Path(__file__).resolve().parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from rq import Connection, Worker  # noqa: E402  (import after sys.path tweak)

from services.analytical_jobs import get_redis_connection  # noqa: E402

DEFAULT_QUEUES: Sequence[str] = ("analytical-reports",)


def run_worker(queue_names: Sequence[str]) -> None:
    """Start an RQ worker bound to the provided queues."""
    redis_conn = get_redis_connection()
    with Connection(redis_conn):
        worker = Worker(list(queue_names))
        worker.work()


def parse_args(argv: Sequence[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Launch the ValoHub analytical report RQ worker."
    )
    parser.add_argument(
        "queues",
        nargs="*",
        help="The queues to listen on (defaults to analytical-reports).",
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> None:
    args = parse_args(argv or sys.argv[1:])
    queues = tuple(args.queues) if args.queues else DEFAULT_QUEUES
    run_worker(queues)


if __name__ == "__main__":
    main()
