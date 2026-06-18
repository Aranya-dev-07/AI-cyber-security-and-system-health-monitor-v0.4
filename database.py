"""
database.py
============
SQLite persistence layer for the System Health Monitor and Cybersecurity
Monitoring Platform.

Responsibilities:
    * Connection management (a fresh ``sqlite3.connect()`` per operation,
      used as a context manager so commits/rollbacks and closing are
      automatic - this is the safest pattern for SQLite when multiple
      threads (the monitoring thread and FastAPI's request threads) need
      to read/write concurrently, since no connection object is ever
      shared across threads).
    * Schema creation (``initialize_database``).
    * Data insertion (``insert_test_run``, ``insert_system_metrics``,
      ``insert_system_processes``).
    * Data retrieval (``get_latest_metrics``, ``get_latest_processes``,
      ``get_run_history``).
    * Bulk-loading the CSV files into the database at the end of a
      monitoring run (``sync_csv_to_database``).

ARCHITECTURE NOTE
------------------
``database.py`` imports only from ``config.py``:

    config.py  <-- database.py  <-- api.py  <-- main.py

COLUMN NAMING NOTE
--------------------
The SQLite table columns intentionally use different names than the dict
keys produced by ``config.py``'s collection functions:

    config.py dict key   ->   database.py column name
    ------------------        ------------------------
    net_sent_mb          ->   bytes_sent       (converted MB -> bytes)
    net_recv_mb          ->   bytes_received   (converted MB -> bytes)
    name                 ->   process_name
    status                ->   process_status
    duration_sec          ->   duration_seconds

The insert functions below accept the dicts exactly as produced by
``config.py`` and perform this mapping internally, so callers never need
to rename keys themselves.
"""

from __future__ import annotations

import csv
import logging
import os
import sqlite3
from contextlib import contextmanager
from datetime import datetime
from typing import Any, Dict, Generator, List, Optional

import config

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Connection management
# ---------------------------------------------------------------------------
@contextmanager
def get_connection() -> Generator[sqlite3.Connection, None, None]:
    """Yield a new SQLite connection scoped to a single operation.

    A brand-new connection is opened for every call (never shared across
    threads or operations), which is the safest concurrency model for
    SQLite when both a background monitoring thread and FastAPI's request
    threads may need database access at the same time.

    On successful exit, the transaction is committed. On any exception,
    the transaction is rolled back and the exception is re-raised. The
    connection is always closed.

    Yields:
        An open ``sqlite3.Connection`` with ``row_factory`` set to
        ``sqlite3.Row`` so result rows can be accessed by column name.
    """
    connection = sqlite3.connect(config.DB_PATH)
    connection.row_factory = sqlite3.Row
    try:
        yield connection
        connection.commit()
    except Exception:
        connection.rollback()
        logger.exception("Database transaction rolled back due to an error.")
        raise
    finally:
        connection.close()


# ---------------------------------------------------------------------------
# Schema creation
# ---------------------------------------------------------------------------
def initialize_database() -> None:
    """Create all required tables if they do not already exist.

    Creates three tables:
        * ``test_run``: one row per monitoring run.
        * ``system_metrics``: one row per collected system metric snapshot,
          linked to a ``test_run`` via ``run_id``.
        * ``system_processes``: one row per top-process snapshot entry,
          linked to a ``test_run`` via ``run_id``.

    Safe to call on every application startup; uses
    ``CREATE TABLE IF NOT EXISTS`` throughout.

    Raises:
        Never raises: errors are logged. Callers should treat a failed
        initialization as fatal for the application if it occurs, but this
        function itself will not crash the caller.
    """
    try:
        with get_connection() as conn:
            cursor = conn.cursor()

            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS test_run (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    start_time TEXT NOT NULL,
                    end_time TEXT,
                    duration_seconds REAL,
                    alert_count INTEGER NOT NULL DEFAULT 0
                )
                """
            )

            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS system_metrics (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    run_id INTEGER NOT NULL,
                    timestamp TEXT NOT NULL,
                    cpu_percent REAL NOT NULL,
                    ram_percent REAL NOT NULL,
                    disk_percent REAL NOT NULL,
                    bytes_sent REAL NOT NULL,
                    bytes_received REAL NOT NULL,
                    FOREIGN KEY (run_id) REFERENCES test_run (id)
                )
                """
            )

            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS system_processes (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    run_id INTEGER NOT NULL,
                    timestamp TEXT NOT NULL,
                    pid INTEGER NOT NULL,
                    process_name TEXT NOT NULL,
                    cpu_percent REAL NOT NULL,
                    memory_percent REAL NOT NULL,
                    process_status TEXT NOT NULL,
                    FOREIGN KEY (run_id) REFERENCES test_run (id)
                )
                """
            )

            # Helpful indexes for the "latest" / "history" query patterns.
            cursor.execute(
                "CREATE INDEX IF NOT EXISTS idx_system_metrics_run_id "
                "ON system_metrics (run_id)"
            )
            cursor.execute(
                "CREATE INDEX IF NOT EXISTS idx_system_processes_run_id "
                "ON system_processes (run_id)"
            )

        logger.info("Database initialized successfully at '%s'.", config.DB_PATH)

    except sqlite3.Error:
        logger.exception("Failed to initialize database schema.")


# ---------------------------------------------------------------------------
# Data insertion
# ---------------------------------------------------------------------------
def insert_test_run(
    start_time: datetime,
    end_time: Optional[datetime] = None,
    alert_count: int = 0,
) -> int:
    """Insert a new row into ``test_run`` and return its assigned ID.

    Args:
        start_time: When the monitoring run started.
        end_time: When the monitoring run ended, if known yet. May be
            ``None`` for a run that is still in progress.
        alert_count: Total alerts raised so far during the run.

    Returns:
        The ``id`` (run_id) assigned to the newly inserted row, or ``-1``
        if the insert failed.
    """
    try:
        duration_seconds: Optional[float] = None
        if end_time is not None:
            duration_seconds = max(0.0, (end_time - start_time).total_seconds())

        with get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                INSERT INTO test_run (start_time, end_time, duration_seconds, alert_count)
                VALUES (?, ?, ?, ?)
                """,
                (
                    start_time.isoformat(),
                    end_time.isoformat() if end_time else None,
                    duration_seconds,
                    alert_count,
                ),
            )
            run_id = cursor.lastrowid

        logger.info("Inserted test_run row with id=%d.", run_id)
        return run_id

    except sqlite3.Error:
        logger.exception("Failed to insert test_run row.")
        return -1


def update_test_run_end(run_id: int, end_time: datetime, alert_count: int) -> None:
    """Update an existing ``test_run`` row with its end time and final stats.

    Intended to be called by ``main.py`` when ``stop_monitoring()`` is
    invoked, to close out the run row created at start time.

    Args:
        run_id: The ``id`` of the ``test_run`` row to update.
        end_time: When the monitoring run ended.
        alert_count: Final total alert count for the run.

    Raises:
        Never raises: errors are logged.
    """
    try:
        with get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT start_time FROM test_run WHERE id = ?", (run_id,))
            row = cursor.fetchone()
            if row is None:
                logger.warning("update_test_run_end: no test_run found with id=%d.", run_id)
                return

            start_time = datetime.fromisoformat(row["start_time"])
            duration_seconds = max(0.0, (end_time - start_time).total_seconds())

            cursor.execute(
                """
                UPDATE test_run
                SET end_time = ?, duration_seconds = ?, alert_count = ?
                WHERE id = ?
                """,
                (end_time.isoformat(), duration_seconds, alert_count, run_id),
            )

        logger.info("Updated test_run id=%d with end_time and final alert_count.", run_id)

    except sqlite3.Error:
        logger.exception("Failed to update test_run id=%d.", run_id)


def insert_system_metrics(metric: Dict[str, Any], run_id: int) -> None:
    """Insert one system metric snapshot into ``system_metrics``.

    Accepts the dict exactly as produced by
    ``config.collect_system_metrics()`` (keys: ``timestamp``,
    ``cpu_percent``, ``ram_percent``, ``disk_percent``, ``net_sent_mb``,
    ``net_recv_mb``) and maps it onto the table's column names,
    converting megabytes back to bytes for storage.

    Args:
        metric: A metric dict as produced by
            :func:`config.collect_system_metrics`.
        run_id: The ``test_run.id`` this metric belongs to.

    Raises:
        Never raises: errors are logged; malformed/empty input is ignored.
    """
    if not metric:
        logger.warning("insert_system_metrics called with empty metric dict; skipping.")
        return

    try:
        bytes_sent = float(metric.get("net_sent_mb", 0.0)) * (1024 ** 2)
        bytes_received = float(metric.get("net_recv_mb", 0.0)) * (1024 ** 2)

        with get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                INSERT INTO system_metrics (
                    run_id, timestamp, cpu_percent, ram_percent,
                    disk_percent, bytes_sent, bytes_received
                )
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    run_id,
                    metric.get("timestamp", datetime.now().isoformat()),
                    metric.get("cpu_percent", 0.0),
                    metric.get("ram_percent", 0.0),
                    metric.get("disk_percent", 0.0),
                    bytes_sent,
                    bytes_received,
                ),
            )

        logger.debug("Inserted system_metrics row for run_id=%d.", run_id)

    except (sqlite3.Error, KeyError, TypeError, ValueError):
        logger.exception("Failed to insert system_metrics row for run_id=%d.", run_id)


def insert_system_processes(processes: List[Dict[str, Any]], run_id: int) -> None:
    """Insert top-process rows into ``system_processes``.

    Accepts the list of dicts exactly as produced by
    ``config.collect_process_metrics()`` (keys: ``timestamp``, ``pid``,
    ``name``, ``cpu_percent``, ``memory_percent``, ``status``) and maps
    each one onto the table's column names.

    Args:
        processes: A list of process dicts as produced by
            :func:`config.collect_process_metrics`.
        run_id: The ``test_run.id`` these process snapshots belong to.

    Raises:
        Never raises: errors are logged; an empty list is a no-op.
    """
    if not processes:
        return

    try:
        with get_connection() as conn:
            cursor = conn.cursor()
            cursor.executemany(
                """
                INSERT INTO system_processes (
                    run_id, timestamp, pid, process_name,
                    cpu_percent, memory_percent, process_status
                )
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        run_id,
                        proc.get("timestamp", datetime.now().isoformat()),
                        proc.get("pid", -1),
                        proc.get("name", "unknown"),
                        proc.get("cpu_percent", 0.0),
                        proc.get("memory_percent", 0.0),
                        proc.get("status", "unknown"),
                    )
                    for proc in processes
                ],
            )

        logger.debug(
            "Inserted %d system_processes rows for run_id=%d.", len(processes), run_id
        )

    except (sqlite3.Error, KeyError, TypeError, ValueError):
        logger.exception("Failed to insert system_processes rows for run_id=%d.", run_id)


# ---------------------------------------------------------------------------
# Data retrieval
# ---------------------------------------------------------------------------
def get_latest_metrics(limit: int = 1) -> List[Dict[str, Any]]:
    """Retrieve the most recent system metric rows.

    Args:
        limit: Maximum number of rows to return, most recent first.
            Defaults to ``1``.

    Returns:
        A list of dictionaries (one per row), ordered from most recent to
        least recent. Returns an empty list on error or if no rows exist.
    """
    try:
        with get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT id, run_id, timestamp, cpu_percent, ram_percent,
                       disk_percent, bytes_sent, bytes_received
                FROM system_metrics
                ORDER BY id DESC
                LIMIT ?
                """,
                (limit,),
            )
            rows = cursor.fetchall()

        return [dict(row) for row in rows]

    except sqlite3.Error:
        logger.exception("Failed to retrieve latest system metrics.")
        return []


def get_latest_processes(limit: int = 5) -> List[Dict[str, Any]]:
    """Retrieve the most recent process snapshot rows.

    Args:
        limit: Maximum number of rows to return, most recent first.
            Defaults to ``5`` (one full top-process snapshot).

    Returns:
        A list of dictionaries (one per row), ordered from most recent to
        least recent. Returns an empty list on error or if no rows exist.
    """
    try:
        with get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT id, run_id, timestamp, pid, process_name,
                       cpu_percent, memory_percent, process_status
                FROM system_processes
                ORDER BY id DESC
                LIMIT ?
                """,
                (limit,),
            )
            rows = cursor.fetchall()

        return [dict(row) for row in rows]

    except sqlite3.Error:
        logger.exception("Failed to retrieve latest system processes.")
        return []


def get_run_history(limit: int = 50) -> List[Dict[str, Any]]:
    """Retrieve past monitoring runs from ``test_run``.

    Args:
        limit: Maximum number of runs to return, most recent first.
            Defaults to ``50``.

    Returns:
        A list of dictionaries (one per run), ordered from most recent to
        least recent. Returns an empty list on error or if no rows exist.
    """
    try:
        with get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT id, start_time, end_time, duration_seconds, alert_count
                FROM test_run
                ORDER BY id DESC
                LIMIT ?
                """,
                (limit,),
            )
            rows = cursor.fetchall()

        return [dict(row) for row in rows]

    except sqlite3.Error:
        logger.exception("Failed to retrieve run history.")
        return []


# ---------------------------------------------------------------------------
# CSV -> Database bulk sync (end-of-run persistence)
# ---------------------------------------------------------------------------
def sync_csv_to_database(run_id: int) -> None:
    """Bulk-load the project's CSV files into the database for a given run.

    Reads ``system_metrics.csv`` and ``system_processes.csv`` in full and
    inserts every row into the corresponding table tagged with ``run_id``.
    Intended to be called once, when a monitoring run ends (in addition to
    - not instead of - the live per-cycle inserts performed during
    collection), to guarantee the database has a durable, complete copy of
    everything that was written to CSV during the run.

    Args:
        run_id: The ``test_run.id`` to associate with every synced row.

    Raises:
        Never raises: missing files or malformed rows are logged and
        skipped; this function makes a best-effort attempt and does not
        abort the caller's shutdown sequence.
    """
    _sync_metrics_csv(run_id)
    _sync_processes_csv(run_id)


def _sync_metrics_csv(run_id: int) -> None:
    """Read ``system_metrics.csv`` in full and insert every row as a metric."""
    path = config.CSV_METRICS_PATH
    if not os.path.isfile(path):
        logger.warning("sync_csv_to_database: '%s' not found; skipping.", path)
        return

    try:
        with open(path, mode="r", newline="", encoding="utf-8") as csv_file:
            reader = csv.DictReader(csv_file)
            rows = list(reader)

        with get_connection() as conn:
            cursor = conn.cursor()
            cursor.executemany(
                """
                INSERT INTO system_metrics (
                    run_id, timestamp, cpu_percent, ram_percent,
                    disk_percent, bytes_sent, bytes_received
                )
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        run_id,
                        row.get("timestamp", ""),
                        float(row.get("cpu_percent") or 0.0),
                        float(row.get("ram_percent") or 0.0),
                        float(row.get("disk_percent") or 0.0),
                        float(row.get("net_sent_mb") or 0.0) * (1024 ** 2),
                        float(row.get("net_recv_mb") or 0.0) * (1024 ** 2),
                    )
                    for row in rows
                ],
            )

        logger.info(
            "Synced %d rows from '%s' into system_metrics for run_id=%d.",
            len(rows), path, run_id,
        )

    except (OSError, sqlite3.Error, ValueError, KeyError):
        logger.exception("Failed to sync '%s' into the database.", path)


def _sync_processes_csv(run_id: int) -> None:
    """Read ``system_processes.csv`` in full and insert every row as a process."""
    path = config.CSV_PROCESSES_PATH
    if not os.path.isfile(path):
        logger.warning("sync_csv_to_database: '%s' not found; skipping.", path)
        return

    try:
        with open(path, mode="r", newline="", encoding="utf-8") as csv_file:
            reader = csv.DictReader(csv_file)
            rows = list(reader)

        with get_connection() as conn:
            cursor = conn.cursor()
            cursor.executemany(
                """
                INSERT INTO system_processes (
                    run_id, timestamp, pid, process_name,
                    cpu_percent, memory_percent, process_status
                )
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        run_id,
                        row.get("timestamp", ""),
                        int(float(row.get("pid") or -1)),
                        row.get("name", "unknown"),
                        float(row.get("cpu_percent") or 0.0),
                        float(row.get("memory_percent") or 0.0),
                        row.get("status", "unknown"),
                    )
                    for row in rows
                ],
            )

        logger.info(
            "Synced %d rows from '%s' into system_processes for run_id=%d.",
            len(rows), path, run_id,
        )

    except (OSError, sqlite3.Error, ValueError, KeyError):
        logger.exception("Failed to sync '%s' into the database.", path)