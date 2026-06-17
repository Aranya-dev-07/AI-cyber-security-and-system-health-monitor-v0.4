"""
database.py
============

Database layer for the System Health Monitor project.

This module is responsible for:
    - Defining the SQLite database (system_monitor.db) and its schema using
      SQLAlchemy ORM (SQLAlchemy 2.x style).
    - Exposing a shared `database_engine` and `database_session` for use by
      other modules (main.py, api.py).
    - Providing ORM models for three tables: test_run, system_metrics, and
      system_processes.
    - Providing simple, safe save/query functions that other modules can
      call directly, without needing to know any SQLAlchemy internals.

Design notes:
    - Writes are committed immediately (no batching/deferred commits), so
      data is durable on disk as soon as it is received, per requirements.
    - All public functions use defensive exception handling: on failure they
      log the error, roll back the session, and return a safe value (None /
      empty list / False) rather than raising, so a single bad write does not
      crash the monitoring loop in main.py or a request in api.py.

Author: System Health Monitor Project
"""

import logging
from datetime import datetime
from typing import List, Optional

from sqlalchemy import create_engine, Column, Integer, Float, String, DateTime
from sqlalchemy.orm import declarative_base, sessionmaker, Session

# ---------------------------------------------------------------------------
# Logging configuration
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("database")


# ---------------------------------------------------------------------------
# Database engine and session setup
# ---------------------------------------------------------------------------
DATABASE_NAME: str = "system_monitor.db"
DATABASE_URL: str = f"sqlite:///{DATABASE_NAME}"

# `check_same_thread=False` allows the same SQLite connection to be used
# safely across multiple threads, which matters if main.py runs monitoring
# on a background thread while api.py serves requests on the main thread.
database_engine = create_engine(
    DATABASE_URL,
    connect_args={"check_same_thread": False},
    echo=False,
)

# Session factory bound to the engine. `database_session` is the shared
# session instance other modules can import directly, e.g.:
#     from database import database_session
#     database_session.query(SystemMetric).all()
DatabaseSessionFactory = sessionmaker(bind=database_engine, autoflush=False, autocommit=False)
database_session: Session = DatabaseSessionFactory()

# Base class for all ORM models.
Base = declarative_base()


# ---------------------------------------------------------------------------
# ORM Models
# ---------------------------------------------------------------------------
class TestRun(Base):
    """
    Represents a single monitoring session ("test run").

    Columns:
        id (int): Primary key, auto-incremented.
        start_time (datetime): When this monitoring session started.
        end_time (datetime): When this monitoring session ended (nullable
                              until the session is finished).
        total_alerts (int): Total number of alerts generated during the run.
    """
    __tablename__ = "test_run"

    id = Column(Integer, primary_key=True, autoincrement=True)
    start_time = Column(DateTime, nullable=False, default=datetime.utcnow)
    end_time = Column(DateTime, nullable=True)
    total_alerts = Column(Integer, nullable=False, default=0)

    def __repr__(self) -> str:
        return (
            f"<TestRun(id={self.id}, start_time={self.start_time}, "
            f"end_time={self.end_time}, total_alerts={self.total_alerts})>"
        )


class SystemMetric(Base):
    """
    Represents a single snapshot of system-wide metrics.

    Columns:
        id (int): Primary key, auto-incremented.
        timestamp (datetime): When this metric snapshot was taken.
        cpu_usage (float): CPU usage percent at time of snapshot.
        ram_usage (float): RAM usage percent at time of snapshot.
        disk_usage (float): Disk usage percent at time of snapshot.
        network_sent (float): Cumulative/measured network bytes sent.
        network_received (float): Cumulative/measured network bytes received.
        system_uptime (float): System uptime in seconds at time of snapshot.
        running_processes (int): Total number of running processes.
    """
    __tablename__ = "system_metrics"

    id = Column(Integer, primary_key=True, autoincrement=True)
    timestamp = Column(DateTime, nullable=False, default=datetime.utcnow)
    cpu_usage = Column(Float, nullable=False, default=0.0)
    ram_usage = Column(Float, nullable=False, default=0.0)
    disk_usage = Column(Float, nullable=False, default=0.0)
    network_sent = Column(Float, nullable=False, default=0.0)
    network_received = Column(Float, nullable=False, default=0.0)
    system_uptime = Column(Float, nullable=False, default=0.0)
    running_processes = Column(Integer, nullable=False, default=0)

    def __repr__(self) -> str:
        return (
            f"<SystemMetric(id={self.id}, timestamp={self.timestamp}, "
            f"cpu_usage={self.cpu_usage}, ram_usage={self.ram_usage})>"
        )


class SystemProcess(Base):
    """
    Represents a single process snapshot record (e.g. one of the "top N"
    processes collected during a monitoring cycle).

    Columns:
        id (int): Primary key, auto-incremented.
        timestamp (datetime): When this process snapshot was taken.
        process_name (str): Name of the process.
        pid (int): Process ID.
        cpu_usage (float): CPU usage percent for this process.
        ram_usage (float): RAM usage percent for this process.
    """
    __tablename__ = "system_processes"

    id = Column(Integer, primary_key=True, autoincrement=True)
    timestamp = Column(DateTime, nullable=False, default=datetime.utcnow)
    process_name = Column(String(255), nullable=False, default="unknown")
    pid = Column(Integer, nullable=True)
    cpu_usage = Column(Float, nullable=False, default=0.0)
    ram_usage = Column(Float, nullable=False, default=0.0)

    def __repr__(self) -> str:
        return (
            f"<SystemProcess(id={self.id}, process_name={self.process_name}, "
            f"pid={self.pid}, cpu_usage={self.cpu_usage})>"
        )


# ---------------------------------------------------------------------------
# Database initialization
# ---------------------------------------------------------------------------
def initialize_database() -> bool:
    """
    Create all tables (test_run, system_metrics, system_processes) in the
    SQLite database if they do not already exist.

    This is safe to call multiple times; `create_all` only creates tables
    that are missing and leaves existing ones untouched.

    Returns:
        bool: True if initialization succeeded, False otherwise.
    """
    try:
        Base.metadata.create_all(bind=database_engine)
        logger.info("Database initialized successfully at '%s'.", DATABASE_NAME)
        return True
    except Exception as exc:
        logger.error("Failed to initialize database: %s", exc)
        return False


# ---------------------------------------------------------------------------
# Save functions (writes commit immediately)
# ---------------------------------------------------------------------------
def save_test_run(
    start_time: Optional[datetime] = None,
    end_time: Optional[datetime] = None,
    total_alerts: int = 0,
    run_id: Optional[int] = None,
) -> Optional[int]:
    """
    Create or update a test_run record and commit immediately.

    If `run_id` is provided, the existing TestRun with that id is updated
    (useful for setting `end_time`/`total_alerts` once a session finishes).
    Otherwise, a new TestRun row is created.

    Args:
        start_time (Optional[datetime]): Session start time. Defaults to
                                          now (UTC) if creating a new run and
                                          not provided.
        end_time (Optional[datetime]): Session end time, if known.
        total_alerts (int): Total alerts generated so far in this run.
        run_id (Optional[int]): If provided, update this existing run
                                 instead of creating a new one.

    Returns:
        Optional[int]: The id of the created/updated TestRun, or None on
                        failure.
    """
    try:
        if run_id is not None:
            test_run = database_session.get(TestRun, run_id)
            if test_run is None:
                logger.warning("save_test_run: no TestRun found with id=%s; creating new.", run_id)
                test_run = TestRun()
                database_session.add(test_run)
        else:
            test_run = TestRun()
            database_session.add(test_run)

        if start_time is not None:
            test_run.start_time = start_time
        elif test_run.start_time is None:
            test_run.start_time = datetime.utcnow()

        if end_time is not None:
            test_run.end_time = end_time

        test_run.total_alerts = total_alerts

        database_session.commit()
        logger.info("Saved test_run (id=%s).", test_run.id)
        return test_run.id

    except Exception as exc:
        logger.error("Failed to save test_run: %s", exc)
        database_session.rollback()
        return None


def save_system_metric(
    cpu_usage: float,
    ram_usage: float,
    disk_usage: float,
    network_sent: float,
    network_received: float,
    system_uptime: float,
    running_processes: int,
    timestamp: Optional[datetime] = None,
) -> Optional[int]:
    """
    Insert a new system_metrics row and commit immediately.

    Args:
        cpu_usage (float): CPU usage percent.
        ram_usage (float): RAM usage percent.
        disk_usage (float): Disk usage percent.
        network_sent (float): Network bytes sent (or bytes/sec, depending on
                               how the caller measures it).
        network_received (float): Network bytes received (or bytes/sec).
        system_uptime (float): System uptime in seconds.
        running_processes (int): Total number of running processes.
        timestamp (Optional[datetime]): When the snapshot was taken.
                                         Defaults to now (UTC).

    Returns:
        Optional[int]: The id of the newly created row, or None on failure.
    """
    try:
        metric = SystemMetric(
            timestamp=timestamp or datetime.utcnow(),
            cpu_usage=cpu_usage,
            ram_usage=ram_usage,
            disk_usage=disk_usage,
            network_sent=network_sent,
            network_received=network_received,
            system_uptime=system_uptime,
            running_processes=running_processes,
        )
        database_session.add(metric)
        database_session.commit()
        logger.info("Saved system_metrics row (id=%s).", metric.id)
        return metric.id

    except Exception as exc:
        logger.error("Failed to save system_metric: %s", exc)
        database_session.rollback()
        return None


def save_system_process(
    process_name: str,
    pid: Optional[int],
    cpu_usage: float,
    ram_usage: float,
    timestamp: Optional[datetime] = None,
) -> Optional[int]:
    """
    Insert a new system_processes row and commit immediately.

    Args:
        process_name (str): Name of the process.
        pid (Optional[int]): Process ID.
        cpu_usage (float): CPU usage percent for this process.
        ram_usage (float): RAM usage percent for this process.
        timestamp (Optional[datetime]): When the snapshot was taken.
                                         Defaults to now (UTC).

    Returns:
        Optional[int]: The id of the newly created row, or None on failure.
    """
    try:
        process_row = SystemProcess(
            timestamp=timestamp or datetime.utcnow(),
            process_name=process_name or "unknown",
            pid=pid,
            cpu_usage=cpu_usage,
            ram_usage=ram_usage,
        )
        database_session.add(process_row)
        database_session.commit()
        logger.info("Saved system_processes row (id=%s, name=%s).", process_row.id, process_row.process_name)
        return process_row.id

    except Exception as exc:
        logger.error("Failed to save system_process: %s", exc)
        database_session.rollback()
        return None


def save_system_processes_bulk(processes: List[dict]) -> int:
    """
    Convenience helper to save multiple process records (e.g. the "top 5"
    processes from a single monitoring cycle) in one commit.

    Each dict in `processes` is expected to have keys compatible with
    `save_system_process`'s fields: process_name, pid, cpu_usage_percent
    (or cpu_usage), ram_usage_percent (or ram_usage), and optionally
    timestamp.

    Args:
        processes (List[dict]): List of process info dictionaries.

    Returns:
        int: Number of rows successfully saved.
    """
    saved_count = 0
    try:
        for proc in processes:
            row = SystemProcess(
                timestamp=proc.get("timestamp", datetime.utcnow()),
                process_name=proc.get("process_name", "unknown"),
                pid=proc.get("pid"),
                cpu_usage=proc.get("cpu_usage_percent", proc.get("cpu_usage", 0.0)),
                ram_usage=proc.get("ram_usage_percent", proc.get("ram_usage", 0.0)),
            )
            database_session.add(row)
            saved_count += 1

        database_session.commit()
        logger.info("Saved %d system_processes rows in bulk.", saved_count)
        return saved_count

    except Exception as exc:
        logger.error("Failed to save system_processes in bulk: %s", exc)
        database_session.rollback()
        return 0


# ---------------------------------------------------------------------------
# Query functions
# ---------------------------------------------------------------------------
def get_latest_metrics(limit: int = 10) -> List[SystemMetric]:
    """
    Retrieve the most recent system_metrics rows, ordered newest first.

    Args:
        limit (int): Maximum number of rows to return. Defaults to 10.

    Returns:
        List[SystemMetric]: List of SystemMetric ORM objects. Returns an
                             empty list on failure or if no data exists.
    """
    try:
        results = (
            database_session.query(SystemMetric)
            .order_by(SystemMetric.timestamp.desc())
            .limit(limit)
            .all()
        )
        return results
    except Exception as exc:
        logger.error("Failed to fetch latest metrics: %s", exc)
        return []


def get_latest_processes(limit: int = 10) -> List[SystemProcess]:
    """
    Retrieve the most recent system_processes rows, ordered newest first.

    Args:
        limit (int): Maximum number of rows to return. Defaults to 10.

    Returns:
        List[SystemProcess]: List of SystemProcess ORM objects. Returns an
                              empty list on failure or if no data exists.
    """
    try:
        results = (
            database_session.query(SystemProcess)
            .order_by(SystemProcess.timestamp.desc())
            .limit(limit)
            .all()
        )
        return results
    except Exception as exc:
        logger.error("Failed to fetch latest processes: %s", exc)
        return []


def get_test_runs(limit: int = 10) -> List[TestRun]:
    """
    Retrieve the most recent test_run rows, ordered newest first.

    Args:
        limit (int): Maximum number of rows to return. Defaults to 10.

    Returns:
        List[TestRun]: List of TestRun ORM objects. Returns an empty list
                        on failure or if no data exists.
    """
    try:
        results = (
            database_session.query(TestRun)
            .order_by(TestRun.start_time.desc())
            .limit(limit)
            .all()
        )
        return results
    except Exception as exc:
        logger.error("Failed to fetch test runs: %s", exc)
        return []


# ---------------------------------------------------------------------------
# Manual / standalone test entry point
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    # Allows quick manual testing of this module in isolation, e.g.:
    #     python database.py
    logger.info("Running database.py in standalone test mode...")

    initialize_database()

    run_id = save_test_run(start_time=datetime.utcnow(), total_alerts=0)
    print("Created test_run with id:", run_id)

    metric_id = save_system_metric(
        cpu_usage=12.5,
        ram_usage=45.0,
        disk_usage=60.2,
        network_sent=1024.0,
        network_received=2048.0,
        system_uptime=3600.0,
        running_processes=120,
    )
    print("Created system_metrics row with id:", metric_id)

    process_id = save_system_process(
        process_name="python3",
        pid=1234,
        cpu_usage=5.5,
        ram_usage=2.1,
    )
    print("Created system_processes row with id:", process_id)

    print("Latest metrics:", get_latest_metrics(limit=5))
    print("Latest processes:", get_latest_processes(limit=5))
    print("Test runs:", get_test_runs(limit=5))