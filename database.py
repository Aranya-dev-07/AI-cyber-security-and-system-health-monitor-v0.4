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
        run_number (int): The human-facing sequential run number for this
                           session (e.g. 1, 2, 3...), as tracked by
                           config.py's persistent run counter. Unique.
        start_time (datetime): When this monitoring session started.
        end_time (datetime): When this monitoring session ended (nullable
                              until the session is finished).
        total_alerts (int): Total number of alerts generated during the run.

        The following summary statistics are filled in once the run ends
        (via save_run_summary()), mirroring what config.py's
        generate_summary_report() computes. They are nullable because they
        are not known until the run completes.
        total_runtime_seconds (float): Total wall-clock duration of the run.
        average_cpu_usage (float): Mean CPU usage percent across the run.
        peak_cpu_usage (float): Maximum CPU usage percent observed.
        average_ram_usage (float): Mean RAM usage percent across the run.
        peak_ram_usage (float): Maximum RAM usage percent observed.
        average_disk_usage (float): Mean disk usage percent across the run.
        peak_disk_usage (float): Maximum disk usage percent observed.
        cpu_alerts (int): Number of CPU-threshold alerts during the run.
        ram_alerts (int): Number of RAM-threshold alerts during the run.
        network_alerts (int): Number of network-threshold alerts during the run.
        total_samples_collected (int): Number of metric snapshots collected.
    """
    __tablename__ = "test_run"

    id = Column(Integer, primary_key=True, autoincrement=True)
    run_number = Column(Integer, nullable=True, unique=True, index=True)
    start_time = Column(DateTime, nullable=False, default=datetime.utcnow)
    end_time = Column(DateTime, nullable=True)
    total_alerts = Column(Integer, nullable=False, default=0)

    total_runtime_seconds = Column(Float, nullable=True)
    average_cpu_usage = Column(Float, nullable=True)
    peak_cpu_usage = Column(Float, nullable=True)
    average_ram_usage = Column(Float, nullable=True)
    peak_ram_usage = Column(Float, nullable=True)
    average_disk_usage = Column(Float, nullable=True)
    peak_disk_usage = Column(Float, nullable=True)
    cpu_alerts = Column(Integer, nullable=True, default=0)
    ram_alerts = Column(Integer, nullable=True, default=0)
    network_alerts = Column(Integer, nullable=True, default=0)
    total_samples_collected = Column(Integer, nullable=True, default=0)

    def __repr__(self) -> str:
        return (
            f"<TestRun(id={self.id}, run_number={self.run_number}, "
            f"start_time={self.start_time}, end_time={self.end_time}, "
            f"total_alerts={self.total_alerts})>"
        )


class SystemMetric(Base):
    """
    Represents a single snapshot of system-wide metrics.

    Columns:
        id (int): Primary key, auto-incremented.
        run_number (int): Which monitoring run this snapshot belongs to.
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
    run_number = Column(Integer, nullable=True, index=True)
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
            f"<SystemMetric(id={self.id}, run_number={self.run_number}, "
            f"timestamp={self.timestamp}, cpu_usage={self.cpu_usage}, "
            f"ram_usage={self.ram_usage})>"
        )


class SystemProcess(Base):
    """
    Represents a single process snapshot record (e.g. one of the "top N"
    processes collected during a monitoring cycle).

    Columns:
        id (int): Primary key, auto-incremented.
        run_number (int): Which monitoring run this snapshot belongs to.
        timestamp (datetime): When this process snapshot was taken.
        process_name (str): Name of the process.
        pid (int): Process ID.
        cpu_usage (float): CPU usage percent for this process.
        ram_usage (float): RAM usage percent for this process.
    """
    __tablename__ = "system_processes"

    id = Column(Integer, primary_key=True, autoincrement=True)
    run_number = Column(Integer, nullable=True, index=True)
    timestamp = Column(DateTime, nullable=False, default=datetime.utcnow)
    process_name = Column(String(255), nullable=False, default="unknown")
    pid = Column(Integer, nullable=True)
    cpu_usage = Column(Float, nullable=False, default=0.0)
    ram_usage = Column(Float, nullable=False, default=0.0)

    def __repr__(self) -> str:
        return (
            f"<SystemProcess(id={self.id}, run_number={self.run_number}, "
            f"process_name={self.process_name}, pid={self.pid}, "
            f"cpu_usage={self.cpu_usage})>"
        )


class Alert(Base):
    """
    Represents a single alert generated by the alert engine in config.py
    (a CPU/RAM/Network threshold breach).

    Columns:
        id (int): Primary key, auto-incremented.
        run_number (int): Which monitoring run this alert belongs to.
        timestamp (datetime): When the alert was generated.
        alert_type (str): One of "CPU", "RAM", or "NETWORK".
        value (float): The measured value that triggered the alert.
        threshold (float): The threshold that was exceeded.
    """
    __tablename__ = "alerts"

    id = Column(Integer, primary_key=True, autoincrement=True)
    run_number = Column(Integer, nullable=True, index=True)
    timestamp = Column(DateTime, nullable=False, default=datetime.utcnow)
    alert_type = Column(String(50), nullable=False, default="UNKNOWN")
    value = Column(Float, nullable=False, default=0.0)
    threshold = Column(Float, nullable=False, default=0.0)

    def __repr__(self) -> str:
        return (
            f"<Alert(id={self.id}, run_number={self.run_number}, "
            f"alert_type={self.alert_type}, value={self.value}, "
            f"threshold={self.threshold})>"
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
    run_number: Optional[int] = None,
) -> Optional[int]:
    """
    Create or update a test_run record and commit immediately.

    Lookup precedence for finding an existing row to update:
        1. `run_id` (the TestRun table's own primary key), if provided.
        2. `run_number` (the human-facing sequential run number), if an
           existing row with that run_number is found.
    If neither resolves to an existing row, a new TestRun is created.

    Args:
        start_time (Optional[datetime]): Session start time. Defaults to
                                          now (UTC) if creating a new run and
                                          not provided.
        end_time (Optional[datetime]): Session end time, if known.
        total_alerts (int): Total alerts generated so far in this run.
        run_id (Optional[int]): If provided, update this existing row
                                 (matched by primary key) instead of
                                 creating a new one.
        run_number (Optional[int]): The sequential run number (from
                                     config.py's persistent counter) to
                                     tag this row with, and/or to look up
                                     an existing row by.

    Returns:
        Optional[int]: The id of the created/updated TestRun, or None on
                        failure.
    """
    try:
        test_run: Optional[TestRun] = None

        if run_id is not None:
            test_run = database_session.get(TestRun, run_id)
            if test_run is None:
                logger.warning("save_test_run: no TestRun found with id=%s; creating new.", run_id)

        if test_run is None and run_number is not None:
            test_run = (
                database_session.query(TestRun)
                .filter(TestRun.run_number == run_number)
                .first()
            )

        if test_run is None:
            test_run = TestRun()
            database_session.add(test_run)

        if run_number is not None:
            test_run.run_number = run_number

        if start_time is not None:
            test_run.start_time = start_time
        elif test_run.start_time is None:
            test_run.start_time = datetime.utcnow()

        if end_time is not None:
            test_run.end_time = end_time

        test_run.total_alerts = total_alerts

        database_session.commit()
        logger.info("Saved test_run (id=%s, run_number=%s).", test_run.id, test_run.run_number)
        return test_run.id

    except Exception as exc:
        logger.error("Failed to save test_run: %s", exc)
        database_session.rollback()
        return None


def save_run_summary(
    run_number: int,
    total_runtime_seconds: float,
    average_cpu_usage: float,
    peak_cpu_usage: float,
    average_ram_usage: float,
    peak_ram_usage: float,
    average_disk_usage: float,
    peak_disk_usage: float,
    cpu_alerts: int,
    ram_alerts: int,
    network_alerts: int,
    total_samples_collected: int,
    total_alerts: Optional[int] = None,
    end_time: Optional[datetime] = None,
) -> Optional[int]:
    """
    Persist the full end-of-run summary statistics for a given run onto its
    TestRun row, creating the row if it does not already exist.

    This is the database-backed counterpart to config.py's
    generate_summary_report(): everything that gets written to
    system_report.csv and printed to the terminal is also saved here, so
    api.py can serve a comprehensive per-run summary regardless of which
    process is running.

    Args:
        run_number (int): The run this summary belongs to. Used to find
                           (or create) the corresponding TestRun row.
        total_runtime_seconds (float): Total wall-clock duration of the run.
        average_cpu_usage (float): Mean CPU usage percent across the run.
        peak_cpu_usage (float): Maximum CPU usage percent observed.
        average_ram_usage (float): Mean RAM usage percent across the run.
        peak_ram_usage (float): Maximum RAM usage percent observed.
        average_disk_usage (float): Mean disk usage percent across the run.
        peak_disk_usage (float): Maximum disk usage percent observed.
        cpu_alerts (int): Number of CPU-threshold alerts during the run.
        ram_alerts (int): Number of RAM-threshold alerts during the run.
        network_alerts (int): Number of network-threshold alerts during the run.
        total_samples_collected (int): Number of metric snapshots collected.
        total_alerts (Optional[int]): Total alert count. Defaults to the sum
                                       of cpu/ram/network alerts if omitted.
        end_time (Optional[datetime]): When the run ended. Defaults to now
                                        (UTC) if not provided.

    Returns:
        Optional[int]: The id of the updated/created TestRun row, or None
                        on failure.
    """
    try:
        test_run = (
            database_session.query(TestRun)
            .filter(TestRun.run_number == run_number)
            .first()
        )

        if test_run is None:
            logger.warning(
                "save_run_summary: no TestRun found for run_number=%s; creating new.", run_number
            )
            test_run = TestRun(run_number=run_number)
            database_session.add(test_run)

        test_run.end_time = end_time or datetime.utcnow()
        test_run.total_runtime_seconds = total_runtime_seconds
        test_run.average_cpu_usage = average_cpu_usage
        test_run.peak_cpu_usage = peak_cpu_usage
        test_run.average_ram_usage = average_ram_usage
        test_run.peak_ram_usage = peak_ram_usage
        test_run.average_disk_usage = average_disk_usage
        test_run.peak_disk_usage = peak_disk_usage
        test_run.cpu_alerts = cpu_alerts
        test_run.ram_alerts = ram_alerts
        test_run.network_alerts = network_alerts
        test_run.total_samples_collected = total_samples_collected
        test_run.total_alerts = (
            total_alerts if total_alerts is not None else (cpu_alerts + ram_alerts + network_alerts)
        )

        database_session.commit()
        logger.info("Saved run summary for run_number=%s (TestRun id=%s).", run_number, test_run.id)
        return test_run.id

    except Exception as exc:
        logger.error("Failed to save run summary for run_number=%s: %s", run_number, exc)
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
    run_number: Optional[int] = None,
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
        run_number (Optional[int]): Which monitoring run this snapshot
                                     belongs to.

    Returns:
        Optional[int]: The id of the newly created row, or None on failure.
    """
    try:
        metric = SystemMetric(
            run_number=run_number,
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
        logger.info("Saved system_metrics row (id=%s, run_number=%s).", metric.id, run_number)
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
    run_number: Optional[int] = None,
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
        run_number (Optional[int]): Which monitoring run this snapshot
                                     belongs to.

    Returns:
        Optional[int]: The id of the newly created row, or None on failure.
    """
    try:
        process_row = SystemProcess(
            run_number=run_number,
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


def save_system_processes_bulk(processes: List[dict], run_number: Optional[int] = None) -> int:
    """
    Convenience helper to save multiple process records (e.g. the "top 5"
    processes from a single monitoring cycle) in one commit.

    Each dict in `processes` is expected to have keys compatible with
    `save_system_process`'s fields: process_name, pid, cpu_usage_percent
    (or cpu_usage), ram_usage_percent (or ram_usage), and optionally
    timestamp and run_number.

    Args:
        processes (List[dict]): List of process info dictionaries.
        run_number (Optional[int]): Run number to tag every row with, used
                                     as a fallback when a given process dict
                                     does not already specify its own
                                     "run_number" key.

    Returns:
        int: Number of rows successfully saved.
    """
    saved_count = 0
    try:
        for proc in processes:
            row = SystemProcess(
                run_number=proc.get("run_number", run_number),
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


def save_alert(
    alert_type: str,
    value: float,
    threshold: float,
    timestamp: Optional[datetime] = None,
    run_number: Optional[int] = None,
) -> Optional[int]:
    """
    Insert a new alert row and commit immediately.

    Args:
        alert_type (str): One of "CPU", "RAM", or "NETWORK".
        value (float): The measured value that triggered the alert.
        threshold (float): The threshold that was exceeded.
        timestamp (Optional[datetime]): When the alert occurred. Defaults
                                         to now (UTC).
        run_number (Optional[int]): Which monitoring run this alert
                                     belongs to.

    Returns:
        Optional[int]: The id of the newly created row, or None on failure.
    """
    try:
        alert_row = Alert(
            run_number=run_number,
            timestamp=timestamp or datetime.utcnow(),
            alert_type=alert_type or "UNKNOWN",
            value=value,
            threshold=threshold,
        )
        database_session.add(alert_row)
        database_session.commit()
        logger.info("Saved alert row (id=%s, type=%s, run_number=%s).", alert_row.id, alert_type, run_number)
        return alert_row.id

    except Exception as exc:
        logger.error("Failed to save alert: %s", exc)
        database_session.rollback()
        return None


def save_alerts_bulk(alerts: List[dict], run_number: Optional[int] = None) -> int:
    """
    Convenience helper to save multiple alert records in one commit.

    Each dict in `alerts` is expected to have keys: "type", "value",
    "threshold", and optionally "timestamp".

    Args:
        alerts (List[dict]): List of alert dictionaries, as produced by
                              config.py's check_alerts().
        run_number (Optional[int]): Run number to tag every row with.

    Returns:
        int: Number of rows successfully saved.
    """
    saved_count = 0
    try:
        for alert in alerts:
            row = Alert(
                run_number=run_number,
                timestamp=alert.get("timestamp", datetime.utcnow()),
                alert_type=alert.get("type", "UNKNOWN"),
                value=alert.get("value", 0.0),
                threshold=alert.get("threshold", 0.0),
            )
            database_session.add(row)
            saved_count += 1

        database_session.commit()
        logger.info("Saved %d alert rows in bulk.", saved_count)
        return saved_count

    except Exception as exc:
        logger.error("Failed to save alerts in bulk: %s", exc)
        database_session.rollback()
        return 0


# ---------------------------------------------------------------------------
# Query functions
# ---------------------------------------------------------------------------
def get_latest_metrics(limit: int = 10, run_number: Optional[int] = None) -> List[SystemMetric]:
    """
    Retrieve the most recent system_metrics rows, ordered newest first.

    Args:
        limit (int): Maximum number of rows to return. Defaults to 10.
        run_number (Optional[int]): If provided, only return rows
                                     belonging to this run.

    Returns:
        List[SystemMetric]: List of SystemMetric ORM objects. Returns an
                             empty list on failure or if no data exists.
    """
    try:
        query = database_session.query(SystemMetric)
        if run_number is not None:
            query = query.filter(SystemMetric.run_number == run_number)
        results = query.order_by(SystemMetric.timestamp.desc()).limit(limit).all()
        return results
    except Exception as exc:
        logger.error("Failed to fetch latest metrics: %s", exc)
        return []


def get_metrics_history(run_number: Optional[int] = None, limit: int = 500) -> List[SystemMetric]:
    """
    Retrieve historical system_metrics rows, ordered oldest first (i.e. in
    chronological order, suitable for plotting a trend over time).

    Args:
        run_number (Optional[int]): If provided, only return rows
                                     belonging to this run. If omitted,
                                     returns history across all runs.
        limit (int): Maximum number of rows to return. Defaults to 500.

    Returns:
        List[SystemMetric]: List of SystemMetric ORM objects in
                             chronological order. Empty list on failure.
    """
    try:
        query = database_session.query(SystemMetric)
        if run_number is not None:
            query = query.filter(SystemMetric.run_number == run_number)
        results = query.order_by(SystemMetric.timestamp.asc()).limit(limit).all()
        return results
    except Exception as exc:
        logger.error("Failed to fetch metrics history: %s", exc)
        return []


def get_latest_processes(limit: int = 10, run_number: Optional[int] = None) -> List[SystemProcess]:
    """
    Retrieve the most recent system_processes rows, ordered newest first.

    Args:
        limit (int): Maximum number of rows to return. Defaults to 10.
        run_number (Optional[int]): If provided, only return rows
                                     belonging to this run.

    Returns:
        List[SystemProcess]: List of SystemProcess ORM objects. Returns an
                              empty list on failure or if no data exists.
    """
    try:
        query = database_session.query(SystemProcess)
        if run_number is not None:
            query = query.filter(SystemProcess.run_number == run_number)
        results = query.order_by(SystemProcess.timestamp.desc()).limit(limit).all()
        return results
    except Exception as exc:
        logger.error("Failed to fetch latest processes: %s", exc)
        return []


def get_processes_history(run_number: Optional[int] = None, limit: int = 500) -> List[SystemProcess]:
    """
    Retrieve historical system_processes rows, ordered oldest first.

    Args:
        run_number (Optional[int]): If provided, only return rows
                                     belonging to this run. If omitted,
                                     returns history across all runs.
        limit (int): Maximum number of rows to return. Defaults to 500.

    Returns:
        List[SystemProcess]: List of SystemProcess ORM objects in
                              chronological order. Empty list on failure.
    """
    try:
        query = database_session.query(SystemProcess)
        if run_number is not None:
            query = query.filter(SystemProcess.run_number == run_number)
        results = query.order_by(SystemProcess.timestamp.asc()).limit(limit).all()
        return results
    except Exception as exc:
        logger.error("Failed to fetch processes history: %s", exc)
        return []


def get_alerts(run_number: Optional[int] = None, limit: int = 200) -> List[Alert]:
    """
    Retrieve alert rows, ordered newest first.

    Args:
        run_number (Optional[int]): If provided, only return alerts
                                     belonging to this run. If omitted,
                                     returns alerts across all runs.
        limit (int): Maximum number of rows to return. Defaults to 200.

    Returns:
        List[Alert]: List of Alert ORM objects. Empty list on failure.
    """
    try:
        query = database_session.query(Alert)
        if run_number is not None:
            query = query.filter(Alert.run_number == run_number)
        results = query.order_by(Alert.timestamp.desc()).limit(limit).all()
        return results
    except Exception as exc:
        logger.error("Failed to fetch alerts: %s", exc)
        return []


def get_run_summary(run_number: int) -> Optional[TestRun]:
    """
    Retrieve the TestRun row (including its summary statistics, if the run
    has completed) for a specific run number.

    Args:
        run_number (int): The run number to look up.

    Returns:
        Optional[TestRun]: The matching TestRun ORM object, or None if not
                            found or on failure.
    """
    try:
        return (
            database_session.query(TestRun)
            .filter(TestRun.run_number == run_number)
            .first()
        )
    except Exception as exc:
        logger.error("Failed to fetch run summary for run_number=%s: %s", run_number, exc)
        return None


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

    test_run_number = 999  # sentinel value used only for this standalone test

    run_id = save_test_run(start_time=datetime.utcnow(), total_alerts=0, run_number=test_run_number)
    print("Created test_run with id:", run_id)

    metric_id = save_system_metric(
        cpu_usage=12.5,
        ram_usage=45.0,
        disk_usage=60.2,
        network_sent=1024.0,
        network_received=2048.0,
        system_uptime=3600.0,
        running_processes=120,
        run_number=test_run_number,
    )
    print("Created system_metrics row with id:", metric_id)

    process_id = save_system_process(
        process_name="python3",
        pid=1234,
        cpu_usage=5.5,
        ram_usage=2.1,
        run_number=test_run_number,
    )
    print("Created system_processes row with id:", process_id)

    alert_id = save_alert(alert_type="CPU", value=95.0, threshold=85.0, run_number=test_run_number)
    print("Created alert row with id:", alert_id)

    save_run_summary(
        run_number=test_run_number,
        total_runtime_seconds=120.0,
        average_cpu_usage=12.5,
        peak_cpu_usage=12.5,
        average_ram_usage=45.0,
        peak_ram_usage=45.0,
        average_disk_usage=60.2,
        peak_disk_usage=60.2,
        cpu_alerts=1,
        ram_alerts=0,
        network_alerts=0,
        total_samples_collected=1,
    )

    print("Latest metrics:", get_latest_metrics(limit=5))
    print("Metrics history for run:", get_metrics_history(run_number=test_run_number))
    print("Latest processes:", get_latest_processes(limit=5))
    print("Alerts for run:", get_alerts(run_number=test_run_number))
    print("Run summary:", get_run_summary(test_run_number))
    print("Test runs:", get_test_runs(limit=5))