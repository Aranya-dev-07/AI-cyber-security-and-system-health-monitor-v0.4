"""
database.py

This module defines the database layer for the System Health Monitor project using SQLAlchemy ORM with SQLite backend.
It includes models, session management, and CRUD functions for the application's data layer.

All functions are importable by main.py and api.py.
"""

import logging
from typing import Optional, Dict, Any, List
from datetime import datetime

from sqlalchemy import create_engine, Column, Integer, Float, String, DateTime, ForeignKey
from sqlalchemy.orm import declarative_base, sessionmaker, Session, relationship
from sqlalchemy.exc import SQLAlchemyError

# Initialize logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("database")

# Database constants
DATABASE_FILE = "system_monitor.db"

# SQLAlchemy setup
Base = declarative_base()

# Create the SQLAlchemy engine and session for use throughout the app
database_engine = create_engine(f"sqlite:///{DATABASE_FILE}", echo=False, future=True)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=database_engine)
database_session: Session = SessionLocal()

# ORM Models

class TestRun(Base):
    """
    Table to record entire test runs with start/end time and total alerts during the run.
    """
    __tablename__ = "test_run"
    id = Column(Integer, primary_key=True, autoincrement=True)
    start_time = Column(DateTime, nullable=False)
    end_time = Column(DateTime, nullable=True)  # May be None if run is ongoing
    total_alerts = Column(Integer, nullable=False, default=0)


class SystemMetric(Base):
    """
    System metrics samples, each with timestamp and core system statistics.
    """
    __tablename__ = "system_metrics"
    id = Column(Integer, primary_key=True, autoincrement=True)
    timestamp = Column(DateTime, nullable=False)
    cpu_usage = Column(Float, nullable=False)
    ram_usage = Column(Float, nullable=False)
    disk_usage = Column(Float, nullable=False)
    network_sent = Column(Float, nullable=False)
    network_received = Column(Float, nullable=False)
    system_uptime = Column(Float, nullable=False)
    running_processes = Column(Integer, nullable=False)


class SystemProcess(Base):
    """
    Top process information for a given sampling time.
    """
    __tablename__ = "system_processes"
    id = Column(Integer, primary_key=True, autoincrement=True)
    timestamp = Column(DateTime, nullable=False)
    process_name = Column(String(255), nullable=False)
    pid = Column(Integer, nullable=False)
    cpu_usage = Column(Float, nullable=False)
    ram_usage = Column(Float, nullable=False)


def initialize_database() -> None:
    """
    Creates all tables as defined by the ORM models if they do not exist already.
    To be called on application startup.
    """
    try:
        Base.metadata.create_all(bind=database_engine)
        logger.info("Database and tables initialized successfully.")
    except SQLAlchemyError as e:
        logger.exception(f"Error during database initialization: {e}")


def save_test_run(start_time: datetime, end_time: Optional[datetime], total_alerts: int) -> Optional[int]:
    """
    Saves a test run entry into the database.
    Returns the id of the saved test run, or None on error.
    """
    try:
        test_run = TestRun(
            start_time=start_time,
            end_time=end_time,
            total_alerts=total_alerts,
        )
        database_session.add(test_run)
        database_session.commit()
        logger.info(f"Saved test run with ID {test_run.id}")
        return test_run.id
    except SQLAlchemyError as e:
        database_session.rollback()
        logger.exception(f"Failed to save test run: {e}")
        return None


def save_system_metric(
    timestamp: datetime,
    cpu_usage: float,
    ram_usage: float,
    disk_usage: float,
    network_sent: float,
    network_received: float,
    system_uptime: float,
    running_processes: int,
) -> Optional[int]:
    """
    Saves a single system metric record to the database.
    Returns the inserted row id, or None on error.
    """
    try:
        metric = SystemMetric(
            timestamp=timestamp,
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
        logger.info(f"Saved system metric with ID {metric.id} (timestamp: {timestamp})")
        return metric.id
    except SQLAlchemyError as e:
        database_session.rollback()
        logger.exception(f"Failed to save system metric: {e}")
        return None


def save_system_process(
    timestamp: datetime,
    process_name: str,
    pid: int,
    cpu_usage: float,
    ram_usage: float,
) -> Optional[int]:
    """
    Saves details of a system process into the database.
    Returns the inserted row id, or None on error.
    """
    try:
        proc = SystemProcess(
            timestamp=timestamp,
            process_name=process_name,
            pid=pid,
            cpu_usage=cpu_usage,
            ram_usage=ram_usage,
        )
        database_session.add(proc)
        database_session.commit()
        logger.info(f"Saved system process '{process_name}' (pid {pid}, timestamp {timestamp}) with ID {proc.id}")
        return proc.id
    except SQLAlchemyError as e:
        database_session.rollback()
        logger.exception(f"Failed to save system process: {e}")
        return None


def get_latest_metrics(limit: int = 10) -> List[Dict[str, Any]]:
    """
    Returns the latest 'limit' system metrics as a list of dicts, sorted by newest first.
    """
    try:
        records = (
            database_session.query(SystemMetric)
            .order_by(SystemMetric.timestamp.desc())
            .limit(limit)
            .all()
        )
        logger.info(f"Fetched {len(records)} latest system metrics from database.")
        return [ 
            {
                "id": r.id,
                "timestamp": r.timestamp,
                "cpu_usage": r.cpu_usage,
                "ram_usage": r.ram_usage,
                "disk_usage": r.disk_usage,
                "network_sent": r.network_sent,
                "network_received": r.network_received,
                "system_uptime": r.system_uptime,
                "running_processes": r.running_processes,
            }
            for r in records
        ]
    except SQLAlchemyError as e:
        logger.exception(f"Failed to fetch latest system metrics: {e}")
        return []


def get_latest_processes(timestamp: Optional[datetime] = None, limit: int = 10) -> List[Dict[str, Any]]:
    """
    Returns the latest 'limit' system processes for a given timestamp.
    If timestamp is not specified, returns the latest timestamp's processes.
    """
    try:
        if timestamp is None:
            # Find the most recent timestamp in system_processes
            recent = (
                database_session.query(SystemProcess.timestamp)
                .order_by(SystemProcess.timestamp.desc())
                .first()
            )
            if not recent:
                logger.info("No system processes records available.")
                return []
            timestamp = recent[0]
        records = (
            database_session.query(SystemProcess)
            .filter(SystemProcess.timestamp == timestamp)
            .order_by(SystemProcess.cpu_usage.desc())  # Most resource-intensive first
            .limit(limit)
            .all()
        )
        logger.info(f"Fetched {len(records)} processes for timestamp {timestamp}.")
        return [
            {
                "id": r.id,
                "timestamp": r.timestamp,
                "process_name": r.process_name,
                "pid": r.pid,
                "cpu_usage": r.cpu_usage,
                "ram_usage": r.ram_usage,
            }
            for r in records
        ]
    except SQLAlchemyError as e:
        logger.exception(f"Failed to fetch latest system processes: {e}")
        return []


def get_test_runs(limit: int = 10) -> List[Dict[str, Any]]:
    """
    Returns last 'limit' test runs as a list of dicts (most recent first).
    """
    try:
        records = (
            database_session.query(TestRun)
            .order_by(TestRun.start_time.desc())
            .limit(limit)
            .all()
        )
        logger.info(f"Fetched {len(records)} latest test runs from database.")
        return [
            {
                "id": r.id,
                "start_time": r.start_time,
                "end_time": r.end_time,
                "total_alerts": r.total_alerts,
            }
            for r in records
        ]
    except SQLAlchemyError as e:
        logger.exception(f"Failed to fetch test runs: {e}")
        return []

# __all__ for import * usage in main.py and api.py
__all__ = [
    "database_engine",
    "database_session",
    "initialize_database",
    "save_test_run",
    "save_system_metric",
    "save_system_process",
    "get_latest_metrics",
    "get_latest_processes",
    "get_test_runs",
    "TestRun",
    "SystemMetric",
    "SystemProcess",
]