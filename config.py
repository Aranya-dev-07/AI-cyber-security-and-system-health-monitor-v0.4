"""
config.py
=========
Central configuration, shared state, and monitoring engine for the
System Health Monitor and Cybersecurity Monitoring Platform.

This module is the single source of truth for:
    * Global configuration constants (thresholds, intervals, file paths)
    * Shared in-memory state (metrics_data, process_data, alert_count, ...)
    * The data collection engine (collect_system_metrics, collect_process_metrics)
    * The alert engine (generate_alert)
    * The CSV persistence layer (save_metrics_to_csv, save_processes_to_csv,
      save_report_to_csv)
    * The run summary generator (generate_run_summary)

ARCHITECTURE NOTE
------------------
``config.py`` sits at the bottom of the import graph:

    config.py  <-- database.py  <-- api.py  <-- main.py

No other project module is imported here, so this file can be safely
imported by every other file in the project without circular-import issues.

NETWORK THRESHOLD NOTE
-----------------------
``psutil.net_io_counters()`` returns *cumulative* byte counters since boot,
not a rate. To make ``NETWORK_THRESHOLD`` meaningful (i.e. comparable to a
"MB transferred in this interval" figure), this module tracks the previous
cycle's cumulative counters and computes the *delta* (MB sent/received
since the last collection cycle) on every call to
:func:`collect_system_metrics`. That delta is what is stored, written to
CSV, and checked against ``NETWORK_THRESHOLD``.
"""

from __future__ import annotations

import csv
import logging
import os
import threading
from dataclasses import dataclass, field, asdict
from datetime import datetime
from typing import Any, Dict, List, Optional

import psutil

# ---------------------------------------------------------------------------
# Logging configuration (configured ONCE, here, for the entire project)
# ---------------------------------------------------------------------------
LOG_FILE_PATH: str = "system_health.log"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE_PATH, encoding="utf-8"),
        logging.StreamHandler(),
    ],
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Global configuration constants
# (MUST remain identical across config.py, database.py, api.py, main.py)
# ---------------------------------------------------------------------------
MONITOR_INTERVAL: int = 5          # seconds between each collection cycle
CPU_THRESHOLD: float = 85.0        # percent
RAM_THRESHOLD: float = 85.0        # percent
NETWORK_THRESHOLD: float = 100.0   # MB transferred per interval (sent + received)

TOP_PROCESS_COUNT: int = 5         # number of top processes tracked per cycle

DB_PATH: str = "system_monitor.db"
CSV_METRICS_PATH: str = "system_metrics.csv"
CSV_PROCESSES_PATH: str = "system_processes.csv"
CSV_REPORT_PATH: str = "system_report.csv"

# CSV column order (locked - database.py / api.py rely on these matching)
METRICS_CSV_FIELDS: List[str] = [
    "timestamp",
    "cpu_percent",
    "ram_percent",
    "disk_percent",
    "net_sent_mb",
    "net_recv_mb",
]
PROCESSES_CSV_FIELDS: List[str] = [
    "timestamp",
    "pid",
    "name",
    "cpu_percent",
    "memory_percent",
    "status",
]
REPORT_CSV_FIELDS: List[str] = [
    "run_id",
    "start_time",
    "end_time",
    "duration_sec",
    "avg_cpu",
    "avg_ram",
    "avg_disk",
    "total_alerts",
]

# ---------------------------------------------------------------------------
# Shared state objects
# (imported by database.py / api.py / main.py - never re-declared elsewhere)
# ---------------------------------------------------------------------------
metrics_data: List[Dict[str, Any]] = []
process_data: List[Dict[str, Any]] = []

alert_count: int = 0
run_start_time: Optional[datetime] = None
run_end_time: Optional[datetime] = None

# Thread-safety primitives shared across the project.
monitoring_active: threading.Event = threading.Event()
data_lock: threading.Lock = threading.Lock()

# Internal bookkeeping for computing network *deltas* between cycles.
# Not part of the public shared-state contract; used only inside this module.
_prev_net_bytes_sent: Optional[int] = None
_prev_net_bytes_recv: Optional[int] = None


# ---------------------------------------------------------------------------
# Dataclasses
# ---------------------------------------------------------------------------
@dataclass
class SystemMetric:
    """A single snapshot of system-wide resource usage.

    Attributes:
        timestamp: ISO-8601 timestamp of when the snapshot was taken.
        cpu_percent: System-wide CPU utilisation, in percent.
        ram_percent: System-wide RAM utilisation, in percent.
        disk_percent: Disk utilisation of the root partition, in percent.
        net_sent_mb: Megabytes sent since the previous collection cycle.
        net_recv_mb: Megabytes received since the previous collection cycle.
    """

    timestamp: str
    cpu_percent: float
    ram_percent: float
    disk_percent: float
    net_sent_mb: float
    net_recv_mb: float

    def to_dict(self) -> Dict[str, Any]:
        """Return a plain ``dict`` representation (CSV/DB friendly)."""
        return asdict(self)


@dataclass
class ProcessMetric:
    """A single snapshot of one process's resource usage.

    Attributes:
        timestamp: ISO-8601 timestamp of when the snapshot was taken.
        pid: Process ID.
        name: Process name.
        cpu_percent: Process CPU utilisation, in percent.
        memory_percent: Process memory utilisation, in percent.
        status: Process status string (e.g. "running", "sleeping").
    """

    timestamp: str
    pid: int
    name: str
    cpu_percent: float
    memory_percent: float
    status: str

    def to_dict(self) -> Dict[str, Any]:
        """Return a plain ``dict`` representation (CSV/DB friendly)."""
        return asdict(self)


@dataclass
class RunSummary:
    """Aggregate statistics for one monitoring run.

    Attributes:
        run_id: Identifier of the run (assigned by database.py; 0 if unset).
        start_time: ISO-8601 timestamp the run started.
        end_time: ISO-8601 timestamp the run ended.
        duration_sec: Total run duration, in seconds.
        avg_cpu: Average CPU utilisation across the run, in percent.
        avg_ram: Average RAM utilisation across the run, in percent.
        avg_disk: Average disk utilisation across the run, in percent.
        total_alerts: Total number of alerts raised during the run.
    """

    run_id: int
    start_time: str
    end_time: str
    duration_sec: float
    avg_cpu: float
    avg_ram: float
    avg_disk: float
    total_alerts: int

    def to_dict(self) -> Dict[str, Any]:
        """Return a plain ``dict`` representation (CSV/DB friendly)."""
        return asdict(self)


# ---------------------------------------------------------------------------
# Data collection engine
# ---------------------------------------------------------------------------
def collect_system_metrics() -> Dict[str, Any]:
    """Collect one snapshot of system-wide resource usage.

    Gathers CPU%, RAM%, disk%, and network throughput (sent/received MB
    since the previous call), appends the snapshot to the shared
    ``metrics_data`` list, writes it to ``system_metrics.csv``, and runs
    the alert engine against it.

    Returns:
        A dictionary representation of the collected :class:`SystemMetric`.

    Raises:
        Never raises: all ``psutil``/IO errors are caught and logged so that
        a single failed collection cycle does not crash the monitoring
        thread.
    """
    global _prev_net_bytes_sent, _prev_net_bytes_recv

    try:
        timestamp = datetime.now().isoformat()

        cpu_percent = psutil.cpu_percent(interval=1)
        ram_percent = psutil.virtual_memory().percent
        disk_percent = psutil.disk_usage(os.sep).percent

        net_io = psutil.net_io_counters()
        if _prev_net_bytes_sent is None or _prev_net_bytes_recv is None:
            # First cycle: no previous reading to diff against.
            net_sent_mb = 0.0
            net_recv_mb = 0.0
        else:
            net_sent_mb = max(0, net_io.bytes_sent - _prev_net_bytes_sent) / (1024 ** 2)
            net_recv_mb = max(0, net_io.bytes_recv - _prev_net_bytes_recv) / (1024 ** 2)

        _prev_net_bytes_sent = net_io.bytes_sent
        _prev_net_bytes_recv = net_io.bytes_recv

        metric = SystemMetric(
            timestamp=timestamp,
            cpu_percent=cpu_percent,
            ram_percent=ram_percent,
            disk_percent=disk_percent,
            net_sent_mb=round(net_sent_mb, 4),
            net_recv_mb=round(net_recv_mb, 4),
        )
        metric_dict = metric.to_dict()

        with data_lock:
            metrics_data.append(metric_dict)

        save_metrics_to_csv(metric_dict)

        # Run the alert engine against this fresh snapshot.
        generate_alert("CPU", cpu_percent, CPU_THRESHOLD)
        generate_alert("RAM", ram_percent, RAM_THRESHOLD)
        generate_alert("NETWORK", net_sent_mb + net_recv_mb, NETWORK_THRESHOLD)

        logger.info(
            "Collected system metrics: CPU=%.1f%% RAM=%.1f%% DISK=%.1f%% "
            "NET_SENT=%.3fMB NET_RECV=%.3fMB",
            cpu_percent, ram_percent, disk_percent, net_sent_mb, net_recv_mb,
        )
        return metric_dict

    except Exception:
        logger.exception("Failed to collect system metrics.")
        return {}


def collect_process_metrics() -> List[Dict[str, Any]]:
    """Collect the top ``TOP_PROCESS_COUNT`` processes by CPU usage.

    Iterates all running processes, reads PID, name, CPU%, memory%, and
    status for each, sorts the result by CPU usage (descending), and keeps
    only the top ``TOP_PROCESS_COUNT`` entries. Appends the result to the
    shared ``process_data`` list and writes it to ``system_processes.csv``.

    Returns:
        A list of dictionaries, one per top process, sorted by CPU usage
        descending. Returns an empty list if collection fails entirely.

    Raises:
        Never raises: per-process errors (e.g. a process exiting mid-scan)
        are caught individually so they don't abort the whole scan.
    """
    try:
        timestamp = datetime.now().isoformat()
        collected: List[ProcessMetric] = []

        for proc in psutil.process_iter(
            attrs=["pid", "name", "cpu_percent", "memory_percent", "status"]
        ):
            try:
                info = proc.info
                collected.append(
                    ProcessMetric(
                        timestamp=timestamp,
                        pid=info.get("pid", -1),
                        name=info.get("name") or "unknown",
                        cpu_percent=float(info.get("cpu_percent") or 0.0),
                        memory_percent=float(info.get("memory_percent") or 0.0),
                        status=info.get("status") or "unknown",
                    )
                )
            except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
                # Process disappeared or is inaccessible mid-scan; skip it.
                continue

        collected.sort(key=lambda p: p.cpu_percent, reverse=True)
        top_processes = collected[:TOP_PROCESS_COUNT]
        top_processes_dicts = [p.to_dict() for p in top_processes]

        with data_lock:
            process_data.append({"timestamp": timestamp, "processes": top_processes_dicts})

        save_processes_to_csv(top_processes_dicts)

        logger.info("Collected top %d processes by CPU usage.", len(top_processes_dicts))
        return top_processes_dicts

    except Exception:
        logger.exception("Failed to collect process metrics.")
        return []


# ---------------------------------------------------------------------------
# Alert engine
# ---------------------------------------------------------------------------
def generate_alert(metric_name: str, value: float, threshold: float) -> Optional[str]:
    """Check a metric against its threshold and raise an alert if exceeded.

    If ``value`` exceeds ``threshold``, increments the shared
    ``alert_count``, prints a human-readable warning to the terminal
    immediately, logs the alert, and returns the alert message.

    Args:
        metric_name: One of ``"CPU"``, ``"RAM"``, or ``"NETWORK"``
            (case-insensitive), used to select the alert message.
        value: The current measured value of the metric.
        threshold: The threshold above which an alert should fire.

    Returns:
        The alert message string if an alert was raised, otherwise
        ``None``.
    """
    global alert_count

    messages: Dict[str, str] = {
        "CPU": "You are over using your device! CPU usage exceeded threshold.",
        "RAM": "You are over using your device! RAM usage exceeded threshold.",
        "NETWORK": "You are over using your device! Network activity exceeded threshold.",
    }

    key = metric_name.upper()
    if key not in messages:
        logger.warning("generate_alert called with unknown metric_name: %s", metric_name)
        return None

    if value > threshold:
        message = messages[key]
        with data_lock:
            alert_count += 1
        print(f"[ALERT] {message} (value={value:.2f}, threshold={threshold:.2f})")
        logger.warning(
            "ALERT triggered for %s: value=%.2f exceeded threshold=%.2f",
            key, value, threshold,
        )
        return message

    return None


# ---------------------------------------------------------------------------
# CSV persistence layer
# ---------------------------------------------------------------------------
def _write_csv_row(path: str, fieldnames: List[str], row: Dict[str, Any]) -> None:
    """Append a single row to a CSV file, writing a header if the file is new.

    Args:
        path: Path to the target CSV file.
        fieldnames: Ordered list of column names.
        row: Dictionary of values to write; must be a subset of fieldnames.

    Raises:
        Never raises: IO errors are caught and logged.
    """
    try:
        file_exists = os.path.isfile(path) and os.path.getsize(path) > 0
        with open(path, mode="a", newline="", encoding="utf-8") as csv_file:
            writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
            if not file_exists:
                writer.writeheader()
            writer.writerow({field: row.get(field, "") for field in fieldnames})
    except OSError:
        logger.exception("Failed to write row to CSV file: %s", path)


def save_metrics_to_csv(metric: Dict[str, Any]) -> None:
    """Append one system metric snapshot to ``system_metrics.csv``.

    Args:
        metric: A dictionary matching :class:`SystemMetric` fields, as
            produced by :func:`collect_system_metrics`.
    """
    if not metric:
        return
    _write_csv_row(CSV_METRICS_PATH, METRICS_CSV_FIELDS, metric)


def save_processes_to_csv(processes: List[Dict[str, Any]]) -> None:
    """Append top-process rows to ``system_processes.csv``.

    Args:
        processes: A list of dictionaries matching :class:`ProcessMetric`
            fields, as produced by :func:`collect_process_metrics`.
    """
    for process_row in processes:
        _write_csv_row(CSV_PROCESSES_PATH, PROCESSES_CSV_FIELDS, process_row)


def save_report_to_csv(summary: Dict[str, Any]) -> None:
    """Append one run summary row to ``system_report.csv``.

    Args:
        summary: A dictionary matching :class:`RunSummary` fields, as
            produced by :func:`generate_run_summary`.
    """
    if not summary:
        return
    _write_csv_row(CSV_REPORT_PATH, REPORT_CSV_FIELDS, summary)


# ---------------------------------------------------------------------------
# Run summary generator
# ---------------------------------------------------------------------------
def generate_run_summary(run_id: int = 0) -> Dict[str, Any]:
    """Compute aggregate statistics for the current monitoring run.

    Uses the shared ``metrics_data``, ``run_start_time``, ``run_end_time``,
    and ``alert_count`` to build a :class:`RunSummary`. If ``run_end_time``
    has not been set yet, the current time is used as a fallback so the
    summary can still be generated safely.

    Args:
        run_id: The database-assigned identifier for this run, if known.
            Defaults to ``0`` when called before the run has been persisted.

    Returns:
        A dictionary representation of the computed :class:`RunSummary`.
        Returns an empty dict if no metrics were collected during the run.
    """
    try:
        with data_lock:
            snapshot = list(metrics_data)
            current_alert_count = alert_count

        if not snapshot:
            logger.warning("generate_run_summary called with no collected metrics.")
            return {}

        start = run_start_time or datetime.now()
        end = run_end_time or datetime.now()
        duration_sec = max(0.0, (end - start).total_seconds())

        avg_cpu = sum(m["cpu_percent"] for m in snapshot) / len(snapshot)
        avg_ram = sum(m["ram_percent"] for m in snapshot) / len(snapshot)
        avg_disk = sum(m["disk_percent"] for m in snapshot) / len(snapshot)

        summary = RunSummary(
            run_id=run_id,
            start_time=start.isoformat(),
            end_time=end.isoformat(),
            duration_sec=round(duration_sec, 2),
            avg_cpu=round(avg_cpu, 2),
            avg_ram=round(avg_ram, 2),
            avg_disk=round(avg_disk, 2),
            total_alerts=current_alert_count,
        )

        logger.info(
            "Generated run summary: duration=%.2fs avg_cpu=%.2f%% "
            "avg_ram=%.2f%% avg_disk=%.2f%% alerts=%d",
            duration_sec, avg_cpu, avg_ram, avg_disk, current_alert_count,
        )
        return summary.to_dict()

    except Exception:
        logger.exception("Failed to generate run summary.")
        return {}