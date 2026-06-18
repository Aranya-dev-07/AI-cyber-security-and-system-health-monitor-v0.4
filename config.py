"""
config.py
=========

Monitoring engine for the System Health Monitor project.

This module is responsible for:
    - Collecting live system metrics (CPU, RAM, Disk, Network, Uptime, Processes)
      using the `psutil` library.
    - Maintaining shared, in-memory state describing the current monitoring
      session (start/end time, alert counters, thresholds, collected data).
    - Persisting collected data to CSV files (system_metrics.csv,
      system_processes.csv) on every monitoring cycle.
    - Running a simple threshold-based alert engine.
    - Generating an end-of-session summary report (system_report.csv).

This file is designed to be imported by other modules in the project
(e.g. main.py, api.py). It does not run a monitoring loop on its own -
the calling module is expected to repeatedly invoke `run_monitoring_cycle()`
(e.g. inside a loop or scheduler) and call `generate_summary_report()`
when monitoring ends.

Author: System Health Monitor Project
"""

import csv
import logging
import os
import time
from datetime import datetime
from typing import Any, Dict, List, Optional

import psutil

# database.py is imported so that alerts and run summaries can be persisted
# to SQLite directly from the alert engine / summary generator, ensuring
# api.py (running in any process) can always serve this data from the DB.
# The import is defensive: if database.py or its dependencies (SQLAlchemy)
# are unavailable, config.py still works standalone (CSV output and
# in-memory state are unaffected) - only DB persistence is skipped.
try:
    import database as _database
    _DATABASE_AVAILABLE = True
except Exception as _db_import_exc:  # pragma: no cover - environment-dependent
    _database = None
    _DATABASE_AVAILABLE = False

# ---------------------------------------------------------------------------
# Logging configuration
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("config")

if not _DATABASE_AVAILABLE:
    logger.warning(
        "database.py could not be imported; alerts and run summaries will "
        "only be available via CSV/in-memory state, not the database."
    )


# ---------------------------------------------------------------------------
# File paths (CSV outputs)
# ---------------------------------------------------------------------------
BASE_DIR: str = os.path.dirname(os.path.abspath(__file__))
METRICS_CSV_PATH: str = os.path.join(BASE_DIR, "system_metrics.csv")
PROCESSES_CSV_PATH: str = os.path.join(BASE_DIR, "system_processes.csv")
REPORT_CSV_PATH: str = os.path.join(BASE_DIR, "system_report.csv")
RUN_COUNTER_PATH: str = os.path.join(BASE_DIR, "run_count.txt")

# "run_number" is included first in both CSVs so every row is traceable
# back to the specific program execution ("run") that produced it.
METRICS_CSV_HEADERS: List[str] = [
    "run_number",
    "timestamp",
    "cpu_usage_percent",
    "ram_usage_percent",
    "disk_usage_percent",
    "network_bytes_sent",
    "network_bytes_received",
    "system_uptime_seconds",
    "total_running_processes",
]

PROCESSES_CSV_HEADERS: List[str] = [
    "run_number",
    "timestamp",
    "process_name",
    "pid",
    "cpu_usage_percent",
    "ram_usage_percent",
]


# ---------------------------------------------------------------------------
# Shared state variables
# ---------------------------------------------------------------------------
# These act as the "global" shared variables for the monitoring session.
# Other modules (main.py, api.py) may import and read/modify these directly,
# e.g. `import config` then `config.monitoring_active = True`.

monitoring_active: bool = False
start_time: Optional[datetime] = None
end_time: Optional[datetime] = None
alert_count: int = 0

# Tracks which "run" (program execution) this is. Persisted to disk
# (RUN_COUNTER_PATH) so it keeps incrementing across separate executions
# of the program, not just within a single session. Set by
# `initialize_run_number()`, normally called once at startup.
current_run_number: int = 0

# Alert thresholds (percentages for cpu/ram, bytes/sec for network).
# These can be overridden by the importing module before monitoring starts.
cpu_threshold: float = 85.0
ram_threshold: float = 85.0
network_threshold: float = 5_000_000  # bytes/sec (~5 MB/s), adjustable

# In-memory collections of everything gathered during the session.
system_metrics_data: List[Dict[str, Any]] = []
system_processes_data: List[Dict[str, Any]] = []
alerts_generated: List[Dict[str, Any]] = []

# Internal helper state used to compute network throughput (bytes/sec)
# between samples, since psutil reports cumulative byte counters.
_last_net_bytes_sent: Optional[int] = None
_last_net_bytes_recv: Optional[int] = None
_last_net_sample_time: Optional[float] = None


# ---------------------------------------------------------------------------
# Run number tracking (persists across separate program executions)
# ---------------------------------------------------------------------------
def initialize_run_number() -> int:
    """
    Determine the current run number for this program execution and persist
    the incremented value to disk so the *next* execution knows to use the
    following number.

    On first-ever execution (no counter file present), this run is "1".
    Each subsequent execution increments by one, regardless of how many
    monitoring cycles occurred in prior runs.

    Updates the global `current_run_number` and prints/logs
    "This is run {n}" immediately.

    Returns:
        int: The run number assigned to this execution.
    """
    global current_run_number

    try:
        if os.path.isfile(RUN_COUNTER_PATH):
            with open(RUN_COUNTER_PATH, "r", encoding="utf-8") as f:
                contents = f.read().strip()
                last_run = int(contents) if contents else 0
        else:
            last_run = 0

        current_run_number = last_run + 1

        with open(RUN_COUNTER_PATH, "w", encoding="utf-8") as f:
            f.write(str(current_run_number))

        logger.info("Run number initialized: %d", current_run_number)

    except Exception as exc:
        # If the counter file is missing, corrupted, or unwritable, fall
        # back to run 1 rather than crashing startup. The run will simply
        # not persist across executions until the underlying issue (e.g.
        # file permissions) is fixed.
        logger.error("Failed to initialize run number, defaulting to 1: %s", exc)
        current_run_number = 1

    print(f"This is run {current_run_number}")
    return current_run_number



def get_cpu_usage() -> float:
    """
    Return current system-wide CPU utilization as a percentage.

    Returns:
        float: CPU usage percent (0.0 - 100.0). Returns 0.0 on failure.
    """
    try:
        # interval=1 blocks for 1 second to produce a meaningful, accurate
        # reading rather than an instantaneous (and often misleading) value.
        return psutil.cpu_percent(interval=1)
    except Exception as exc:
        logger.error("Failed to collect CPU usage: %s", exc)
        return 0.0


def get_ram_usage() -> float:
    """
    Return current system-wide RAM utilization as a percentage.

    Returns:
        float: RAM usage percent (0.0 - 100.0). Returns 0.0 on failure.
    """
    try:
        return psutil.virtual_memory().percent
    except Exception as exc:
        logger.error("Failed to collect RAM usage: %s", exc)
        return 0.0


def get_disk_usage(path: str = "/") -> float:
    """
    Return current disk utilization as a percentage for the given path.

    Args:
        path (str): Mount point / path to check disk usage for.
                     Defaults to root ("/").

    Returns:
        float: Disk usage percent (0.0 - 100.0). Returns 0.0 on failure.
    """
    try:
        return psutil.disk_usage(path).percent
    except Exception as exc:
        logger.error("Failed to collect disk usage for path '%s': %s", path, exc)
        return 0.0


def get_network_bytes_sent() -> int:
    """
    Return total cumulative bytes sent over the network since boot.

    Returns:
        int: Bytes sent. Returns 0 on failure.
    """
    try:
        return psutil.net_io_counters().bytes_sent
    except Exception as exc:
        logger.error("Failed to collect network bytes sent: %s", exc)
        return 0


def get_network_bytes_received() -> int:
    """
    Return total cumulative bytes received over the network since boot.

    Returns:
        int: Bytes received. Returns 0 on failure.
    """
    try:
        return psutil.net_io_counters().bytes_recv
    except Exception as exc:
        logger.error("Failed to collect network bytes received: %s", exc)
        return 0


def get_network_throughput() -> Dict[str, float]:
    """
    Compute approximate network throughput (bytes/sec) since the last call,
    using the cumulative counters from psutil.

    This is used internally by the alert engine to evaluate
    `network_threshold` against a rate rather than a cumulative total.

    Returns:
        Dict[str, float]: {"sent_bytes_per_sec": ..., "recv_bytes_per_sec": ...}
                           Returns zeros on the first call (no prior sample)
                           or on failure.
    """
    global _last_net_bytes_sent, _last_net_bytes_recv, _last_net_sample_time

    try:
        current_sent = get_network_bytes_sent()
        current_recv = get_network_bytes_received()
        current_time = time.time()

        if _last_net_sample_time is None:
            sent_rate = 0.0
            recv_rate = 0.0
        else:
            elapsed = max(current_time - _last_net_sample_time, 1e-6)
            sent_rate = max(current_sent - _last_net_bytes_sent, 0) / elapsed
            recv_rate = max(current_recv - _last_net_bytes_recv, 0) / elapsed

        _last_net_bytes_sent = current_sent
        _last_net_bytes_recv = current_recv
        _last_net_sample_time = current_time

        return {"sent_bytes_per_sec": sent_rate, "recv_bytes_per_sec": recv_rate}
    except Exception as exc:
        logger.error("Failed to compute network throughput: %s", exc)
        return {"sent_bytes_per_sec": 0.0, "recv_bytes_per_sec": 0.0}


def get_system_uptime() -> float:
    """
    Return system uptime in seconds since boot.

    Returns:
        float: Uptime in seconds. Returns 0.0 on failure.
    """
    try:
        return time.time() - psutil.boot_time()
    except Exception as exc:
        logger.error("Failed to collect system uptime: %s", exc)
        return 0.0


def get_total_running_processes() -> int:
    """
    Return the total number of currently running processes.

    Returns:
        int: Number of running processes. Returns 0 on failure.
    """
    try:
        return len(psutil.pids())
    except Exception as exc:
        logger.error("Failed to collect total running processes: %s", exc)
        return 0


# ---------------------------------------------------------------------------
# Aggregate collectors
# ---------------------------------------------------------------------------
def collect_system_metrics() -> Dict[str, Any]:
    """
    Collect all core system metrics in one call and return them as a
    single dictionary, timestamped at collection time.

    Returns:
        Dict[str, Any]: Dictionary containing all collected metrics, keyed
                         to match METRICS_CSV_HEADERS (minus formatting).
    """
    try:
        timestamp = datetime.now().isoformat()
        cpu = get_cpu_usage()
        ram = get_ram_usage()
        disk = get_disk_usage()
        net_sent = get_network_bytes_sent()
        net_recv = get_network_bytes_received()
        uptime = get_system_uptime()
        process_count = get_total_running_processes()

        metrics: Dict[str, Any] = {
            "run_number": current_run_number,
            "timestamp": timestamp,
            "cpu_usage_percent": cpu,
            "ram_usage_percent": ram,
            "disk_usage_percent": disk,
            "network_bytes_sent": net_sent,
            "network_bytes_received": net_recv,
            "system_uptime_seconds": uptime,
            "total_running_processes": process_count,
        }

        logger.info(
            "Collected metrics: CPU=%.1f%% RAM=%.1f%% DISK=%.1f%% PROCS=%d",
            cpu, ram, disk, process_count,
        )
        return metrics

    except Exception as exc:
        logger.error("Failed to collect system metrics: %s", exc)
        # Return a safe, fully-keyed fallback so downstream code (CSV
        # writers, dashboards) never has to special-case a missing key.
        return {
            "run_number": current_run_number,
            "timestamp": datetime.now().isoformat(),
            "cpu_usage_percent": 0.0,
            "ram_usage_percent": 0.0,
            "disk_usage_percent": 0.0,
            "network_bytes_sent": 0,
            "network_bytes_received": 0,
            "system_uptime_seconds": 0.0,
            "total_running_processes": 0,
        }


def collect_top_processes(limit: int = 5) -> List[Dict[str, Any]]:
    """
    Collect the top N processes sorted by CPU usage (descending).

    For every process, attempts to gather:
        - Process Name
        - PID
        - CPU Usage (%)
        - RAM Usage (%)

    Args:
        limit (int): Number of top processes to return. Defaults to 5.

    Returns:
        List[Dict[str, Any]]: List of process info dictionaries, sorted by
                               CPU usage descending. Processes that can no
                               longer be inspected (e.g. they exited mid-scan)
                               are safely skipped.
    """
    timestamp = datetime.now().isoformat()
    processes: List[Dict[str, Any]] = []

    try:
        # Prime cpu_percent for each process. psutil requires an initial
        # call to start measuring, otherwise the first reading is always 0.0.
        for proc in psutil.process_iter(["pid", "name"]):
            try:
                proc.cpu_percent(interval=None)
            except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
                continue

        # Small delay lets psutil compute a meaningful interval-based
        # CPU percentage on the second pass.
        time.sleep(0.1)

        for proc in psutil.process_iter(["pid", "name"]):
            try:
                pid = proc.info.get("pid")
                name = proc.info.get("name") or "unknown"
                cpu_percent = proc.cpu_percent(interval=None)
                ram_percent = proc.memory_percent()

                processes.append({
                    "run_number": current_run_number,
                    "timestamp": timestamp,
                    "process_name": name,
                    "pid": pid,
                    "cpu_usage_percent": cpu_percent,
                    "ram_usage_percent": round(ram_percent, 2),
                })
            except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
                # Process may have exited or be inaccessible; skip safely.
                continue
            except Exception as inner_exc:
                logger.warning("Skipping process due to error: %s", inner_exc)
                continue

        processes.sort(key=lambda p: p["cpu_usage_percent"], reverse=True)
        top_processes = processes[:limit]

        logger.info("Collected top %d processes by CPU usage.", len(top_processes))
        return top_processes

    except Exception as exc:
        logger.error("Failed to collect top processes: %s", exc)
        return []


# ---------------------------------------------------------------------------
# CSV persistence helpers
# ---------------------------------------------------------------------------
def _ensure_csv_exists(path: str, headers: List[str]) -> None:
    """
    Create a CSV file with the given headers if it does not already exist.

    Args:
        path (str): Full path to the CSV file.
        headers (List[str]): Column headers to write if the file is created.
    """
    try:
        if not os.path.isfile(path):
            with open(path, mode="w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=headers)
                writer.writeheader()
            logger.info("Created new CSV file: %s", path)
    except Exception as exc:
        logger.error("Failed to create CSV file '%s': %s", path, exc)


def _append_csv_row(path: str, headers: List[str], row: Dict[str, Any]) -> None:
    """
    Append a single row to a CSV file, creating the file with headers first
    if necessary.

    Args:
        path (str): Full path to the CSV file.
        headers (List[str]): Column headers (also used as DictWriter fieldnames).
        row (Dict[str, Any]): Data to append. Keys must match `headers`.
    """
    try:
        _ensure_csv_exists(path, headers)
        with open(path, mode="a", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=headers)
            writer.writerow(row)
    except Exception as exc:
        logger.error("Failed to append row to CSV file '%s': %s", path, exc)


def _append_csv_rows(path: str, headers: List[str], rows: List[Dict[str, Any]]) -> None:
    """
    Append multiple rows to a CSV file, creating the file with headers first
    if necessary.

    Args:
        path (str): Full path to the CSV file.
        headers (List[str]): Column headers (also used as DictWriter fieldnames).
        rows (List[Dict[str, Any]]): List of row dictionaries to append.
    """
    if not rows:
        return
    try:
        _ensure_csv_exists(path, headers)
        with open(path, mode="a", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=headers)
            writer.writerows(rows)
    except Exception as exc:
        logger.error("Failed to append rows to CSV file '%s': %s", path, exc)


# ---------------------------------------------------------------------------
# Alert engine
# ---------------------------------------------------------------------------
def check_alerts(metrics: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Evaluate the given metrics dictionary against the configured thresholds
    (cpu_threshold, ram_threshold, network_threshold) and generate alerts
    for any that are exceeded.

    On any threshold breach, this function:
        - Prints "You are over using your device!"
        - Appends a structured alert record to `alerts_generated`
        - Increments the global `alert_count`

    Args:
        metrics (Dict[str, Any]): A metrics dictionary as returned by
                                   `collect_system_metrics()`.

    Returns:
        List[Dict[str, Any]]: The list of new alerts generated during this
                               check (empty list if none were triggered).
    """
    global alert_count

    new_alerts: List[Dict[str, Any]] = []

    try:
        net_throughput = get_network_throughput()
        total_network_rate = (
            net_throughput.get("sent_bytes_per_sec", 0.0)
            + net_throughput.get("recv_bytes_per_sec", 0.0)
        )

        triggered = False

        if metrics.get("cpu_usage_percent", 0.0) > cpu_threshold:
            new_alerts.append({
                "timestamp": metrics.get("timestamp", datetime.now().isoformat()),
                "type": "CPU",
                "value": metrics.get("cpu_usage_percent"),
                "threshold": cpu_threshold,
            })
            triggered = True

        if metrics.get("ram_usage_percent", 0.0) > ram_threshold:
            new_alerts.append({
                "timestamp": metrics.get("timestamp", datetime.now().isoformat()),
                "type": "RAM",
                "value": metrics.get("ram_usage_percent"),
                "threshold": ram_threshold,
            })
            triggered = True

        if total_network_rate > network_threshold:
            new_alerts.append({
                "timestamp": metrics.get("timestamp", datetime.now().isoformat()),
                "type": "NETWORK",
                "value": total_network_rate,
                "threshold": network_threshold,
            })
            triggered = True

        if triggered:
            print("You are over using your device!")
            for alert in new_alerts:
                alerts_generated.append(alert)
                alert_count += 1
                logger.warning(
                    "ALERT [%s] value=%.2f exceeded threshold=%.2f",
                    alert["type"], alert["value"], alert["threshold"],
                )

                if _DATABASE_AVAILABLE:
                    try:
                        _database.save_alert(
                            alert_type=alert["type"],
                            value=alert["value"],
                            threshold=alert["threshold"],
                            run_number=current_run_number,
                        )
                    except Exception as db_exc:
                        # A database hiccup must never break the alert
                        # engine itself - the CSV/in-memory record above
                        # has already succeeded regardless.
                        logger.error("Failed to persist alert to database: %s", db_exc)

        return new_alerts

    except Exception as exc:
        logger.error("Failed to evaluate alerts: %s", exc)
        return []


# ---------------------------------------------------------------------------
# Summary report generation
# ---------------------------------------------------------------------------
def generate_summary_report() -> Optional[str]:
    """
    Generate a summary report of the monitoring session and write it to
    system_report.csv.

    The report includes:
        - Start Time
        - End Time
        - Total Runtime (seconds)
        - Average CPU Usage
        - Peak CPU Usage
        - Average RAM Usage
        - Peak RAM Usage
        - Total Alerts Generated
        - Total Samples Collected

    Returns:
        Optional[str]: The path to the generated report file, or None if
                        generation failed.
    """
    global end_time

    try:
        if end_time is None:
            end_time = datetime.now()

        if not system_metrics_data:
            logger.warning("No metrics data available; generating an empty summary report.")
            cpu_values: List[float] = []
            ram_values: List[float] = []
        else:
            cpu_values = [m.get("cpu_usage_percent", 0.0) for m in system_metrics_data]
            ram_values = [m.get("ram_usage_percent", 0.0) for m in system_metrics_data]

        avg_cpu = sum(cpu_values) / len(cpu_values) if cpu_values else 0.0
        peak_cpu = max(cpu_values) if cpu_values else 0.0
        avg_ram = sum(ram_values) / len(ram_values) if ram_values else 0.0
        peak_ram = max(ram_values) if ram_values else 0.0

        if start_time is not None:
            total_runtime_seconds = (end_time - start_time).total_seconds()
            start_time_str = start_time.isoformat()
        else:
            total_runtime_seconds = 0.0
            start_time_str = "N/A"

        if disk_values := [m.get("disk_usage_percent", 0.0) for m in system_metrics_data]:
            avg_disk = sum(disk_values) / len(disk_values)
            peak_disk = max(disk_values)
        else:
            avg_disk = peak_disk = 0.0

        # Break down alerts by type (CPU / RAM / NETWORK) for a more
        # informative end-of-run summary.
        alert_type_counts: Dict[str, int] = {}
        for alert in alerts_generated:
            alert_type = alert.get("type", "UNKNOWN")
            alert_type_counts[alert_type] = alert_type_counts.get(alert_type, 0) + 1

        report_row = {
            "Run Number": current_run_number,
            "Start Time": start_time_str,
            "End Time": end_time.isoformat(),
            "Total Runtime (seconds)": round(total_runtime_seconds, 2),
            "Average CPU Usage (%)": round(avg_cpu, 2),
            "Peak CPU Usage (%)": round(peak_cpu, 2),
            "Average RAM Usage (%)": round(avg_ram, 2),
            "Peak RAM Usage (%)": round(peak_ram, 2),
            "Average Disk Usage (%)": round(avg_disk, 2),
            "Peak Disk Usage (%)": round(peak_disk, 2),
            "Total Alerts Generated": alert_count,
            "CPU Alerts": alert_type_counts.get("CPU", 0),
            "RAM Alerts": alert_type_counts.get("RAM", 0),
            "Network Alerts": alert_type_counts.get("NETWORK", 0),
            "Total Samples Collected": len(system_metrics_data),
        }

        report_headers = list(report_row.keys())

        # Summary report is overwritten fresh each time it's generated
        # (it represents a single completed session), rather than appended.
        with open(REPORT_CSV_PATH, mode="w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=report_headers)
            writer.writeheader()
            writer.writerow(report_row)

        logger.info("Summary report generated at: %s", REPORT_CSV_PATH)

        # Also print a human-readable summary directly to the terminal,
        # so the user sees the full run's results immediately on stop,
        # without needing to open the CSV file.
        print("\n" + "=" * 60)
        print(f"SUMMARY FOR RUN {current_run_number}")
        print("=" * 60)
        print(f"Start Time            : {start_time_str}")
        print(f"End Time              : {end_time.isoformat()}")
        print(f"Total Runtime         : {round(total_runtime_seconds, 2)} sec")
        print(f"Average CPU Usage     : {round(avg_cpu, 2)}%")
        print(f"Peak CPU Usage        : {round(peak_cpu, 2)}%")
        print(f"Average RAM Usage     : {round(avg_ram, 2)}%")
        print(f"Peak RAM Usage        : {round(peak_ram, 2)}%")
        print(f"Average Disk Usage    : {round(avg_disk, 2)}%")
        print(f"Peak Disk Usage       : {round(peak_disk, 2)}%")
        print(f"Total Alerts Generated: {alert_count} "
              f"(CPU={alert_type_counts.get('CPU', 0)}, "
              f"RAM={alert_type_counts.get('RAM', 0)}, "
              f"Network={alert_type_counts.get('NETWORK', 0)})")
        print(f"Total Samples Collected: {len(system_metrics_data)}")
        print("=" * 60)

        if _DATABASE_AVAILABLE:
            try:
                _database.save_run_summary(
                    run_number=current_run_number,
                    total_runtime_seconds=round(total_runtime_seconds, 2),
                    average_cpu_usage=round(avg_cpu, 2),
                    peak_cpu_usage=round(peak_cpu, 2),
                    average_ram_usage=round(avg_ram, 2),
                    peak_ram_usage=round(peak_ram, 2),
                    average_disk_usage=round(avg_disk, 2),
                    peak_disk_usage=round(peak_disk, 2),
                    cpu_alerts=alert_type_counts.get("CPU", 0),
                    ram_alerts=alert_type_counts.get("RAM", 0),
                    network_alerts=alert_type_counts.get("NETWORK", 0),
                    total_samples_collected=len(system_metrics_data),
                    total_alerts=alert_count,
                    end_time=end_time,
                )
            except Exception as db_exc:
                # A database hiccup must never prevent the CSV report (the
                # primary deliverable of this function) from being returned.
                logger.error("Failed to persist run summary to database: %s", db_exc)

        return REPORT_CSV_PATH

    except Exception as exc:
        logger.error("Failed to generate summary report: %s", exc)
        return None


# ---------------------------------------------------------------------------
# Central monitoring cycle
# ---------------------------------------------------------------------------
def run_monitoring_cycle() -> Dict[str, Any]:
    """
    Execute a single, complete monitoring cycle:
        1. Collect system metrics.
        2. Collect top processes.
        3. Run the alert engine against the new metrics.
        4. Persist both metrics and process data to their respective CSVs.
        5. Update in-memory shared state.

    This is the primary entry point intended to be called repeatedly
    (e.g. on a timer/loop) by main.py or api.py.

    Returns:
        Dict[str, Any]: A dictionary containing the cycle's results:
            {
                "metrics": <dict>,
                "processes": <list of dicts>,
                "alerts": <list of dicts>,
            }
    """
    global monitoring_active, start_time, current_run_number

    try:
        if not monitoring_active:
            monitoring_active = True

        if start_time is None:
            start_time = datetime.now()

        if current_run_number == 0:
            # Defensive fallback: main.py is expected to call
            # initialize_run_number() once at startup before monitoring
            # begins, but if a cycle runs without that having happened,
            # initialize it here so run_number is never left at 0.
            initialize_run_number()

        metrics = collect_system_metrics()
        processes = collect_top_processes(limit=5)
        new_alerts = check_alerts(metrics)

        # Update shared in-memory state.
        system_metrics_data.append(metrics)
        system_processes_data.extend(processes)

        # Persist to disk.
        _append_csv_row(METRICS_CSV_PATH, METRICS_CSV_HEADERS, metrics)
        _append_csv_rows(PROCESSES_CSV_PATH, PROCESSES_CSV_HEADERS, processes)

        logger.info("Monitoring cycle complete. Alerts this cycle: %d", len(new_alerts))

        return {
            "metrics": metrics,
            "processes": processes,
            "alerts": new_alerts,
        }

    except Exception as exc:
        logger.error("Monitoring cycle failed: %s", exc)
        return {"metrics": {}, "processes": [], "alerts": []}


def stop_monitoring() -> None:
    """
    Mark the monitoring session as stopped and record the end time.
    Intended to be called by main.py/api.py when monitoring should halt,
    typically right before calling `generate_summary_report()`.
    """
    global monitoring_active, end_time

    monitoring_active = False
    end_time = datetime.now()
    logger.info("Monitoring stopped at %s", end_time.isoformat())


# ---------------------------------------------------------------------------
# Manual / standalone test entry point
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    # Allows quick manual testing of this module in isolation, e.g.:
    #     python config.py
    logger.info("Running config.py in standalone test mode...")
    initialize_run_number()
    result = run_monitoring_cycle()
    print(result)
    stop_monitoring()
    generate_summary_report()