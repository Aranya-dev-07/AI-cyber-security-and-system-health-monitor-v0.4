"""
main.py
=======

Central orchestrator for the System Health Monitor project.

main.py is the entry point of the application. It is responsible for:
    - Greeting the user and waiting for a 'start' command before any
      monitoring begins.
    - Initializing the database (via database.py).
    - Running the monitoring loop on a background thread (using
      run_monitoring_cycle() from config.py), while listening for a 'stop'
      command on the main thread at the same time.
    - Persisting every collected metric/process/alert to SQLite immediately
      via database.py's save functions.
    - Printing metrics, top processes, and alerts to the terminal as they
      are collected.
    - Cleanly shutting down monitoring on 'stop': recording end_time, saving
      the final test_run entry, and generating the summary report.

Note on api.py: api.py is a FastAPI app meant to be served independently via
    uvicorn api:app --reload
It is not started as a subprocess from here, since FastAPI servers are
normally run by an ASGI server (uvicorn), not invoked as a plain function
call. main.py still imports it (per the required architecture) so that any
import-time errors in api.py are caught immediately, and so the same
config/database state is available for the API to read from while
monitoring runs separately.

Author: System Health Monitor Project
"""

import logging
import threading
import time
from datetime import datetime
from typing import Optional

import config
import database

# api.py is imported to satisfy the required architecture (main -> config ->
# database -> api) and to surface any import-time errors early. The FastAPI
# app itself is run separately via `uvicorn api:app --reload`, not from here.
import api  # noqa: F401  (imported for integration/validation, not directly used)


# ---------------------------------------------------------------------------
# Logging configuration
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("main")


# ---------------------------------------------------------------------------
# Module-level state for thread coordination
# ---------------------------------------------------------------------------
# Tracks the database id of the current test_run row, so it can be updated
# (end_time, total_alerts) when monitoring stops.
_current_test_run_id: Optional[int] = None

# Seconds to wait between monitoring cycles. Kept short so the monitoring
# thread also checks `config.monitoring_active` frequently and stops promptly.
MONITORING_INTERVAL_SECONDS: float = 2.0


# ---------------------------------------------------------------------------
# Display helpers
# ---------------------------------------------------------------------------
def display_welcome_banner() -> None:
    """Print the startup welcome banner."""
    print("=" * 60)
    print("Welcome To System Health Monitor")
    print("=" * 60)


def display_metrics(metrics: dict) -> None:
    """
    Print a single collected metrics snapshot to the terminal in a
    readable format.

    Args:
        metrics (dict): A metrics dictionary as returned by
                         config.collect_system_metrics().
    """
    try:
        print("\n--- System Metrics ---")
        print(f"Run Number           : {metrics.get('run_number')}")
        print(f"Timestamp           : {metrics.get('timestamp')}")
        print(f"CPU Usage            : {metrics.get('cpu_usage_percent')}%")
        print(f"RAM Usage            : {metrics.get('ram_usage_percent')}%")
        print(f"Disk Usage           : {metrics.get('disk_usage_percent')}%")
        print(f"Network Sent         : {metrics.get('network_bytes_sent')} bytes")
        print(f"Network Received     : {metrics.get('network_bytes_received')} bytes")
        print(f"System Uptime        : {metrics.get('system_uptime_seconds')} sec")
        print(f"Running Processes    : {metrics.get('total_running_processes')}")
    except Exception as exc:
        logger.error("Failed to display metrics: %s", exc)


def display_processes(processes: list) -> None:
    """
    Print the top processes collected during a monitoring cycle.

    Args:
        processes (list): List of process info dictionaries as returned by
                           config.collect_top_processes().
    """
    try:
        print("\n--- Top Processes (by CPU usage) ---")
        if not processes:
            print("No process data available.")
            return
        for proc in processes:
            print(
                f"  PID={proc.get('pid'):<8} "
                f"Name={proc.get('process_name'):<20} "
                f"CPU={proc.get('cpu_usage_percent')}%  "
                f"RAM={proc.get('ram_usage_percent')}%"
            )
    except Exception as exc:
        logger.error("Failed to display processes: %s", exc)


def display_alerts(alerts: list) -> None:
    """
    Print any newly generated alerts to the terminal immediately.

    Args:
        alerts (list): List of alert dictionaries generated this cycle.
    """
    try:
        if not alerts:
            return
        print("\n*** ALERTS ***")
        for alert in alerts:
            print(
                f"  [ALERT] {alert.get('type')} usage = {alert.get('value')} "
                f"exceeded threshold = {alert.get('threshold')} "
                f"at {alert.get('timestamp')}"
            )
    except Exception as exc:
        logger.error("Failed to display alerts: %s", exc)


# ---------------------------------------------------------------------------
# Persistence helper - bridges config.py's output to database.py's writers
# ---------------------------------------------------------------------------
def persist_cycle_data(metrics: dict, processes: list) -> None:
    """
    Persist a single monitoring cycle's metrics and processes to SQLite,
    using the save functions defined in database.py.

    Args:
        metrics (dict): Metrics dictionary from config.collect_system_metrics().
        processes (list): List of process dictionaries from
                           config.collect_top_processes().
    """
    try:
        database.save_system_metric(
            cpu_usage=metrics.get("cpu_usage_percent", 0.0),
            ram_usage=metrics.get("ram_usage_percent", 0.0),
            disk_usage=metrics.get("disk_usage_percent", 0.0),
            network_sent=metrics.get("network_bytes_sent", 0.0),
            network_received=metrics.get("network_bytes_received", 0.0),
            system_uptime=metrics.get("system_uptime_seconds", 0.0),
            running_processes=metrics.get("total_running_processes", 0),
        )
    except Exception as exc:
        logger.error("Failed to persist system metric to database: %s", exc)

    try:
        if processes:
            database.save_system_processes_bulk(processes)
    except Exception as exc:
        logger.error("Failed to persist processes to database: %s", exc)


# ---------------------------------------------------------------------------
# Monitoring loop (runs on a background thread)
# ---------------------------------------------------------------------------
def monitoring_loop() -> None:
    """
    Background-thread target function. Repeatedly runs monitoring cycles
    (collect metrics/processes/alerts, persist to DB, display to terminal)
    until config.monitoring_active becomes False (set by the 'stop' command
    on the main thread).
    """
    logger.info("Monitoring thread started.")

    while config.monitoring_active:
        try:
            cycle_result = config.run_monitoring_cycle()
            metrics = cycle_result.get("metrics", {})
            processes = cycle_result.get("processes", [])
            alerts = cycle_result.get("alerts", [])

            persist_cycle_data(metrics, processes)

            display_metrics(metrics)
            display_processes(processes)
            display_alerts(alerts)

        except Exception as exc:
            logger.error("Error during monitoring cycle: %s", exc)

        # Sleep in short slices so a 'stop' command takes effect promptly
        # instead of waiting out a long interval.
        slept = 0.0
        while slept < MONITORING_INTERVAL_SECONDS and config.monitoring_active:
            time.sleep(0.2)
            slept += 0.2

    logger.info("Monitoring thread exiting (monitoring_active is False).")


# ---------------------------------------------------------------------------
# Start / stop orchestration
# ---------------------------------------------------------------------------
def start_monitoring() -> threading.Thread:
    """
    Begin a monitoring session:
        - Record start_time
        - Initialize the database
        - Create a test_run row
        - Launch the monitoring loop on a background thread

    Returns:
        threading.Thread: The running monitoring thread (already started).
    """
    global _current_test_run_id

    try:
        config.monitoring_active = True
        config.start_time = datetime.now()
        config.end_time = None

        logger.info("Initializing database...")
        database.initialize_database()

        _current_test_run_id = database.save_test_run(
            start_time=config.start_time,
            total_alerts=0,
        )
        logger.info("Created test_run with id=%s", _current_test_run_id)

    except Exception as exc:
        logger.error("Failed during monitoring startup sequence: %s", exc)

    monitor_thread = threading.Thread(target=monitoring_loop, name="MonitoringThread", daemon=True)
    monitor_thread.start()
    logger.info("Monitoring started at %s", config.start_time)
    return monitor_thread


def stop_monitoring_session(monitor_thread: threading.Thread) -> None:
    """
    Stop the current monitoring session:
        - Set monitoring_active = False
        - Record end_time
        - Wait for the monitoring thread to finish its current cycle
        - Save the final test_run entry (end_time, total_alerts)
        - Generate the summary report

    Args:
        monitor_thread (threading.Thread): The background monitoring thread
                                            to wait on before finishing up.
    """
    global _current_test_run_id

    try:
        config.monitoring_active = False
        config.end_time = datetime.now()
        logger.info("Stop command received. Stopping monitoring at %s", config.end_time)

        # Give the monitoring thread a moment to exit its loop cleanly.
        monitor_thread.join(timeout=MONITORING_INTERVAL_SECONDS + 2.0)

        if _current_test_run_id is not None:
            database.save_test_run(
                end_time=config.end_time,
                total_alerts=config.alert_count,
                run_id=_current_test_run_id,
            )
        else:
            # Fallback: if no test_run id was captured at start, save a new
            # complete record now so the session is not lost.
            database.save_test_run(
                start_time=config.start_time,
                end_time=config.end_time,
                total_alerts=config.alert_count,
            )

        config.generate_summary_report()

    except Exception as exc:
        logger.error("Error during monitoring shutdown sequence: %s", exc)


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------
def main() -> None:
    """
    Main entry point for the System Health Monitor application.

    Flow:
        1. Display the welcome banner.
        2. Wait for the user to type 'start' (ignoring anything else).
        3. Start monitoring on a background thread.
        4. Listen on the main thread for a 'stop' command.
        5. On 'stop', cleanly shut everything down and exit.
    """
    display_welcome_banner()

    # Determine and announce this execution's run number immediately, before
    # waiting for 'start'. The number is persisted by config.py so it keeps
    # incrementing across separate program executions.
    config.initialize_run_number()

    # Step 1: Wait for 'start'. Monitoring must not begin until the user
    # explicitly types 'start'.
    while True:
        user_input = input("Type 'start' to begin monitoring: ").strip().lower()
        if user_input == "start":
            break
        print("Invalid input. Please type 'start' to begin monitoring.")

    monitor_thread = start_monitoring()

    # Step 2: Listen for 'stop' on the main thread while monitoring runs
    # concurrently on the background thread.
    print("\nMonitoring is now running. Type 'stop' at any time to end the session.\n")
    while True:
        user_input = input().strip().lower()
        if user_input == "stop":
            break
        elif user_input:
            print("Unrecognized command. Type 'stop' to end monitoring.")

    stop_monitoring_session(monitor_thread)

    print("\nUser has stopped data collection")
    print("Exiting!!")
    print("Thank You for using The System Health Monitor \U0001F600")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.info("Interrupted by user (Ctrl+C). Shutting down...")
        config.monitoring_active = False
        config.end_time = datetime.now()
        try:
            config.generate_summary_report()
        except Exception as exc:
            logger.error("Failed to generate summary report during interrupt shutdown: %s", exc)
        print("\nExiting!!")
        print("Thank You for using The System Health Monitor \U0001F600")
    except Exception as exc:
        logger.critical("Fatal error in main application: %s", exc)