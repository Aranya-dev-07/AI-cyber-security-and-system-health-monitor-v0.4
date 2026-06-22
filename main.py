"""
main.py
=======
Central orchestrator for the System Health Monitor and Cybersecurity
Monitoring Platform.

Responsibilities:
    * Display the welcome screen and command prompt.
    * Parse user commands (``start``, ``stop``, ``exit`` / ``quit``).
    * Manage the background monitoring thread (start/stop, clean shutdown).
    * Manage the background FastAPI server thread (launched once, for the
      lifetime of the program).
    * Own the database lifecycle for each run: create the ``test_run`` row
      at start, insert metrics/processes every cycle, and close out the
      run (plus reconcile CSV -> DB) at stop.

ARCHITECTURE NOTE
------------------
``main.py`` sits at the top of the import graph and imports all three
other project modules:

    config.py  <-- database.py  <-- api.py  <-- main.py

DATABASE WRITE STRATEGY NOTE
-------------------------------
``insert_system_metrics()`` and ``insert_system_processes()`` are called
**live, once per collection cycle**, inside the monitoring loop below -
not bulk-replayed at the end of the run. This keeps data durable even if
the program is killed mid-run. ``insert_test_run()`` is called exactly
once, when ``start_monitoring()`` runs, to obtain the ``run_id`` used by
every subsequent insert. When ``stop_monitoring()`` runs,
``update_test_run_end()`` closes out that row with its final end time and
alert count.

``database.sync_csv_to_database()`` is deliberately **not** called from
``stop_monitoring()``: since every row is already inserted live during
the run, re-reading the CSV files and inserting them again at stop would
duplicate every row in ``system_metrics`` and ``system_processes``.
``sync_csv_to_database()`` remains available in ``database.py`` for
manual/recovery use (e.g. reconciling the database from CSV after a crash
where live inserts were missed), but is not part of the normal run
lifecycle.

API SERVER NOTE
------------------
The FastAPI app (``api.app``) is launched once, in its own background
thread, when the program starts - independent of whether monitoring is
currently running. This lets ``/metrics``, ``/processes``, ``/runs``, and
``/summary`` be queried over HTTP at any time while the interactive
``start`` / ``stop`` CLI is used in the foreground.
"""

from __future__ import annotations

import logging
import threading
import time
from datetime import datetime
from typing import Optional

import config
import database
import api

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Module-level orchestration state
# ---------------------------------------------------------------------------
_monitoring_thread: Optional[threading.Thread] = None
_api_thread: Optional[threading.Thread] = None
_current_run_id: int = -1

API_HOST: str = "127.0.0.1"
API_PORT: int = 8000

WELCOME_BANNER: str = r"""
╔══════════════════════════════════════════════════════════════════════╗
║                                                                      ║
║   ███████╗██╗  ██╗███╗   ███╗                                        ║
║   ██╔════╝██║  ██║████╗ ████║                                        ║
║   ███████╗███████║██╔████╔██║                                        ║
║   ╚════██║██╔══██║██║╚██╔╝██║                                        ║
║   ███████║██║  ██║██║ ╚═╝ ██║                                        ║
║   ╚══════╝╚═╝  ╚═╝╚═╝     ╚═╝                                        ║
║                                                                      ║
║   ██╗  ██╗███████╗ █████╗ ██╗  ████████╗██╗  ██╗                    ║
║   ██║  ██║██╔════╝██╔══██╗██║  ╚══██╔══╝██║  ██║                    ║
║   ███████║█████╗  ███████║██║     ██║   ███████║                    ║
║   ██╔══██║██╔══╝  ██╔══██║██║     ██║   ██╔══██║                    ║
║   ██║  ██║███████╗██║  ██║███████╗██║   ██║  ██║                    ║
║   ╚═╝  ╚═╝╚══════╝╚═╝  ╚═╝╚══════╝╚═╝   ╚═╝  ╚═╝                    ║
║                                                                      ║
║   ███╗   ███╗ ██████╗ ███╗   ██╗██╗████████╗ ██████╗ ██████╗        ║
║   ████╗ ████║██╔═══██╗████╗  ██║██║╚══██╔══╝██╔═══██╗██╔══██╗       ║
║   ██╔████╔██║██║   ██║██╔██╗ ██║██║   ██║   ██║   ██║██████╔╝       ║
║   ██║╚██╔╝██║██║   ██║██║╚██╗██║██║   ██║   ██║   ██║██╔══██╗       ║
║   ██║ ╚═╝ ██║╚██████╔╝██║ ╚████║██║   ██║   ╚██████╔╝██║  ██║       ║
║   ╚═╝     ╚═╝ ╚═════╝ ╚═╝  ╚═══╝╚═╝   ╚═╝    ╚═════╝ ╚═╝  ╚═╝       ║
║                                                                      ║
╠══════════════════════════════════════════════════════════════════════╣
║                                                                      ║
║   Cybersecurity & System Health Monitoring Platform  v1.0.0          ║
║   Powered by Python 3.12 • FastAPI • SQLite • psutil                 ║
║                                                                      ║
╠══════════════════════════════════════════════════════════════════════╣
║                                                                      ║
║   Welcome, User! System is ready.                                    ║
║                                                                      ║
║   COMMANDS                                                           ║
║   ──────────────────────────────────────                             ║
║     start  →  Begin monitoring CPU, RAM, Disk & Network              ║
║     stop   →  Stop monitoring & save run summary                     ║
║     exit   →  Quit the program (auto-stops if running)               ║
║     quit   →  Quit the program (auto-stops if running)               ║
║                                                                      ║
║   API available at: http://127.0.0.1:8000/docs                       ║
║                                                                      ║
╚══════════════════════════════════════════════════════════════════════╝
"""


# ---------------------------------------------------------------------------
# CSV run header stamper
# ---------------------------------------------------------------------------
def _stamp_csv_run_header(run_number: int) -> None:
    """Write a 'Run N' separator header into every CSV file and print it.

    Called once at the start of each monitoring run, before any data rows
    are written, so it's always clear in the CSV files and terminal output
    which rows belong to which run.

    The header line is written as a comment row (prefixed with ``#``) so
    standard CSV parsers that ignore comment lines won't choke on it, while
    it remains clearly visible when the file is opened in a text editor or
    spreadsheet.

    Args:
        run_number: The 1-based run number to display (matches the
            ``test_run.id`` value assigned by the database).
    """
    header_line = f"# {'=' * 60}"
    run_label   = f"# Data for Run {run_number}  —  started {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
    footer_line = f"# {'=' * 60}"

    terminal_msg = (
        f"\n{'=' * 64}\n"
        f"  📋  Now recording data for Run {run_number}\n"
        f"  ⏱   Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
        f"  📁  Writing to: {config.CSV_METRICS_PATH}, "
        f"{config.CSV_PROCESSES_PATH}, {config.CSV_REPORT_PATH}\n"
        f"{'=' * 64}\n"
    )

    csv_files = [
        config.CSV_METRICS_PATH,
        config.CSV_PROCESSES_PATH,
        config.CSV_REPORT_PATH,
    ]

    for csv_path in csv_files:
        try:
            with open(csv_path, mode="a", encoding="utf-8") as f:
                f.write(f"{header_line}\n")
                f.write(f"{run_label}\n")
                f.write(f"{footer_line}\n")
        except OSError:
            logger.exception("Failed to write run header to '%s'.", csv_path)

    print(terminal_msg)
    logger.info("Stamped Run %d header into CSV files.", run_number)


# ---------------------------------------------------------------------------
# API server thread
# ---------------------------------------------------------------------------
def _run_api_server() -> None:
    """Run the FastAPI app (``api.app``) via uvicorn on a background thread.

    Intended to be the target of a daemon ``threading.Thread`` started
    once at program launch. Any failure to import or run uvicorn is
    logged clearly rather than crashing the whole program, since the CLI
    should remain usable even if the API server cannot start (e.g.
    uvicorn not installed in the current environment).
    """
    try:
        import uvicorn
    except ImportError:
        logger.error(
            "uvicorn is not installed; the FastAPI server will not start. "
            "Install it with 'pip install uvicorn' to enable the API."
        )
        return

    try:
        logger.info("Starting FastAPI server on http://%s:%d", API_HOST, API_PORT)
        uvicorn_config = uvicorn.Config(
            app=api.app,
            host=API_HOST,
            port=API_PORT,
            log_level="warning",
        )
        server = uvicorn.Server(uvicorn_config)
        server.run()
    except Exception:
        logger.exception("FastAPI server thread terminated unexpectedly.")


def _launch_api_server() -> None:
    """Start the API server thread exactly once, if not already running.

    The thread is created as a daemon so it never blocks program exit.
    """
    global _api_thread

    if _api_thread is not None and _api_thread.is_alive():
        logger.warning("API server thread already running; skipping relaunch.")
        return

    _api_thread = threading.Thread(target=_run_api_server, daemon=True, name="APIServerThread")
    _api_thread.start()
    logger.info("API server thread launched.")


# ---------------------------------------------------------------------------
# Monitoring loop
# ---------------------------------------------------------------------------
def _monitoring_loop(run_id: int) -> None:
    """Repeatedly collect metrics and processes until monitoring is stopped.

    Runs on its own background thread. On every cycle: collects system
    metrics and top-process data via ``config.py``, persists both to
    SQLite via ``database.py`` tagged with ``run_id``, then sleeps for
    ``config.MONITOR_INTERVAL`` seconds (minus time already spent
    collecting, to keep the cadence close to the configured interval).

    Args:
        run_id: The ``test_run.id`` to associate with every metric and
            process row inserted during this run.
    """
    logger.info("Monitoring loop started for run_id=%d.", run_id)

    while config.monitoring_active.is_set():
        cycle_start = time.monotonic()

        try:
            metric = config.collect_system_metrics()
            if metric:
                database.insert_system_metrics(metric, run_id)

            processes = config.collect_process_metrics()
            if processes:
                database.insert_system_processes(processes, run_id)

        except Exception:
            logger.exception("Unhandled error during a monitoring cycle; continuing loop.")

        elapsed = time.monotonic() - cycle_start
        sleep_time = max(0.0, config.MONITOR_INTERVAL - elapsed)

        # Sleep in small increments so stop_monitoring() (which clears the
        # Event) is noticed promptly rather than waiting out a full
        # MONITOR_INTERVAL after the user types 'stop'.
        slept = 0.0
        while slept < sleep_time and config.monitoring_active.is_set():
            time.sleep(min(0.5, sleep_time - slept))
            slept += 0.5

    logger.info("Monitoring loop exited cleanly for run_id=%d.", run_id)


# ---------------------------------------------------------------------------
# Monitoring control
# ---------------------------------------------------------------------------
def start_monitoring() -> None:
    """Begin a new monitoring run.

    Creates a new ``test_run`` row, records ``config.run_start_time``,
    resets the shared in-memory buffers and alert counter for the new
    run, sets the ``monitoring_active`` event, and starts the background
    monitoring thread.

    If monitoring is already active, logs a warning and does nothing
    further (idempotent - calling ``start`` twice will not spawn a second
    thread).
    """
    global _monitoring_thread, _current_run_id

    if config.monitoring_active.is_set():
        print("Monitoring is already running.")
        logger.warning("start_monitoring() called while monitoring was already active.")
        return

    try:
        with config.data_lock:
            config.metrics_data.clear()
            config.process_data.clear()
            config.alert_count = 0
            config.run_start_time = datetime.now()
            config.run_end_time = None

        _current_run_id = database.insert_test_run(
            start_time=config.run_start_time, end_time=None, alert_count=0
        )
        if _current_run_id == -1:
            print("Failed to start monitoring: could not create a database run record.")
            logger.error("start_monitoring() aborted: insert_test_run() returned -1.")
            return

        _stamp_csv_run_header(_current_run_id)

        config.monitoring_active.set()
        _monitoring_thread = threading.Thread(
            target=_monitoring_loop,
            args=(_current_run_id,),
            daemon=True,
            name="MonitoringThread",
        )
        _monitoring_thread.start()

        print(f"Monitoring started (run_id={_current_run_id}).")
        logger.info("start_monitoring() succeeded; run_id=%d.", _current_run_id)

    except Exception:
        logger.exception("Failed to start monitoring.")
        print("An error occurred while starting monitoring. Check the logs for details.")


def stop_monitoring() -> None:
    """Stop the current monitoring run and finalize its records.

    Clears the ``monitoring_active`` event (signalling the monitoring
    thread to exit), waits for that thread to finish its current cycle,
    records ``config.run_end_time``, generates the run summary, writes it
    to ``system_report.csv``, closes out the ``test_run`` row in the
    database, and reconciles the CSV files with the database via
    ``sync_csv_to_database()``.

    If monitoring is not currently active, logs a warning and does
    nothing further.
    """
    global _monitoring_thread, _current_run_id

    if not config.monitoring_active.is_set():
        print("Monitoring is not currently running.")
        logger.warning("stop_monitoring() called while monitoring was not active.")
        return

    try:
        config.monitoring_active.clear()

        if _monitoring_thread is not None:
            _monitoring_thread.join(timeout=config.MONITOR_INTERVAL + 5)

        with config.data_lock:
            config.run_end_time = datetime.now()
            final_alert_count = config.alert_count

        summary = config.generate_run_summary(run_id=_current_run_id)
        if summary:
            config.save_report_to_csv(summary)

        if _current_run_id != -1:
            database.update_test_run_end(
                run_id=_current_run_id,
                end_time=config.run_end_time,
                alert_count=final_alert_count,
            )
            # NOTE: sync_csv_to_database() is intentionally NOT called here.
            # insert_system_metrics() / insert_system_processes() already
            # persist every row live, once per collection cycle, inside
            # _monitoring_loop(). Calling sync_csv_to_database() here as
            # well would re-insert the same rows a second time (read back
            # from the CSV files), producing duplicates in system_metrics
            # and system_processes. sync_csv_to_database() remains
            # available in database.py for manual/recovery use (e.g.
            # reconciling the DB after a crash where live inserts were
            # missed), but the normal stop flow relies solely on the
            # live inserts already performed during the run.

        print("User has stopped data collection")
        print("Exiting!!")
        logger.info("stop_monitoring() completed for run_id=%d.", _current_run_id)

    except Exception:
        logger.exception("Failed to stop monitoring cleanly.")
        print("An error occurred while stopping monitoring. Check the logs for details.")


# ---------------------------------------------------------------------------
# Program entry point
# ---------------------------------------------------------------------------
def _handle_command(command: str) -> bool:
    """Process a single user command.

    Args:
        command: The raw command string entered by the user.

    Returns:
        ``True`` if the program should continue running, ``False`` if the
        program should exit.
    """
    normalized = command.strip().lower()

    if normalized == "start":
        start_monitoring()
    elif normalized == "stop":
        stop_monitoring()
    elif normalized in ("exit", "quit"):
        if config.monitoring_active.is_set():
            stop_monitoring()
        print("Thank You for using The System Health Monitor \U0001F600")
        return False
    elif normalized == "":
        pass
    else:
        print(f"Unrecognized command: '{command}'. Valid commands: start, stop, exit, quit.")

    return True


def main() -> None:
    """Program entry point: display the welcome screen and run the CLI loop.

    Initializes the database schema, launches the FastAPI server in a
    background thread, then repeatedly prompts the user for commands
    until ``exit`` or ``quit`` is entered (or the process receives a
    keyboard interrupt), at which point any active monitoring run is
    stopped cleanly before the program exits.
    """
    try:
        database.initialize_database()
    except Exception:
        logger.exception("Failed to initialize the database at startup.")
        print("Warning: database initialization failed. Check the logs for details.")

    _launch_api_server()

    print(WELCOME_BANNER)

    try:
        running = True
        while running:
            try:
                command = input("> ")
            except EOFError:
                # No more input available (e.g. piped stdin exhausted).
                break
            running = _handle_command(command)

    except KeyboardInterrupt:
        print("\nKeyboard interrupt received.")
        if config.monitoring_active.is_set():
            stop_monitoring()
        print("Thank You for using The System Health Monitor \U0001F600")

    except Exception:
        logger.exception("Unhandled exception in main program loop.")
        if config.monitoring_active.is_set():
            stop_monitoring()
        print("An unexpected error occurred. Exiting. Check the logs for details.")


if __name__ == "__main__":
    main()
    