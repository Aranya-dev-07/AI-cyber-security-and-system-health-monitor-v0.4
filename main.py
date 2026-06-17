import threading
import logging
import time
from datetime import datetime
from typing import Any

import config
import database
import api  # API can be started optionally if needed

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("main")

# Global flag for stopping the monitoring loop
input_thread_should_exit = False

def print_welcome() -> None:
    print("=" * 35)
    print("Welcome To System Health Monitor")
    print("=" * 35)

def prompt_to_start() -> None:
    while True:
        user_input = input("\nType 'start' to begin monitoring: ").strip().lower()
        if user_input == "start":
            break

def prompt_to_stop() -> None:
    global input_thread_should_exit
    while config.monitoring_active:
        user_input = input("\nType 'stop' to end monitoring and save results: ").strip().lower()
        if user_input == "stop":
            config.monitoring_active = False
            input_thread_should_exit = True
            break
    return

def display_metrics(metrics: dict) -> None:
    print("\n--- System Metrics ---")
    for key, value in metrics.items():
        print(f"{key}: {value}")

def display_processes(processes: list) -> None:
    print("\n--- Top Processes ---")
    header = "{:<6} {:<20} {:<10} {:<12}".format("PID", "Name", "CPU %", "Memory %")
    print(header)
    print("-" * len(header))
    for proc in processes:
        print("{:<6} {:<20} {:<10.2f} {:<12.2f}".format(
            proc.get('PID', 0),
            proc.get('Name', ''),
            proc.get('CPU%', 0.0),
            proc.get('Memory%', 0.0)
        ))

def display_alerts(alerts: list) -> None:
    if alerts:
        print("\n!!! ALERT(S) GENERATED !!!")
        for alert in alerts:
            print(f"ALERT: {alert}")

def monitoring_loop(test_run_id: int) -> None:
    logger.info("Monitoring loop started.")
    try:
        while config.monitoring_active:
            # Run monitoring cycle from config
            result = config.run_monitoring_cycle()
            metrics = result.get("metrics", {})
            processes = result.get("processes", [])
            alerts = result.get("alerts_generated", [])

            # Persist metrics to database
            db_metric_id = database.add_system_metric(metrics)
            # Persist processes to database, associate via metric_id
            for proc in processes:
                database.add_process_sample(proc, metric_id=db_metric_id)
            # Persist alerts to database, associate with test_run_id
            for alert in alerts:
                database.add_alert(alert, test_run_id=test_run_id, metric_id=db_metric_id)

            # Terminal output
            display_metrics(metrics)
            display_processes(processes)
            display_alerts(alerts)

            # Sleep between cycles (customize as needed, e.g., every 5 seconds)
            time.sleep(5)
    except Exception as exc:
        logger.error(f"Exception in monitoring loop: {exc}", exc_info=True)
        config.monitoring_active = False

def main() -> None:
    global input_thread_should_exit
    print_welcome()
    prompt_to_start()

    # Record monitoring start time
    start_time = datetime.now()
    config.start_time = start_time
    config.monitoring_active = True
    logger.info(f"Monitoring started at {start_time.strftime('%Y-%m-%d %H:%M:%S')}")

    # Initialize DB and create tables if not present
    try:
        database.Base.metadata.create_all(database.database_engine)
    except Exception as exc:
        logger.error(f"Failed to initialize database: {exc}", exc_info=True)
        print("Database initialization failed, exiting.")
        return

    # Create initial test_run entry
    test_run_id = database.start_test_run(start_time)

    # Start thread to watch for 'stop'
    stop_thread = threading.Thread(target=prompt_to_stop, daemon=True)
    stop_thread.start()

    # Start the monitoring loop
    monitoring_loop(test_run_id)

    # Wait for stop input (if monitoring ended via error, etc.)
    if stop_thread.is_alive():
        input_thread_should_exit = True
        stop_thread.join()

    # Record monitoring end time
    end_time = datetime.now()
    config.end_time = end_time
    logger.info(f"Monitoring stopped at {end_time.strftime('%Y-%m-%d %H:%M:%S')}")

    # Finalize DB entries
    try:
        database.end_test_run(test_run_id, end_time, total_alerts=config.alert_count)
    except Exception as exc:
        logger.error(f"Failed to update test run end time: {exc}", exc_info=True)

    # Generate and display summary report
    try:
        report = config.generate_summary_report()
        print("\n===== Summary Report =====")
        print(report)
    except Exception as exc:
        logger.error(f"Failed to generate summary report: {exc}", exc_info=True)
        print("Summary report generation failed.")

    print("\nUser has stopped data collection")
    print("\nExiting!!")
    print("\nThank You for using The System Health Monitor 😀\n")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nMonitoring interrupted by user.")
        config.monitoring_active = False
        print("\nThank You for using The System Health Monitor 😀\n")
    except Exception as exc:
        logger.error(f"Fatal error in main program: {exc}", exc_info=True)
        print("A fatal error occurred. Exiting. Please check logs.")