import psutil
import os
import csv
import time
import logging
from typing import Dict, Any, List, Tuple

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

# ======================
# Shared Variables
# ======================
monitoring_active: bool = False
start_time: float = 0.0
end_time: float = 0.0
alert_count: int = 0

cpu_threshold: float = 90.0        # percent
ram_threshold: float = 90.0        # percent
network_threshold: float = 104857600.0   # bytes/sec (default 100MB/sec)

system_metrics_data: List[Dict[str, Any]] = []
system_processes_data: List[List[Dict[str, Any]]] = []
alerts_generated: List[Dict[str, Any]] = []

METRICS_CSV = "system_metrics.csv"
PROCESSES_CSV = "system_processes.csv"
SUMMARY_CSV = "system_report.csv"

# ======================
# CSV Initialization
# ======================
def init_csv_file(filename: str, headers: List[str]) -> None:
    """
    Ensure a CSV file exists with the given headers.
    """
    try:
        if not os.path.isfile(filename):
            with open(filename, mode="w", newline='') as file:
                writer = csv.writer(file)
                writer.writerow(headers)
            logging.info(f"Created CSV file: {filename}")
    except Exception as e:
        logging.error(f"Failed to initialize {filename}: {e}")

# Initialize necessary CSV files
init_csv_file(METRICS_CSV, [
    "Timestamp", "CPU_Usage", "RAM_Usage", "Disk_Usage", "Network_Bytes_Sent", "Network_Bytes_Recv", "System_Uptime", "Total_Processes"
])
init_csv_file(PROCESSES_CSV, [
    "Timestamp", "Process_Name", "PID", "CPU_Usage", "RAM_Usage"
])

# ======================
# Metrics Collection Functions
# ======================
def get_cpu_usage() -> float:
    """
    Returns the current system-wide CPU usage percent.
    """
    try:
        return psutil.cpu_percent(interval=1)
    except Exception as e:
        logging.error(f"Error collecting CPU usage: {e}")
        return 0.0

def get_ram_usage() -> float:
    """
    Returns the current system-wide RAM usage percent.
    """
    try:
        return psutil.virtual_memory().percent
    except Exception as e:
        logging.error(f"Error collecting RAM usage: {e}")
        return 0.0

def get_disk_usage() -> float:
    """
    Returns the current disk usage percent (root).
    """
    try:
        return psutil.disk_usage('/').percent
    except Exception as e:
        logging.error(f"Error collecting Disk usage: {e}")
        return 0.0

def get_network_bytes() -> Tuple[float, float]:
    """
    Returns (bytes_sent, bytes_received) since boot.
    """
    try:
        net = psutil.net_io_counters()
        return float(net.bytes_sent), float(net.bytes_recv)
    except Exception as e:
        logging.error(f"Error collecting Network bytes: {e}")
        return (0.0, 0.0)

def get_system_uptime() -> float:
    """
    Returns system uptime in seconds.
    """
    try:
        boot = psutil.boot_time()
        return time.time() - boot
    except Exception as e:
        logging.error(f"Error collecting System uptime: {e}")
        return 0.0

def get_total_processes() -> int:
    """
    Returns total running processes.
    """
    try:
        return len(psutil.pids())
    except Exception as e:
        logging.error(f"Error collecting Process count: {e}")
        return 0

# ======================
# Composite Data Collectors
# ======================
def collect_system_metrics() -> Dict[str, Any]:
    """
    Collects all system metrics and returns them in a dictionary.
    """
    metrics: Dict[str, Any] = {}
    metrics["Timestamp"] = time.strftime("%Y-%m-%d %H:%M:%S")
    metrics["CPU_Usage"] = get_cpu_usage()
    metrics["RAM_Usage"] = get_ram_usage()
    metrics["Disk_Usage"] = get_disk_usage()
    network_sent, network_recv = get_network_bytes()
    metrics["Network_Bytes_Sent"] = network_sent
    metrics["Network_Bytes_Recv"] = network_recv
    metrics["System_Uptime"] = get_system_uptime()
    metrics["Total_Processes"] = get_total_processes()
    return metrics

def collect_top_processes() -> List[Dict[str, Any]]:
    """
    Returns the top 5 processes sorted by CPU usage, safely handling exceptions.
    """
    processes: List[Dict[str, Any]] = []
    try:
        proc_list = []
        # Pre-fetch cpu_percent for more accurate sorting
        for proc in psutil.process_iter(attrs=['pid', 'name']):
            try:
                cpu = proc.cpu_percent(interval=None)  # Non-blocking
                proc_list.append((proc, cpu))
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                continue
        time.sleep(0.7)  # Allow CPU percent to update after initial call
        for proc, _ in proc_list:
            try:
                pinfo = proc.as_dict(attrs=['pid', 'name'])
                cpu = proc.cpu_percent(interval=None)
                ram = proc.memory_percent()
                processes.append({
                    "Process_Name": pinfo.get("name", ""),
                    "PID": pinfo.get("pid", 0),
                    "CPU_Usage": cpu,
                    "RAM_Usage": ram
                })
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                continue
        # Sort by CPU usage (desc), fetch top 5
        processes.sort(key=lambda x: x["CPU_Usage"], reverse=True)
        return processes[:5]
    except Exception as e:
        logging.error(f"Error collecting top processes: {e}")
        return []

# ======================
# CSV Appending
# ======================
def append_metrics_to_csv(metrics: Dict[str, Any]) -> None:
    """
    Appends a system metrics dictionary to the CSV file.
    """
    try:
        with open(METRICS_CSV, mode='a', newline='') as file:
            writer = csv.writer(file)
            writer.writerow([
                metrics.get("Timestamp"),
                metrics.get("CPU_Usage"),
                metrics.get("RAM_Usage"),
                metrics.get("Disk_Usage"),
                metrics.get("Network_Bytes_Sent"),
                metrics.get("Network_Bytes_Recv"),
                metrics.get("System_Uptime"),
                metrics.get("Total_Processes")
            ])
    except Exception as e:
        logging.error(f"Error writing system metrics CSV: {e}")

def append_processes_to_csv(processes: List[Dict[str, Any]], timestamp: str) -> None:
    """
    Appends process data to the process CSV.
    """
    try:
        with open(PROCESSES_CSV, mode='a', newline='') as file:
            writer = csv.writer(file)
            for proc in processes:
                writer.writerow([
                    timestamp,
                    proc.get("Process_Name"),
                    proc.get("PID"),
                    proc.get("CPU_Usage"),
                    proc.get("RAM_Usage")
                ])
    except Exception as e:
        logging.error(f"Error writing system processes CSV: {e}")

# ======================
# Alert Engine
# ======================
def check_and_generate_alert(metrics: Dict[str, Any]) -> None:
    """
    Checks if thresholds are exceeded and generates/stores alerts.
    """
    global alert_count
    alert_triggered = False
    details = []

    # CPU
    if metrics.get("CPU_Usage", 0) >= cpu_threshold:
        alert_triggered = True
        details.append("CPU")

    # RAM
    if metrics.get("RAM_Usage", 0) >= ram_threshold:
        alert_triggered = True
        details.append("RAM")
        
    # Network: check difference in bytes/sec if possible
    if hasattr(check_and_generate_alert, "prev_net"):
        prev_time, prev_sent, prev_recv = check_and_generate_alert.prev_net
        seconds = time.time() - prev_time
        if seconds > 0:
            sent_rate = (metrics.get("Network_Bytes_Sent", 0) - prev_sent) / seconds
            recv_rate = (metrics.get("Network_Bytes_Recv", 0) - prev_recv) / seconds
            if sent_rate >= network_threshold or recv_rate >= network_threshold:
                alert_triggered = True
                details.append("Network")
    # store for next call
    check_and_generate_alert.prev_net = (
        time.time(),
        metrics.get("Network_Bytes_Sent", 0),
        metrics.get("Network_Bytes_Recv", 0)
    )

    if alert_triggered:
        print("You are over using your device!")
        alert = {
            "Timestamp": metrics.get("Timestamp"),
            "Alert_Type": ", ".join(details),
            "CPU_Usage": metrics.get("CPU_Usage"),
            "RAM_Usage": metrics.get("RAM_Usage"),
            "Network_Bytes_Sent": metrics.get("Network_Bytes_Sent"),
            "Network_Bytes_Recv": metrics.get("Network_Bytes_Recv")
        }
        alerts_generated.append(alert)
        alert_count += 1
        logging.warning(f"Alert triggered: {alert}")

# Initialize the prev_net attribute for the alert function
check_and_generate_alert.prev_net = (time.time(), 0.0, 0.0)

# ======================
# Summary Report Generator
# ======================
def generate_summary_report() -> None:
    """
    Generates a CSV report summarizing the system monitoring session.
    """
    try:
        if not system_metrics_data:
            logging.warning("No system metrics collected. Report will not be generated.")
            return

        avg_cpu = sum(d["CPU_Usage"] for d in system_metrics_data) / len(system_metrics_data)
        peak_cpu = max(d["CPU_Usage"] for d in system_metrics_data)
        avg_ram = sum(d["RAM_Usage"] for d in system_metrics_data) / len(system_metrics_data)
        peak_ram = max(d["RAM_Usage"] for d in system_metrics_data)
        total_runtime = end_time - start_time if start_time and end_time else 0

        with open(SUMMARY_CSV, mode="w", newline='') as file:
            writer = csv.writer(file)
            writer.writerow([
                "Start Time", "End Time", "Total Runtime (seconds)",
                "Average CPU Usage (%)", "Peak CPU Usage (%)",
                "Average RAM Usage (%)", "Peak RAM Usage (%)",
                "Total Alerts Generated", "Total Samples Collected"
            ])
            writer.writerow([
                time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(start_time)) if start_time else "",
                time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(end_time)) if end_time else "",
                "{:.2f}".format(total_runtime),
                "{:.2f}".format(avg_cpu),
                "{:.2f}".format(peak_cpu),
                "{:.2f}".format(avg_ram),
                "{:.2f}".format(peak_ram),
                alert_count,
                len(system_metrics_data)
            ])
        logging.info("Summary report generated as system_report.csv")
    except Exception as e:
        logging.error(f"Failed to generate summary report: {e}")

# ======================
# Central Monitoring Cycle
# ======================
def run_monitoring_cycle() -> Dict[str, Any]:
    """
    Central function that collects metrics, collects processes, generates alerts, writes CSV entries,
    and returns the collected data.
    """
    try:
        metrics = collect_system_metrics()
        processes = collect_top_processes()

        # Store samples
        system_metrics_data.append(metrics)
        system_processes_data.append(processes)

        # CSV Output
        append_metrics_to_csv(metrics)
        append_processes_to_csv(processes, metrics.get("Timestamp", ""))

        # Alerting
        check_and_generate_alert(metrics)

        return {
            "metrics": metrics,
            "processes": processes,
            "alerts_generated": alerts_generated,
            "alert_count": alert_count
        }
    except Exception as e:
        logging.error(f"Error in run_monitoring_cycle: {e}")
        return {}

# ======================
# Exportable functions for main.py and api.py
# ======================
__all__ = [
    "monitoring_active",
    "start_time",
    "end_time",
    "alert_count",
    "cpu_threshold",
    "ram_threshold",
    "network_threshold",
    "system_metrics_data",
    "system_processes_data",
    "alerts_generated",
    "collect_system_metrics",
    "collect_top_processes",
    "generate_summary_report",
    "run_monitoring_cycle"
]