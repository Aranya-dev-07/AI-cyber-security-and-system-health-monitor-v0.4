"""
api.py
======

FastAPI backend for the System Health Monitor project.

This module exposes monitoring data persisted in SQLite (via database.py)
over a REST API, so a frontend dashboard or other client can query system
health information.

Design note (important): every endpoint here is backed exclusively by the
database, not by config.py's in-memory state. This is deliberate. main.py
(which runs the monitoring loop) and api.py (typically served via
`uvicorn api:app --reload`) usually run as two separate OS processes, which
do NOT share Python memory. Reading from config.py's in-memory variables
inside api.py only works if both happen to run in the same process - in
the far more common case of two separate processes, those variables would
simply be empty/default in the API process. Routing everything through the
database avoids this entirely and makes the API correct regardless of how
many monitoring processes have run, or whether one is currently running.

Run with:
    uvicorn api:app --reload

Author: System Health Monitor Project
"""

import logging
from datetime import datetime
from typing import List, Optional

from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel, ConfigDict

import database

# ---------------------------------------------------------------------------
# Logging configuration
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("api")


# ---------------------------------------------------------------------------
# FastAPI app initialization
# ---------------------------------------------------------------------------
app = FastAPI(
    title="System Health Monitor API",
    description="API Backend for AI-Based System Health Monitoring Platform",
    version="1.0",
)


# ---------------------------------------------------------------------------
# Pydantic response models
# ---------------------------------------------------------------------------
class RootResponse(BaseModel):
    """Response model for the root health-check endpoint."""
    message: str


class StatusResponse(BaseModel):
    """
    Response model for /status - current monitoring state, derived from
    the database (the most recent test_run row).
    """
    monitoring_active: bool
    current_run_number: Optional[int] = None
    alert_count: int


class SystemMetricResponse(BaseModel):
    """Response model for a single system_metrics row."""
    model_config = ConfigDict(from_attributes=True)

    id: int
    run_number: Optional[int] = None
    timestamp: datetime
    cpu_usage: float
    ram_usage: float
    disk_usage: float
    network_sent: float
    network_received: float
    system_uptime: float
    running_processes: int


class SystemProcessResponse(BaseModel):
    """Response model for a single system_processes row."""
    model_config = ConfigDict(from_attributes=True)

    id: int
    run_number: Optional[int] = None
    timestamp: datetime
    process_name: str
    pid: Optional[int] = None
    cpu_usage: float
    ram_usage: float


class AlertResponse(BaseModel):
    """Response model for a single alert row."""
    model_config = ConfigDict(from_attributes=True)

    id: int
    run_number: Optional[int] = None
    timestamp: datetime
    alert_type: str
    value: float
    threshold: float


class TestRunResponse(BaseModel):
    """
    Response model for a single test_run row (a historical monitoring
    session), including its summary statistics once the run has completed.
    """
    model_config = ConfigDict(from_attributes=True)

    id: int
    run_number: Optional[int] = None
    start_time: datetime
    end_time: Optional[datetime] = None
    total_alerts: int
    total_runtime_seconds: Optional[float] = None
    average_cpu_usage: Optional[float] = None
    peak_cpu_usage: Optional[float] = None
    average_ram_usage: Optional[float] = None
    peak_ram_usage: Optional[float] = None
    average_disk_usage: Optional[float] = None
    peak_disk_usage: Optional[float] = None
    cpu_alerts: Optional[int] = None
    ram_alerts: Optional[int] = None
    network_alerts: Optional[int] = None
    total_samples_collected: Optional[int] = None


class RunSummaryResponse(BaseModel):
    """
    Response model for /report and /runs/{run_number}/summary - a
    comprehensive, human-friendly summary of a single completed (or
    in-progress) monitoring run.
    """
    run_number: Optional[int] = None
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    total_runtime_seconds: Optional[float] = None
    average_cpu_usage: Optional[float] = None
    peak_cpu_usage: Optional[float] = None
    average_ram_usage: Optional[float] = None
    peak_ram_usage: Optional[float] = None
    average_disk_usage: Optional[float] = None
    peak_disk_usage: Optional[float] = None
    total_alerts_generated: int = 0
    cpu_alerts: int = 0
    ram_alerts: int = 0
    network_alerts: int = 0
    total_samples_collected: int = 0


class ErrorResponse(BaseModel):
    """Generic error response model."""
    detail: str


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------
def _build_run_summary_response(test_run: database.TestRun) -> RunSummaryResponse:
    """
    Convert a TestRun ORM row into a RunSummaryResponse, filling in zeros
    for any statistics not yet computed (e.g. the run is still in progress).

    Args:
        test_run (database.TestRun): The ORM row to convert.

    Returns:
        RunSummaryResponse: A clean, fully-populated response model.
    """
    return RunSummaryResponse(
        run_number=test_run.run_number,
        start_time=test_run.start_time,
        end_time=test_run.end_time,
        total_runtime_seconds=test_run.total_runtime_seconds,
        average_cpu_usage=test_run.average_cpu_usage,
        peak_cpu_usage=test_run.peak_cpu_usage,
        average_ram_usage=test_run.average_ram_usage,
        peak_ram_usage=test_run.peak_ram_usage,
        average_disk_usage=test_run.average_disk_usage,
        peak_disk_usage=test_run.peak_disk_usage,
        total_alerts_generated=test_run.total_alerts or 0,
        cpu_alerts=test_run.cpu_alerts or 0,
        ram_alerts=test_run.ram_alerts or 0,
        network_alerts=test_run.network_alerts or 0,
        total_samples_collected=test_run.total_samples_collected or 0,
    )


# ---------------------------------------------------------------------------
# Endpoints - Health
# ---------------------------------------------------------------------------
@app.get("/", response_model=RootResponse, tags=["Health"])
def read_root() -> RootResponse:
    """
    Root endpoint. Confirms the API is running.

    Returns:
        RootResponse: A simple confirmation message.
    """
    return RootResponse(message="System Health Monitor API Running")


# ---------------------------------------------------------------------------
# Endpoints - Monitoring
# ---------------------------------------------------------------------------
@app.get("/status", response_model=StatusResponse, tags=["Monitoring"])
def get_status() -> StatusResponse:
    """
    Return the current monitoring status, derived from the database.

    `monitoring_active` is inferred from the most recent test_run row: a
    run is considered active if it has a start_time but no end_time yet.
    `alert_count` reflects the total alerts recorded for that most recent
    run.

    Returns:
        StatusResponse: Current monitoring state.

    Raises:
        HTTPException: 500 if the status cannot be read.
    """
    try:
        recent_runs = database.get_test_runs(limit=1)
        if not recent_runs:
            return StatusResponse(monitoring_active=False, current_run_number=None, alert_count=0)

        latest_run = recent_runs[0]
        is_active = latest_run.start_time is not None and latest_run.end_time is None

        return StatusResponse(
            monitoring_active=is_active,
            current_run_number=latest_run.run_number,
            alert_count=latest_run.total_alerts or 0,
        )
    except Exception as exc:
        logger.error("Failed to fetch status: %s", exc)
        raise HTTPException(status_code=500, detail="Failed to retrieve monitoring status.")


@app.get("/metrics", response_model=List[SystemMetricResponse], tags=["Monitoring"])
def get_metrics(
    limit: int = Query(default=10, ge=1, le=1000),
    run_number: Optional[int] = Query(default=None, description="Filter to a specific run."),
) -> List[SystemMetricResponse]:
    """
    Return the most recent system metrics from the database, newest first.

    Args:
        limit (int): Maximum number of records to return. Defaults to 10.
        run_number (Optional[int]): If provided, only return metrics from
                                     this specific run.

    Returns:
        List[SystemMetricResponse]: Most recent metric snapshots.

    Raises:
        HTTPException: 500 if the database query fails.
    """
    try:
        metrics = database.get_latest_metrics(limit=limit, run_number=run_number)
        return [SystemMetricResponse.model_validate(m) for m in metrics]
    except Exception as exc:
        logger.error("Failed to fetch metrics: %s", exc)
        raise HTTPException(status_code=500, detail="Failed to retrieve system metrics.")


@app.get("/metrics/history", response_model=List[SystemMetricResponse], tags=["Monitoring"])
def get_metrics_history(
    run_number: Optional[int] = Query(default=None, description="Filter to a specific run. Omit for all runs."),
    limit: int = Query(default=500, ge=1, le=5000),
) -> List[SystemMetricResponse]:
    """
    Return historical system metrics in chronological order (oldest first) -
    suitable for plotting CPU/RAM/disk trends over time.

    Args:
        run_number (Optional[int]): If provided, restrict history to this
                                     run. If omitted, returns history across
                                     all runs.
        limit (int): Maximum number of records to return. Defaults to 500.

    Returns:
        List[SystemMetricResponse]: Metric snapshots in chronological order.

    Raises:
        HTTPException: 500 if the database query fails.
    """
    try:
        metrics = database.get_metrics_history(run_number=run_number, limit=limit)
        return [SystemMetricResponse.model_validate(m) for m in metrics]
    except Exception as exc:
        logger.error("Failed to fetch metrics history: %s", exc)
        raise HTTPException(status_code=500, detail="Failed to retrieve metrics history.")


@app.get("/processes", response_model=List[SystemProcessResponse], tags=["Monitoring"])
def get_processes(
    limit: int = Query(default=10, ge=1, le=1000),
    run_number: Optional[int] = Query(default=None, description="Filter to a specific run."),
) -> List[SystemProcessResponse]:
    """
    Return the most recent top-process records from the database, newest
    first.

    Args:
        limit (int): Maximum number of records to return. Defaults to 10.
        run_number (Optional[int]): If provided, only return processes from
                                     this specific run.

    Returns:
        List[SystemProcessResponse]: Most recent process snapshots.

    Raises:
        HTTPException: 500 if the database query fails.
    """
    try:
        processes = database.get_latest_processes(limit=limit, run_number=run_number)
        return [SystemProcessResponse.model_validate(p) for p in processes]
    except Exception as exc:
        logger.error("Failed to fetch processes: %s", exc)
        raise HTTPException(status_code=500, detail="Failed to retrieve process records.")


@app.get("/processes/history", response_model=List[SystemProcessResponse], tags=["Monitoring"])
def get_processes_history(
    run_number: Optional[int] = Query(default=None, description="Filter to a specific run. Omit for all runs."),
    limit: int = Query(default=500, ge=1, le=5000),
) -> List[SystemProcessResponse]:
    """
    Return historical process records in chronological order (oldest first).

    Args:
        run_number (Optional[int]): If provided, restrict history to this
                                     run. If omitted, returns history across
                                     all runs.
        limit (int): Maximum number of records to return. Defaults to 500.

    Returns:
        List[SystemProcessResponse]: Process snapshots in chronological order.

    Raises:
        HTTPException: 500 if the database query fails.
    """
    try:
        processes = database.get_processes_history(run_number=run_number, limit=limit)
        return [SystemProcessResponse.model_validate(p) for p in processes]
    except Exception as exc:
        logger.error("Failed to fetch processes history: %s", exc)
        raise HTTPException(status_code=500, detail="Failed to retrieve processes history.")


@app.get("/alerts", response_model=List[AlertResponse], tags=["Monitoring"])
def get_alerts(
    run_number: Optional[int] = Query(default=None, description="Filter to a specific run. Omit for all runs."),
    limit: int = Query(default=200, ge=1, le=2000),
) -> List[AlertResponse]:
    """
    Return alerts generated by the alert engine, newest first, sourced
    directly from the database.

    Args:
        run_number (Optional[int]): If provided, only return alerts from
                                     this specific run.
        limit (int): Maximum number of records to return. Defaults to 200.

    Returns:
        List[AlertResponse]: Alert records.

    Raises:
        HTTPException: 500 if the database query fails.
    """
    try:
        alerts = database.get_alerts(run_number=run_number, limit=limit)
        return [AlertResponse.model_validate(a) for a in alerts]
    except Exception as exc:
        logger.error("Failed to fetch alerts: %s", exc)
        raise HTTPException(status_code=500, detail="Failed to retrieve alerts.")


# ---------------------------------------------------------------------------
# Endpoints - Reporting
# ---------------------------------------------------------------------------
@app.get("/report", response_model=RunSummaryResponse, tags=["Reporting"])
def get_report() -> RunSummaryResponse:
    """
    Return a comprehensive summary of the most recent monitoring run,
    sourced from the database (the test_run row with the highest
    run_number / most recent start_time).

    Returns:
        RunSummaryResponse: Summary statistics for the most recent run.

    Raises:
        HTTPException: 404 if no runs exist yet, 500 on database failure.
    """
    try:
        recent_runs = database.get_test_runs(limit=1)
        if not recent_runs:
            raise HTTPException(status_code=404, detail="No monitoring runs found yet.")

        return _build_run_summary_response(recent_runs[0])
    except HTTPException:
        raise
    except Exception as exc:
        logger.error("Failed to generate report: %s", exc)
        raise HTTPException(status_code=500, detail="Failed to generate report.")


@app.get("/runs/{run_number}/summary", response_model=RunSummaryResponse, tags=["Reporting"])
def get_run_summary(run_number: int) -> RunSummaryResponse:
    """
    Return a comprehensive summary for one specific monitoring run,
    identified by its run_number (e.g. 1, 2, 3...).

    Args:
        run_number (int): The run number to summarize.

    Returns:
        RunSummaryResponse: Summary statistics for the requested run.

    Raises:
        HTTPException: 404 if the run does not exist, 500 on database failure.
    """
    try:
        test_run = database.get_run_summary(run_number)
        if test_run is None:
            raise HTTPException(status_code=404, detail=f"Run {run_number} not found.")

        return _build_run_summary_response(test_run)
    except HTTPException:
        raise
    except Exception as exc:
        logger.error("Failed to fetch run summary for run_number=%s: %s", run_number, exc)
        raise HTTPException(status_code=500, detail="Failed to retrieve run summary.")


@app.get("/test-runs", response_model=List[TestRunResponse], tags=["Reporting"])
def get_test_runs(limit: int = Query(default=20, ge=1, le=500)) -> List[TestRunResponse]:
    """
    Return all historical monitoring runs from the database, newest first,
    including each run's summary statistics where available.

    Args:
        limit (int): Maximum number of records to return. Defaults to 20.

    Returns:
        List[TestRunResponse]: Historical test_run records.

    Raises:
        HTTPException: 500 if the database query fails.
    """
    try:
        runs = database.get_test_runs(limit=limit)
        return [TestRunResponse.model_validate(r) for r in runs]
    except Exception as exc:
        logger.error("Failed to fetch test runs: %s", exc)
        raise HTTPException(status_code=500, detail="Failed to retrieve test runs.")


# ---------------------------------------------------------------------------
# Startup event - ensure database tables exist before serving requests
# ---------------------------------------------------------------------------
@app.on_event("startup")
def on_startup() -> None:
    """
    Ensure the database is initialized (tables created) when the API starts.
    """
    try:
        database.initialize_database()
        logger.info("API startup complete. Database initialized.")
    except Exception as exc:
        logger.error("Database initialization failed on startup: %s", exc)


# ---------------------------------------------------------------------------
# Standalone run support (in addition to `uvicorn api:app --reload`)
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    import uvicorn
    logger.info("Starting System Health Monitor API via uvicorn...")
    uvicorn.run("api:app", host="127.0.0.1", port=8000, reload=True)