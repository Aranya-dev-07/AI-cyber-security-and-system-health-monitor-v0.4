"""
api.py
======
FastAPI REST interface for the System Health Monitor and Cybersecurity
Monitoring Platform.

Exposes monitoring data collected by ``config.py`` and persisted by
``database.py`` over HTTP, with interactive Swagger (``/docs``) and Redoc
(``/redoc``) documentation enabled by default.

ARCHITECTURE NOTE
------------------
``api.py`` imports from both ``config.py`` and ``database.py``:

    config.py  <-- database.py  <-- api.py  <-- main.py

DATA SOURCE NOTE (per project decision)
-----------------------------------------
Endpoints are intentionally split between two data sources:

    * ``/metrics`` and ``/processes`` read the shared **in-memory** state
      (``config.metrics_data`` / ``config.process_data``) for the lowest
      possible latency on "what is happening right now" queries.
    * ``/runs`` reads from **SQLite** via ``database.get_run_history()``,
      since run history is inherently a durable, historical record.
    * ``/summary`` has no dedicated database table (only a CSV report is
      ever produced), so it is computed live via
      ``config.generate_run_summary()`` against the current in-memory
      ``metrics_data`` / ``run_start_time`` / ``run_end_time`` /
      ``alert_count``.

This file never duplicates SQL or schema logic - all database access goes
through the functions already defined in ``database.py``.

NO MONITORING CONTROL ENDPOINTS
----------------------------------
This API is read-only by design (per the locked endpoint list:
``/health``, ``/metrics``, ``/processes``, ``/runs``, ``/summary``).
Starting and stopping the monitoring loop is controlled exclusively by
``main.py`` calling ``start_monitoring()`` / ``stop_monitoring()``
directly - there are no ``POST /monitoring/start`` or
``POST /monitoring/stop`` routes here. If remote start/stop control is
ever needed, two additional endpoints can be added following the same
pattern used below.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

from fastapi import FastAPI, HTTPException, status
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

import config
import database

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# FastAPI application
# ---------------------------------------------------------------------------
app = FastAPI(
    title="System Health Monitor & Cybersecurity Monitoring Platform",
    description=(
        "REST API exposing live and historical CPU, RAM, disk, network, "
        "and process monitoring data, plus run history and run summaries."
    ),
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
)

# Permissive CORS so any frontend (dev or otherwise) can consume this API.
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ---------------------------------------------------------------------------
# Pydantic response models
# ---------------------------------------------------------------------------
class HealthResponse(BaseModel):
    """Response model for the liveness check endpoint."""

    status: str = Field(..., description="Service health status.", examples=["healthy"])


class MetricResponse(BaseModel):
    """Response model for a single system metric snapshot."""

    timestamp: str = Field(..., description="ISO-8601 timestamp of the snapshot.")
    cpu_percent: float = Field(..., description="CPU utilisation, in percent.")
    ram_percent: float = Field(..., description="RAM utilisation, in percent.")
    disk_percent: float = Field(..., description="Disk utilisation, in percent.")
    net_sent_mb: float = Field(..., description="MB sent since the previous cycle.")
    net_recv_mb: float = Field(..., description="MB received since the previous cycle.")


class ProcessResponse(BaseModel):
    """Response model for a single process snapshot entry."""

    timestamp: str = Field(..., description="ISO-8601 timestamp of the snapshot.")
    pid: int = Field(..., description="Process ID.")
    name: str = Field(..., description="Process name.")
    cpu_percent: float = Field(..., description="Process CPU utilisation, in percent.")
    memory_percent: float = Field(..., description="Process memory utilisation, in percent.")
    status: str = Field(..., description="Process status (e.g. 'running', 'sleeping').")


class RunHistoryResponse(BaseModel):
    """Response model for a single historical monitoring run."""

    id: int = Field(..., description="Unique run identifier.")
    start_time: str = Field(..., description="ISO-8601 timestamp the run started.")
    end_time: Optional[str] = Field(None, description="ISO-8601 timestamp the run ended.")
    duration_seconds: Optional[float] = Field(None, description="Run duration, in seconds.")
    alert_count: int = Field(..., description="Total alerts raised during the run.")


class RunSummaryResponse(BaseModel):
    """Response model for the current/most recent run summary."""

    run_id: int = Field(..., description="Identifier of the run this summary covers.")
    start_time: str = Field(..., description="ISO-8601 timestamp the run started.")
    end_time: str = Field(..., description="ISO-8601 timestamp the run ended (or 'now').")
    duration_sec: float = Field(..., description="Run duration, in seconds.")
    avg_cpu: float = Field(..., description="Average CPU utilisation across the run.")
    avg_ram: float = Field(..., description="Average RAM utilisation across the run.")
    avg_disk: float = Field(..., description="Average disk utilisation across the run.")
    total_alerts: int = Field(..., description="Total alerts raised during the run.")


class ErrorResponse(BaseModel):
    """Standard error response shape for documented error cases."""

    detail: str = Field(..., description="Human-readable error message.")


# ---------------------------------------------------------------------------
# Startup: ensure the database schema exists before serving requests
# ---------------------------------------------------------------------------
@app.on_event("startup")
async def on_startup() -> None:
    """Initialize the database schema when the API starts.

    Calling this here (in addition to ``main.py``) makes ``api.py`` safe
    to run standalone (e.g. via ``uvicorn api:app``) without depending on
    ``main.py`` having run first.
    """
    try:
        database.initialize_database()
        logger.info("API startup complete: database initialized.")
    except Exception:
        logger.exception("API startup failed during database initialization.")


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------
@app.get(
    "/health",
    response_model=HealthResponse,
    tags=["Health"],
    summary="Liveness check",
)
async def get_health() -> HealthResponse:
    """Return a simple liveness indicator for the API service.

    Returns:
        ``{"status": "healthy"}`` if the service is up and responding.
    """
    return HealthResponse(status="healthy")


@app.get(
    "/metrics",
    response_model=MetricResponse,
    tags=["Metrics"],
    summary="Get the latest system metrics",
    responses={404: {"model": ErrorResponse, "description": "No metrics collected yet."}},
)
async def get_metrics() -> MetricResponse:
    """Return the most recently collected system metric snapshot.

    Reads directly from the shared in-memory ``config.metrics_data`` list
    (not the database) so the response reflects the absolute latest
    collection cycle with no query latency.

    Returns:
        The most recent :class:`MetricResponse`.

    Raises:
        HTTPException: 404 if no metrics have been collected yet (i.e. the
            monitoring loop has not started or has not completed a cycle).
    """
    try:
        with config.data_lock:
            if not config.metrics_data:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail="No metrics data available yet. Start monitoring first.",
                )
            latest = config.metrics_data[-1]

        return MetricResponse(**latest)

    except HTTPException:
        raise
    except Exception:
        logger.exception("Failed to retrieve latest metrics.")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="An unexpected error occurred while retrieving metrics.",
        )


@app.get(
    "/processes",
    response_model=List[ProcessResponse],
    tags=["Processes"],
    summary="Get the latest top-process snapshot",
    responses={404: {"model": ErrorResponse, "description": "No process data collected yet."}},
)
async def get_processes() -> List[ProcessResponse]:
    """Return the most recently collected top-process snapshot.

    Reads directly from the shared in-memory ``config.process_data`` list
    (not the database) so the response reflects the absolute latest
    collection cycle with no query latency.

    Returns:
        A list of :class:`ProcessResponse`, one per top process from the
        most recent collection cycle (sorted by CPU usage, descending).

    Raises:
        HTTPException: 404 if no process data has been collected yet.
    """
    try:
        with config.data_lock:
            if not config.process_data:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail="No process data available yet. Start monitoring first.",
                )
            latest_snapshot = config.process_data[-1]

        processes = latest_snapshot.get("processes", [])
        return [ProcessResponse(**proc) for proc in processes]

    except HTTPException:
        raise
    except Exception:
        logger.exception("Failed to retrieve latest process data.")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="An unexpected error occurred while retrieving process data.",
        )


@app.get(
    "/runs",
    response_model=List[RunHistoryResponse],
    tags=["Runs"],
    summary="Get test run history",
)
async def get_runs(limit: int = 50) -> List[RunHistoryResponse]:
    """Return historical monitoring run records from the database.

    Args:
        limit: Maximum number of runs to return, most recent first.
            Defaults to ``50``.

    Returns:
        A list of :class:`RunHistoryResponse`, ordered from most recent to
        least recent. Returns an empty list if no runs have been recorded.
    """
    try:
        runs = database.get_run_history(limit=limit)
        return [RunHistoryResponse(**run) for run in runs]

    except Exception:
        logger.exception("Failed to retrieve run history.")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="An unexpected error occurred while retrieving run history.",
        )


@app.get(
    "/summary",
    response_model=RunSummaryResponse,
    tags=["Summary"],
    summary="Get the current/most recent run summary",
    responses={404: {"model": ErrorResponse, "description": "No run data available yet."}},
)
async def get_summary() -> RunSummaryResponse:
    """Return aggregate statistics for the current (or most recent) run.

    Computed live via ``config.generate_run_summary()`` against the
    current in-memory ``metrics_data``, ``run_start_time``,
    ``run_end_time``, and ``alert_count`` - there is no dedicated database
    table for summaries, only the CSV report written at the end of a run.

    Returns:
        The computed :class:`RunSummaryResponse`.

    Raises:
        HTTPException: 404 if no metrics have been collected yet, meaning
            no summary can be computed.
    """
    try:
        summary = config.generate_run_summary()

        if not summary:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="No run summary available yet. Start monitoring first.",
            )

        return RunSummaryResponse(**summary)

    except HTTPException:
        raise
    except Exception:
        logger.exception("Failed to generate run summary.")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="An unexpected error occurred while generating the run summary.",
        )