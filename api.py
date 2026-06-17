from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from typing import List, Optional, Any
import logging

import database
import config

app = FastAPI(
    title="System Health Monitor API",
    description="API Backend for AI-Based System Health Monitoring Platform",
    version="1.0"
)

logger = logging.getLogger("api")

# ========================
# Pydantic Response Models
# ========================

class StatusResponse(BaseModel):
    monitoring_active: bool
    alert_count: int

class SystemMetricOut(BaseModel):
    id: int
    timestamp: str
    cpu_usage: float
    ram_usage: float
    network_sent: float
    network_recv: float

class ProcessOut(BaseModel):
    id: int
    timestamp: str
    pid: int
    name: str
    cpu_percent: float
    memory_percent: float

class AlertOut(BaseModel):
    alerts: List[Any]

class ReportOut(BaseModel):
    summary: str
    system_stats: dict
    processes: list

class TestRunOut(BaseModel):
    id: int
    start_time: str
    end_time: Optional[str]
    total_alerts: int

# ====================
# API Endpoints
# ====================

@app.get("/", response_model=dict)
async def root():
    return {"message": "System Health Monitor API Running"}


@app.get("/status", response_model=StatusResponse)
async def get_status():
    try:
        return StatusResponse(
            monitoring_active=bool(getattr(config, "monitoring_active", False)),
            alert_count=int(getattr(config, "alert_count", 0))
        )
    except Exception as e:
        logger.error(f"Error retrieving status: {e}")
        raise HTTPException(status_code=500, detail="Error retrieving status.")


@app.get("/metrics", response_model=SystemMetricOut)
async def get_latest_metrics():
    try:
        db = database.SessionLocal()
        metric = (
            db.query(database.SystemMetric)
            .order_by(database.SystemMetric.timestamp.desc())
            .first()
        )
        db.close()
        if metric is None:
            raise HTTPException(status_code=404, detail="No metrics found.")
        return SystemMetricOut(
            id=metric.id,
            timestamp=metric.timestamp.isoformat(),
            cpu_usage=metric.cpu_usage,
            ram_usage=metric.ram_usage,
            network_sent=metric.network_sent,
            network_recv=metric.network_recv
        )
    except Exception as e:
        logger.error(f"Error retrieving metrics: {e}")
        raise HTTPException(status_code=500, detail="Error retrieving latest metrics.")


@app.get("/processes", response_model=List[ProcessOut])
async def get_latest_processes():
    try:
        db = database.SessionLocal()
        # Find latest metrics timestamp
        latest_metrics = (
            db.query(database.SystemMetric)
            .order_by(database.SystemMetric.timestamp.desc())
            .first()
        )
        if not latest_metrics:
            db.close()
            raise HTTPException(status_code=404, detail="No process records found.")
        latest_time = latest_metrics.timestamp
        # Select all processes for that timestamp (assuming you store timestamp on processes)
        processes = (
            db.query(database.TopProcess)
            .filter(database.TopProcess.timestamp == latest_time)
            .all()
        )
        db.close()
        if not processes:
            raise HTTPException(status_code=404, detail="No process records found.")
        return [
            ProcessOut(
                id=p.id,
                timestamp=p.timestamp.isoformat(),
                pid=p.pid,
                name=p.name,
                cpu_percent=p.cpu_percent,
                memory_percent=p.memory_percent
            )
            for p in processes
        ]
    except Exception as e:
        logger.error(f"Error retrieving processes: {e}")
        raise HTTPException(status_code=500, detail="Error retrieving processes.")


@app.get("/alerts", response_model=AlertOut)
async def get_alerts():
    try:
        alerts = getattr(config, "alerts_generated", [])
        return AlertOut(alerts=alerts)
    except Exception as e:
        logger.error(f"Error retrieving alerts: {e}")
        raise HTTPException(status_code=500, detail="Error retrieving alerts.")


@app.get("/report", response_model=ReportOut)
async def get_report():
    try:
        report = config.generate_summary_report()
        if not isinstance(report, dict):
            raise HTTPException(status_code=500, detail="Report format error.")
        return ReportOut(
            summary=report.get("summary", ""),
            system_stats=report.get("system_stats", {}),
            processes=report.get("processes", [])
        )
    except Exception as e:
        logger.error(f"Error generating report: {e}")
        raise HTTPException(status_code=500, detail="Error generating report.")


@app.get("/test-runs", response_model=List[TestRunOut])
async def get_test_runs():
    try:
        db = database.SessionLocal()
        runs = db.query(database.TestRun).order_by(database.TestRun.start_time.desc()).all()
        db.close()
        if not runs:
            return []
        return [
            TestRunOut(
                id=r.id,
                start_time=r.start_time.isoformat(),
                end_time=r.end_time.isoformat() if r.end_time else None,
                total_alerts=r.total_alerts
            )
            for r in runs
        ]
    except Exception as e:
        logger.error(f"Error retrieving test runs: {e}")
        raise HTTPException(status_code=500, detail="Error retrieving test runs.")