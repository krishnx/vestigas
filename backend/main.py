import uuid
import logging
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Annotated, Optional

from fastapi import FastAPI, Depends, BackgroundTasks, HTTPException, Query

from backend.database import engine
from backend.orm_models import Base

from backend.schemas import (
    FetchJobInput,
    JobFetchResponse,
    JobStatusOut,
    DeliveryListResponse,
    DeliveryEventOut,
)
from backend.data_access import DeliveryRepository, get_repository
from backend.partners.client import JobManager, get_job_manager

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("vestigas")


@asynccontextmanager
async def lifespan(app: FastAPI):  # noqa
    """
    Handles startup events (like database initialization) and shutdown events.
    """
    logger.info("Initializing database tables...")
    Base.metadata.create_all(bind=engine)
    logger.info("Database tables initialized successfully.")

    yield

    logger.info("Application shutting down.")


app = FastAPI(
    title="VESTIGAS Delivery Ingestion API",
    version="1.0.0",
    description="Backend service for fetching, transforming, and querying logistics data.",
    root_path="/backend/deliveries",
)


@app.post("/fetch", status_code=202, tags=["Jobs"], response_model=JobFetchResponse)
async def start_fetch_job(
        input_data: FetchJobInput,
        background_tasks: BackgroundTasks,
        repo: Annotated[DeliveryRepository, Depends(get_repository)],
        manager: Annotated[JobManager, Depends(get_job_manager)]
) -> dict:
    """
    Initiates an asynchronous delivery fetch job for a specific site and date.
    Returns a jobId to track the background operation.
    """
    job_id = str(uuid.uuid4())
    logger.info(f"Starting new fetch job: {job_id} for site {input_data.siteId}")

    job_status = repo.create_job(
        jobId=job_id,
        siteId=input_data.siteId,
        date=input_data.date
    )

    background_tasks.add_task(
        manager.run_fetch_job,
        job_id,
        input_data.siteId,
        input_data.date,
        repo
    )

    return {
        "jobId": job_id,
        "status": job_status.status,
        "message": f"Data fetch job initiated. Use the jobId '{job_id}' to check status."
    }


@app.get("/jobs/{jobId}", response_model=JobStatusOut, tags=["Jobs"])
def get_job_status(
        jobId: str,
        repo: Annotated[DeliveryRepository, Depends(get_repository)],
):
    job = repo.get_job_by_id(jobId)
    if not job:
        raise HTTPException(status_code=404, detail=f"Job with ID '{jobId}' not found.")

    site_id = job.input.get("siteId")
    date_str = job.input.get("date")

    job_dict = {
        "jobId": job.jobId,
        "status": job.status,
        "siteId": site_id,
        "date": date_str,
        "createdAt": job.createdAt,
        "finishedAt": job.finishedAt,
        "stats": job.stats,
        "error": job.error,
    }

    return JobStatusOut.model_validate(job_dict)


@app.get("/jobs/{jobId}/results", response_model=DeliveryListResponse, tags=["Jobs"])
def get_job_results(
        jobId: str,
        repo: Annotated[DeliveryRepository, Depends(get_repository)],
        limit: Annotated[int, Query(ge=1, le=100, description="Max results to return.")] = 50,
        offset: Annotated[int, Query(ge=0, description="Pagination offset.")] = 0,
):
    """Retrieve normalized delivery events associated with a specific job ID."""
    job = repo.get_job_by_id(jobId)
    if not job:
        raise HTTPException(status_code=404, detail=f"Job with ID '{jobId}' not found.")

    if job.status not in ["finished", "failed"]:
        raise HTTPException(status_code=400, detail="Job still processing. Check status first.")

    total_count, deliveries = repo.get_deliveries_by_job_id(jobId, limit, offset)

    return DeliveryListResponse(
        total_count=total_count,
        limit=limit,
        offset=offset,
        data=[DeliveryEventOut.model_validate(d) for d in deliveries],
    )


@app.get("/deliveries", response_model=DeliveryListResponse, tags=["Deliveries"])
def search_deliveries(
        repo: Annotated[DeliveryRepository, Depends(get_repository)],
        siteId: Optional[str] = Query(None, description="Filter by Site ID."),
        status: Optional[str] = Query(None,
                                      description="Filter by normalized status ('delivered', 'cancelled', etc.)."),
        min_score: Optional[float] = Query(None, ge=1.0, le=5.0,
                                           description="Filter by minimum delivery score (1.0–5.0)."),
        limit: int = Query(50, ge=1, le=100, description="Max results to return."),
        offset: int = Query(0, ge=0, description="Pagination offset."),
):
    """Query normalized deliveries based on filter criteria."""
    total_count, deliveries = repo.search_deliveries(
        siteId=siteId,
        status=status,
        min_score=min_score,
        limit=limit,
        offset=offset,
    )

    return DeliveryListResponse(
        total_count=total_count,
        limit=limit,
        offset=offset,
        data=[DeliveryEventOut.model_validate(d) for d in deliveries],
    )


@app.get("/health", tags=["System"])
def health_check():
    """Simple health endpoint."""
    return {"status": "ok", "timestamp": datetime.now(timezone.utc).isoformat()}


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8000,
        reload=True
    )
