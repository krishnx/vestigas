from datetime import datetime, timezone
from typing import Optional, Dict, Any, Tuple, Sequence

from fastapi import Depends

from sqlalchemy import select, func, Row, RowMapping
from sqlalchemy.orm import Session
from sqlalchemy.dialects.sqlite import insert as sqlite_insert

from backend.database import get_db
from backend.orm_models import Base, DeliveryEvent, JobStatus
from backend.schemas import DeliveryEventBase, JobStats


class DeliveryRepository:
    """
    Implements the Repository pattern for all database interactions.
    Handles CRUD operations, complex queries, and enforces idempotency.
    """

    def __init__(self, db: Session):
        self.db = db
        self._ensure_tables_exist()

    def _ensure_tables_exist(self):
        """Ensures that all tables defined by Base are created in the database."""
        Base.metadata.create_all(bind=self.db.bind)

    def create_job(self, jobId: str, siteId: str, date: str) -> JobStatus:
        """Creates a new job status record."""
        job = JobStatus(
            jobId=jobId,
            status="created",
            input={"siteId": siteId, "date": date},
            stats=JobStats().model_dump()
        )
        self.db.add(job)
        self.db.commit()
        self.db.refresh(job)
        return job

    def get_job_by_id(self, jobId: str) -> Optional[JobStatus]:
        """Retrieves a job status record by ID."""
        stmt = select(JobStatus).where(JobStatus.jobId == jobId)
        return self.db.execute(stmt).scalar_one_or_none()

    def update_job_status(self, jobId: str, status: str, stats_update: Optional[Dict[str, Any]] = None,
                          error: Optional[str] = None) -> Optional[JobStatus]:
        """Updates the status and optional statistics of a job."""
        job = self.get_job_by_id(jobId)
        if not job:
            return None

        job.status = status

        if stats_update:
            job.stats.update(stats_update)

        if error:
            job.error = error

        if status in ["finished", "failed"]:
            job.finishedAt = datetime.now(timezone.utc)

        self.db.commit()
        self.db.refresh(job)

        return job

    def insert_or_update_delivery_event(self, data: DeliveryEventBase, jobId: str, source_data: Dict[str, Any],
                                        data_errors: Optional[Dict[str, Any]] = None) -> DeliveryEvent:
        """
        Inserts a new DeliveryEvent or updates an existing one (Idempotency check).
        Idempotency is based on the combination of supplierDeliveryId and supplier.
        """

        # Prepare the data for UPSERT (insert or update)
        delivery_data = data.model_dump()
        delivery_data['jobId'] = jobId
        delivery_data['sourceData'] = source_data
        delivery_data['dataErrors'] = data_errors
        delivery_data['createdAt'] = datetime.now(timezone.utc)
        delivery_data['updatedAt'] = datetime.now(timezone.utc)

        stmt = sqlite_insert(DeliveryEvent).values(**delivery_data)

        on_conflict_stmt = stmt.on_conflict_do_update(
            index_elements=['supplierDeliveryId', 'supplier'],
            set_={
                'jobId': jobId,
                'updatedAt': datetime.now(timezone.utc),
            }
        ).returning(DeliveryEvent)

        result = self.db.execute(on_conflict_stmt).scalar_one()
        self.db.commit()

        return result

    def get_deliveries_by_job_id(self, jobId: str, limit: int = 50, offset: int = 0):
        """Retrieve deliveries for a job with total count and pagination."""

        base_query = select(DeliveryEvent).where(DeliveryEvent.jobId == jobId)

        total_count = self.db.execute(
            select(func.count()).select_from(DeliveryEvent).where(DeliveryEvent.jobId == jobId)
        ).scalar_one()

        deliveries = self.db.execute(
            base_query.order_by(DeliveryEvent.createdAt.desc()).limit(limit).offset(offset)
        ).scalars().all()

        return total_count, deliveries

    def search_deliveries(self, siteId=None, status=None, min_score=None, limit=50, offset=0):
        """Search deliveries with optional filters and pagination."""

        query = select(DeliveryEvent)

        if siteId:
            query = query.where(DeliveryEvent.siteId == siteId)

        if status:
            query = query.where(DeliveryEvent.status == status)

        if min_score is not None:
            query = query.where(DeliveryEvent.deliveryScore >= min_score)

        total_count = self.db.execute(
            select(func.count()).select_from(DeliveryEvent).where(
                *(cond for cond in [
                    DeliveryEvent.siteId == siteId if siteId else None,
                    DeliveryEvent.status == status if status else None,
                    DeliveryEvent.deliveryScore >= min_score if min_score is not None else None
                ] if cond is not None)
            )
        ).scalar_one()

        deliveries = self.db.execute(
            query.order_by(DeliveryEvent.createdAt.desc()).limit(limit).offset(offset)
        ).scalars().all()

        return total_count, deliveries


def get_repository(db: Session = Depends(get_db)) -> DeliveryRepository:
    """Dependency for providing the repository instance."""
    return DeliveryRepository(db)
