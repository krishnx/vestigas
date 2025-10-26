from datetime import datetime, timezone

from sqlalchemy import (
    Column, Integer, String, DateTime, Boolean, Float,
    ForeignKey, JSON, UniqueConstraint
)
from sqlalchemy.orm import relationship, DeclarativeBase
from sqlalchemy.ext.mutable import MutableDict


class Base(DeclarativeBase):
    """Base class which provides automated table name
    and common declarative properties."""
    pass


class TimestampMixin:
    """Mixin for common timestamp fields."""
    createdAt = Column(DateTime, default=datetime.now(timezone.utc), nullable=False)
    updatedAt = Column(DateTime, default=datetime.now(timezone.utc), onupdate=datetime.now(timezone.utc),
                       nullable=False)


class JobStatus(Base, TimestampMixin):
    """
    Tracks the status and statistics of a single asynchronous delivery fetch job.
    """
    __tablename__ = 'job_status'

    id = Column(Integer, primary_key=True, index=True)
    jobId = Column(String, unique=True, nullable=False, index=True)

    status = Column(String, nullable=False, default="created")

    input = Column(MutableDict.as_mutable(JSON), nullable=False)

    stats = Column(MutableDict.as_mutable(JSON), nullable=False)

    error = Column(String, nullable=True)

    finishedAt = Column(DateTime, nullable=True)

    deliveries = relationship(
        "DeliveryEvent",
        back_populates="job",
        cascade="all, delete-orphan"
    )

    def __repr__(self):
        return f"<JobStatus(jobId={self.jobId}, status={self.status})>"


class DeliveryEvent(Base, TimestampMixin):
    """
    Stores a single, normalized delivery event record.
    """
    __tablename__ = 'delivery_events'

    id = Column(Integer, primary_key=True, index=True)

    # Foreign Key to JobStatus
    jobId = Column(String, ForeignKey('job_status.jobId'), nullable=False, index=True)
    job = relationship("JobStatus", back_populates="deliveries")

    siteId = Column(String, nullable=False, index=True)
    supplier = Column(String, nullable=False, index=True)
    supplierDeliveryId = Column(String, nullable=False)  # Part of the unique constraint

    deliveredAt = Column(DateTime, nullable=False, index=True)

    status = Column(String, nullable=False, index=True)

    deliveryScore = Column(Float, nullable=False, index=True)

    isSigned = Column(Boolean, nullable=False, default=False, index=True)

    dataErrors = Column(MutableDict.as_mutable(JSON), nullable=True)

    sourceData = Column(MutableDict.as_mutable(JSON), nullable=True)

    __table_args__ = (
        UniqueConstraint('supplierDeliveryId', 'supplier', name='uix_supplier_delivery'),
    )

    def __repr__(self):
        return f"<DeliveryEvent(id={self.id}, siteId={self.siteId}, score={self.deliveryScore})>"
