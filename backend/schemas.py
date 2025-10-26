from datetime import datetime
from typing import Optional, Dict, Any, List
from pydantic import BaseModel, Field, field_validator


class ConfiguredBaseModel(BaseModel):
    model_config = {
        "arbitrary_types_allowed": True,
        "from_attributes": True,
    }


class FetchJobInput(ConfiguredBaseModel):
    """Input for starting a new delivery fetch job."""
    siteId: str = Field(..., description="The target construction site ID.")
    date: str = Field(..., description="The date for which to fetch deliveries (format YYYY-MM-DD).")

    @field_validator('date')
    @classmethod
    def date_must_be_iso(cls, v):
        try:
            datetime.strptime(v, '%Y-%m-%d')
        except ValueError:
            raise ValueError("Date must be in YYYY-MM-DD format.")
        return v


class DeliveryEventBase(ConfiguredBaseModel):
    """Base schema for a normalized delivery event."""
    siteId: str = Field(..., description="Normalized ID of the construction site.")
    supplier: str = Field(..., description="The name of the logistics partner/supplier.")
    supplierDeliveryId: str = Field(..., description="The original unique ID from the supplier's system.")
    deliveredAt: datetime = Field(..., description="Normalized UTC timestamp of delivery.")
    status: str = Field(..., description="Normalized status ('delivered', 'cancelled', 'pending', 'other').")
    deliveryScore: float = Field(..., description="VESTIGAS quality score (1.0 to 5.0).")
    isSigned: bool = Field(False, description="Whether a proof of delivery/signature was recorded.")


class DeliveryEventOut(DeliveryEventBase):
    """Output schema for a stored delivery event, including audit fields."""
    id: int
    jobId: str = Field(..., description="The job ID that initiated this record.")
    createdAt: datetime
    updatedAt: datetime
    dataErrors: Optional[Dict[str, Any]] = Field(None,
                                                 description="Any validation or transformation errors encountered.")
    sourceData: Optional[Dict[str, Any]] = Field(None, description="The raw data fetched from the partner API.")


class PartnerStats(ConfiguredBaseModel):
    fetched: int = 0
    transformed: int = 0
    errors: int = 0
    error_message: Optional[str] = None


class JobStats(ConfiguredBaseModel):
    """Overall statistics for the entire fetch job."""
    Partner_A: PartnerStats = PartnerStats()
    Partner_B: PartnerStats = PartnerStats()
    stored: int = 0
    total_fetched: int = 0


class JobStatusBase(ConfiguredBaseModel):
    """Base schema for job status tracking."""
    jobId: str = Field(..., description="Unique ID for the job.")
    status: str = Field(..., description="Job status: 'created', 'processing', 'finished', 'failed'.")
    siteId: str = Field(..., description="Input site ID.")
    date: str = Field(..., description="Input date.")


class JobStatusOut(JobStatusBase):
    """Complete output schema for job status."""
    createdAt: datetime
    finishedAt: Optional[datetime] = None
    stats: Optional[JobStats]
    error: Optional[str] = None


class JobFetchResponse(ConfiguredBaseModel):
    """Response returned when initiating a new fetch job."""
    jobId: Optional[str] = None
    status: Optional[str] = None
    message: Optional[str] = None

    model_config = {
        "arbitrary_types_allowed": True
    }


class DeliveryListResponse(ConfiguredBaseModel):
    """Response returned for general delivery queries."""
    total_count: Optional[int] = Field(..., description="Total number of records matching the query.")
    limit: Optional[int] = Field(..., description="The maximum number of records requested.")
    offset: Optional[int] = Field(..., description="The starting offset for pagination.")
    data: List[DeliveryEventOut]
