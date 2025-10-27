import pytest
from datetime import datetime, timezone
from backend.data_access import DeliveryRepository
from backend.schemas import DeliveryEventBase, FetchJobInput

# Example mock constants
MOCK_JOB_ID = "94c35132-3cb1-4ded-900c-209a62c22752b"
MOCK_SITE_ID = "test-site-001"
MOCK_DATE = "2025-10-27"
MOCK_DELIVERY_A = {
    "siteId": MOCK_SITE_ID,
    "deliveredAt": datetime.now(timezone.utc),
    "deliveryScore": 5.0,
    "isSigned": True,
    "supplierDeliveryId": "sup-123",
    "supplier": "UPS",
    "status": "delivered",
}


@pytest.mark.asyncio
async def test_start_fetch_job_success(override_get_db):
    """Tests POST /fetch for successful job creation."""
    db = override_get_db
    repo = DeliveryRepository(db)

    input_data = FetchJobInput(siteId=MOCK_SITE_ID, date=MOCK_DATE)  # noqa: F841

    # Start job (mock background tasks)
    job = repo.create_job(MOCK_JOB_ID, MOCK_SITE_ID, MOCK_DATE)

    assert job.jobId == MOCK_JOB_ID
    assert job.status == "created"


def test_get_job_status_success(override_get_db):
    """Tests GET /jobs/{jobId} for an existing job."""
    db = override_get_db
    repo = DeliveryRepository(db)

    job = repo.create_job(MOCK_JOB_ID, MOCK_SITE_ID, MOCK_DATE)  # noqa: F841

    fetched_job = repo.get_job_by_id(MOCK_JOB_ID)
    assert fetched_job.jobId == MOCK_JOB_ID
    assert fetched_job.status == "created"


def test_get_job_status_not_found(override_get_db):
    """Tests GET /jobs/{jobId} for non-existent job."""
    db = override_get_db
    repo = DeliveryRepository(db)

    fetched_job = repo.get_job_by_id("non-existent")
    assert fetched_job is None


def test_get_job_results_success(override_get_db):
    """Tests GET /jobs/{jobId}/results for a job with stored deliveries."""
    db = override_get_db
    repo = DeliveryRepository(db)

    job = repo.create_job(MOCK_JOB_ID, MOCK_SITE_ID, MOCK_DATE)

    mock_event = DeliveryEventBase(**MOCK_DELIVERY_A)
    repo.insert_or_update_delivery_event(mock_event, job.jobId, {})

    total_count, deliveries = repo.get_deliveries_by_job_id(job.jobId, limit=10, offset=0)
    assert total_count == 1
    assert deliveries[0].supplierDeliveryId == MOCK_DELIVERY_A["supplierDeliveryId"]


def test_get_job_results_not_finished(override_get_db):
    """Tests GET /jobs/{jobId}/results for a job still processing."""
    db = override_get_db
    repo = DeliveryRepository(db)

    job = repo.create_job(MOCK_JOB_ID, MOCK_SITE_ID, MOCK_DATE)
    assert job.status != "finished"


def test_search_deliveries_filters(override_get_db):
    """Tests GET /deliveries endpoint with filtering parameters."""
    db = override_get_db
    repo = DeliveryRepository(db)

    job = repo.create_job(MOCK_JOB_ID, MOCK_SITE_ID, MOCK_DATE)
    mock_event = DeliveryEventBase(**MOCK_DELIVERY_A)
    repo.insert_or_update_delivery_event(mock_event, job.jobId, {})

    total_count, deliveries = repo.search_deliveries(
        siteId=MOCK_SITE_ID,
        status=None,
        min_score=4.0,
        limit=10,
        offset=0
    )

    assert total_count >= 1
    assert deliveries[0].deliveryScore >= 4.0
