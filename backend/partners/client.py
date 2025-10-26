import asyncio
import logging
import os
from typing import Dict, Any, List, Optional, Tuple, Literal
from fastapi import Depends

import httpx
from pydantic import ValidationError

from backend.data_access import DeliveryRepository, get_repository
from backend.partners.transformers import PARTNER_TRANSFORMERS, DeliveryTransformer
from backend.utils.retry import async_retry
from backend.schemas import PartnerStats

logger = logging.getLogger(__name__)

PARTNER_A: Literal["Partner_A"] = "Partner_A"
PARTNER_B: Literal["Partner_B"] = "Partner_B"

PARTNER_CONFIG: Dict[str, Dict[str, Any]] = {
    PARTNER_A: {
        "url_env": "LOGISTICS_A_URL",
        "transformer": PARTNER_TRANSFORMERS[PARTNER_A],
    },
    PARTNER_B: {
        "url_env": "LOGISTICS_B_URL",
        "transformer": PARTNER_TRANSFORMERS[PARTNER_B],
    },
}


class PartnerAPIClient:
    """
    Configured HTTP Client for external partner communication.
    Uses a standard timeout.
    """

    def __init__(self):
        # 5-second timeout for external requests
        self.client = httpx.AsyncClient(timeout=5.0)

    @async_retry(max_retries=3)
    async def _fetch_raw_data(self, url: str, site_id: str, date: str) -> httpx.Response:
        """
        Internal function to execute the raw HTTP GET request.
        The @async_retry decorator handles transient errors (network, 5xx).
        """
        params = {"siteId": site_id, "date": date}
        logger.info(f"Fetching raw data from {url} with params {params}")

        response = await self.client.get(url, params=params)

        # httpx raises an exception for 4xx/5xx status codes only if raise_for_status() is called.
        # Here, we only rely on the status code check inside the retry decorator.
        return response

    async def fetch_partner_data(self, partner_name: str, site_id: str, date: str, repo: DeliveryRepository) \
            -> Tuple[PartnerStats, Optional[str]]:
        """
        Fetches data from a single partner, processes it, and stores it in the database.
        Returns PartnerStats and an optional error message if the entire fetch failed.
        """
        config = PARTNER_CONFIG[partner_name]
        url = os.getenv(config['url_env'])
        transformer: DeliveryTransformer = config['transformer']

        stats = PartnerStats()
        job_error_message = None

        try:
            response = await self._fetch_raw_data(url, site_id, date)

            response.raise_for_status()

            raw_records: List[Dict[str, Any]] = response.json()
            stats.fetched = len(raw_records)

            for record in raw_records:
                try:
                    normalized_event, data_errors = transformer.transform_and_score(record)

                    repo.insert_or_update_delivery_event(
                        data=normalized_event,
                        jobId=repo.db.info['job_id'],
                        source_data=record,
                        data_errors=data_errors
                    )
                    stats.transformed += 1

                except (ValidationError, Exception) as e:
                    stats.errors += 1
                    logger.error(f"[{partner_name}] Record error (ID: "
                                 f"{record.get('order_id') or record.get('reference_id')}): {e}")

        except httpx.HTTPStatusError as e:
            stats.error_message = f"HTTP Error {e.response.status_code}: {e.response.text[:100]}..."
            job_error_message = f"Failed to fetch data from {partner_name} " \
                                f"due to terminal  HTTP error: {e.response.status_code}"
            logger.error(job_error_message)

        except Exception as e:
            # Catch all other final errors (e.g., JSON decode errors, final network failures)
            stats.error_message = f"Critical Fetch Error: {type(e).__name__} - {e}"
            job_error_message = f"Failed to fetch data from {partner_name} due to final error: {e}"
            logger.error(job_error_message)

        return stats, job_error_message


class JobManager:
    """Manages the full lifecycle of a delivery fetch job."""

    def __init__(self, client: PartnerAPIClient):
        self.client = client
        self.partners = list(PARTNER_CONFIG.keys())

    async def run_fetch_job(self, job_id: str, site_id: str, date: str, repo: DeliveryRepository):
        """
        The main asynchronous function executed in the background.
        Orchestrates parallel fetching from all partners.
        """
        logger.info(f"Job {job_id}: Starting parallel fetch operations.")

        repo.update_job_status(job_id, "processing")

        repo.db.info['job_id'] = job_id

        try:
            tasks = [
                self.client.fetch_partner_data(partner, site_id, date, repo)
                for partner in self.partners
            ]

            results: List[Tuple[PartnerStats, Optional[str]]] = await asyncio.gather(*tasks)

            final_stats = {
                "stored": 0,
                "total_fetched": 0,
            }
            has_fatal_error = False
            overall_error_message = None

            for i, (partner_stats, fatal_error) in enumerate(results):
                partner_name = self.partners[i]

                partner_stats_dict = partner_stats.model_dump()
                final_stats[partner_name] = partner_stats_dict
                final_stats["total_fetched"] += partner_stats.fetched
                final_stats["stored"] += partner_stats.transformed

                if fatal_error:
                    has_fatal_error = True
                    overall_error_message = overall_error_message or fatal_error

            if has_fatal_error and final_stats["stored"] == 0:
                repo.update_job_status(job_id, "failed", stats_update=final_stats, error=overall_error_message)
                logger.error(f"Job {job_id}: Failed with no data stored.")
            else:
                final_status = "finished"
                repo.update_job_status(job_id, final_status, stats_update=final_stats, error=overall_error_message)
                logger.info(f"Job {job_id}: Finished. Stored {final_stats['stored']} records.")

        except Exception as e:
            logger.error(f"Job {job_id}: Catastrophic job runner failure: {e}", exc_info=True)
            repo.update_job_status(job_id, "failed", error=f"Catastrophic job runner failure: {e}")


def get_partner_client() -> PartnerAPIClient:
    """Dependency to provide a single instance of the HTTP client."""
    return PartnerAPIClient()


def get_job_manager(client: PartnerAPIClient = Depends(get_partner_client)) -> JobManager:
    """Dependency to provide a single instance of the Job Manager."""
    return JobManager(client)
