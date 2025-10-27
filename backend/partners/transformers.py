import logging
from datetime import datetime, timezone

from typing import Dict, Any, Protocol, Optional, Tuple
from pydantic import ValidationError

from backend.schemas import DeliveryEventBase

logger = logging.getLogger(__name__)


class DeliveryTransformer(Protocol):
    """Protocol for any logistics partner transformer."""
    supplier_name: str

    def transform_and_score(self, raw_data: Dict[str, Any]) -> Tuple[DeliveryEventBase, Optional[Dict[str, Any]]]:
        """
        Transforms raw partner data into a normalized DeliveryEventBase and calculates score.
        Returns the Pydantic model and an optional dictionary of data errors.
        """
        ...


def _normalize_status(partner_status: str) -> str:
    """Maps partner status strings to a unified set."""
    status_lower = partner_status.lower()
    if any(s in status_lower for s in ['delivered', 'done', 'complete']):
        return 'delivered'
    if any(s in status_lower for s in ['cancel', 'fail', 'rejected']):
        return 'cancelled'
    if any(s in status_lower for s in ['transit', 'shipped', 'pending', 'scheduled']):
        return 'pending'
    return 'other'


def _calculate_score(is_delivered: bool, is_signed: bool) -> float:
    """
    Calculates the VESTIGAS delivery score (1.0 to 5.0).
    Score = 1.0 (Base) + 2.0 (if delivered) + 2.0 (if signed)
    """
    score = 1.0
    if is_delivered:
        score += 2.0
    if is_signed:
        score += 2.0
    return min(5.0, score)  # Cap score at 5.0


class PartnerATransformer:
    """Transformer for Partner A's data format."""
    supplier_name = "Partner_A"

    def transform_and_score(self, raw_data: Dict[str, Any]) -> Tuple[DeliveryEventBase, Optional[Dict[str, Any]]]:
        errors = {}

        # 1. Normalize Status & Check Delivery
        status_raw = raw_data.get('deliveryStatus', 'UNKNOWN')
        status_normalized = _normalize_status(status_raw)
        is_delivered = (status_normalized == 'delivered')

        # 2. Check Signature
        is_signed = raw_data.get('podSigned', False)

        # 3. Calculate Score
        score = _calculate_score(is_delivered, is_signed)

        # 4. Normalize Timestamp (Handle common date formats)
        try:
            delivered_at_str = raw_data.get('deliveryTime')
            if delivered_at_str:
                # Try parsing the likely ISO format, assuming UTC if no timezone is present
                delivered_at = datetime.fromisoformat(delivered_at_str.replace('Z', '+00:00')).replace(tzinfo=None)
            else:
                raise ValueError("deliveryTime field is missing or empty.")
        except Exception as e:
            errors['deliveredAt'] = f"Failed to parse deliveryTime '{raw_data.get('deliveryTime')}': {e}"
            # Use current time as fallback for fatal timestamp errors
            delivered_at = datetime.now(timezone.utc)

        # 5. Build and Validate Normalized Model
        try:
            normalized_event = DeliveryEventBase(
                siteId=raw_data.get('site_id'),
                supplier=self.supplier_name,
                supplierDeliveryId=raw_data.get('order_id'),
                deliveredAt=delivered_at,
                status=status_normalized,
                deliveryScore=score,
                isSigned=is_signed
            )
            return normalized_event, errors if errors else None
        except ValidationError as e:
            # If Pydantic validation fails, log it and return partial error details
            errors['pydantic'] = e.errors()
            logger.error(f"Validation failed for Partner A data: {errors['pydantic']}")
            raise


class PartnerBTransformer:
    """Transformer for Partner B's data format."""
    supplier_name = "Partner_B"

    def transform_and_score(self, raw_data: Dict[str, Any]) -> Tuple[DeliveryEventBase, Optional[Dict[str, Any]]]:
        errors = {}

        # 1. Normalize Status & Check Delivery
        # Partner B uses a complex status dictionary
        status_raw = raw_data.get('status', {}).get('code', 'UNKNOWN')
        status_normalized = _normalize_status(status_raw)
        is_delivered = (status_normalized == 'delivered')

        # 2. Check Signature (Partner B uses a boolean flag in a 'metadata' dict)
        is_signed = raw_data.get('proof', {}).get('signed', False)

        # 3. Calculate Score
        score = _calculate_score(is_delivered, is_signed)

        # 4. Normalize Timestamp
        try:
            delivered_at_str = raw_data.get('timestamps', {}).get('delivery_completion')
            if delivered_at_str:
                # Partner B uses Unix timestamp (integer or string of integer)
                delivered_at_ts = int(delivered_at_str)
                delivered_at = datetime.fromtimestamp(delivered_at_ts, tz=timezone.utc)
            else:
                raise ValueError("delivery_completion timestamp is missing or empty.")
        except Exception as e:
            errors['deliveredAt'] = f"Failed to parse delivery_completion timestamp: {e}"
            delivered_at = datetime.now(timezone.utc)

        # 5. Build and Validate Normalized Model
        try:
            normalized_event = DeliveryEventBase(
                siteId=raw_data.get('location', {}).get('site_ref'),
                supplier=self.supplier_name,
                supplierDeliveryId=raw_data.get('reference_id'),
                deliveredAt=delivered_at,
                status=status_normalized,
                deliveryScore=score,
                isSigned=is_signed
            )
            return normalized_event, errors if errors else None
        except ValidationError as e:
            errors['pydantic'] = e.errors()
            logger.error(f"Validation failed for Partner B data: {errors['pydantic']}")
            raise


PARTNER_TRANSFORMERS: Dict[str, DeliveryTransformer] = {
    "Partner_A": PartnerATransformer(),
    "Partner_B": PartnerBTransformer(),
}
