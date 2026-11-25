from __future__ import annotations

from datetime import datetime
import json

from fastapi import APIRouter, Header, HTTPException, Request, status
from pydantic import BaseModel

from app.config import get_settings
from app.services.dlr_service import process_carrier_dlr, verify_dlr_signature

router = APIRouter(tags=["carrier"])


class DlrPayload(BaseModel):
    carrier_id: str
    status: str
    timestamp: datetime


@router.post("/carrier/dlr")
async def receive_dlr(
    request: Request,
    x_signature: str | None = Header(default=None, alias="X-Signature"),
) -> dict[str, str]:
    settings = get_settings()
    raw_body = await request.body()
    verify_dlr_signature(raw_body, x_signature, settings.dlr_secret)

    try:
        parsed = DlrPayload.model_validate_json(raw_body)
        raw_dict = json.loads(raw_body.decode("utf-8"))
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid DLR payload: {exc}",
        ) from exc

    return await process_carrier_dlr(
        pool=request.app.state.db_pool,
        scheduler=request.app.state.scheduler,
        carrier_id=parsed.carrier_id,
        raw_status=parsed.status,
        carrier_timestamp=parsed.timestamp,
        raw_payload=raw_dict,
    )
