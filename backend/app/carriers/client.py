from __future__ import annotations

from dataclasses import dataclass
import httpx


@dataclass
class CarrierSubmitResponse:
    status_code: int
    carrier_id: str | None = None
    error: str | None = None


class CarrierClient:
    def __init__(self, timeout_sec: float = 5.0) -> None:
        self.client = httpx.AsyncClient(timeout=timeout_sec)

    async def close(self) -> None:
        await self.client.aclose()

    async def submit(
        self,
        base_url: str,
        message_id: str,
        to_number: str,
        from_sender: str,
        text: str,
        encoding: str,
    ) -> CarrierSubmitResponse:
        resp = await self.client.post(
            f"{base_url.rstrip('/')}/submit",
            json={
                "message_id": message_id,
                "to": to_number,
                "from": from_sender,
                "text": text,
                "encoding": encoding,
            },
        )
        data = resp.json() if resp.content else {}
        if resp.status_code == 200:
            return CarrierSubmitResponse(
                status_code=200,
                carrier_id=data.get("carrier_id"),
            )
        return CarrierSubmitResponse(
            status_code=resp.status_code,
            error=data.get("error", f"HTTP_{resp.status_code}"),
        )
