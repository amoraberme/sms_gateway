from __future__ import annotations

from dataclasses import dataclass
import hashlib
from uuid import UUID

from fastapi import Header, HTTPException, Request, status


@dataclass(frozen=True)
class AuthenticatedTenant:
    id: UUID
    name: str
    slug: str
    default_priority: str


def hash_api_key(api_key: str) -> str:
    return hashlib.sha256(api_key.encode("utf-8")).hexdigest()


async def get_current_tenant(
    request: Request,
    x_api_key: str | None = Header(default=None, alias="X-API-Key"),
) -> AuthenticatedTenant:
    if not x_api_key:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing X-API-Key header",
        )

    pool = request.app.state.db_pool
    key_hash = hash_api_key(x_api_key.strip())
    row = await pool.fetchrow(
        """
        SELECT id, name, slug, default_priority::text AS default_priority
        FROM tenants
        WHERE api_key_hash = $1 AND is_active = TRUE
        """,
        key_hash,
    )
    if row is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or inactive API key",
        )

    return AuthenticatedTenant(
        id=row["id"],
        name=row["name"],
        slug=row["slug"],
        default_priority=row["default_priority"],
    )
