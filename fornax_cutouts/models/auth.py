from typing import Any

from pydantic import BaseModel, Field


class Principal(BaseModel):
    """
    Resolved identity for a request, used to look up its cutout limit budget.

    ``cutout_limit=None`` means unlimited (no budget enforced). ``window_seconds=None``
    means the default rolling window from ``CONFIG.cutout_limit.window_seconds`` applies.
    """

    identity: str
    is_anonymous: bool = False
    cutout_limit: int | None = None
    window_seconds: int | None = None
    extras: dict[str, Any] = Field(default_factory=dict)
