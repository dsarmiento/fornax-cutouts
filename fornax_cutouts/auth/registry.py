import hashlib
import logging
from dataclasses import dataclass, field
from functools import cached_property

from starlette.requests import Request

from fornax_cutouts.auth.base import AbstractAuthProvider
from fornax_cutouts.config import CONFIG
from fornax_cutouts.models.auth import Principal
from fornax_cutouts.utils.logging import get_logger
from fornax_cutouts.utils.middleware import client_ip_from_request

_UNKNOWN_CLIENT_BUCKET = "unknown"


@dataclass
class AuthRegistry:
    _PROVIDERS: dict[str, AbstractAuthProvider] = field(default_factory=dict, init=False)
    logger: logging.Logger = field(default_factory=get_logger, init=False)

    @cached_property
    def _PROVIDER_NAMES(self) -> list[str]:
        return sorted(self._PROVIDERS.keys())

    def register_provider(self):
        def _decorator(cls: AbstractAuthProvider) -> AbstractAuthProvider:
            self._PROVIDERS[cls.name] = cls()
            return cls

        return _decorator

    def get_provider_names(self) -> list[str]:
        return self._PROVIDER_NAMES

    def _anonymous_principal(self, request: Request) -> Principal:
        client_ip = client_ip_from_request(request)
        identity = hashlib.sha256(client_ip.encode()).hexdigest()[:32] if client_ip else _UNKNOWN_CLIENT_BUCKET
        return Principal(
            identity=identity,
            is_anonymous=True,
            cutout_limit=CONFIG.cutout_limit.anon_cutout_limit,
        )

    async def resolve_principal(self, request: Request) -> Principal:
        """Resolve the Principal for a request.

        Providers registered via `register_provider()` are tried in sorted-name order
        (deterministic regardless of filesystem discovery order); the first one to return
        a non-None Principal wins. A provider that raises is logged and skipped so a broken
        auth backend degrades to the anonymous limit rather than failing the request. Falls
        back to an IP-derived anonymous Principal when no provider claims the request.
        """
        for name in self._PROVIDER_NAMES:
            try:
                principal = await self._PROVIDERS[name].resolve(request)
            except Exception as e:
                self.logger.error(
                    f"Auth provider {name!r} failed to resolve principal: {e}",
                    extra={"event": "auth_provider_error", "provider": name, "error": str(e)},
                    exc_info=True,
                )
                continue

            if principal is not None:
                return principal

        return self._anonymous_principal(request)
