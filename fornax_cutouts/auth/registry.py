import hashlib
import logging
from dataclasses import dataclass, field
from typing import TypeVar

from starlette.requests import Request

from fornax_cutouts.auth.base import AbstractAuthProvider
from fornax_cutouts.config import CONFIG
from fornax_cutouts.models.auth import Principal
from fornax_cutouts.utils.exceptions import PrincipalResolutionError
from fornax_cutouts.utils.logging import get_logger
from fornax_cutouts.utils.middleware import client_ip_from_request

_UNKNOWN_CLIENT_BUCKET = "unknown"
_AuthProviderT = TypeVar("_AuthProviderT", bound=type[AbstractAuthProvider])


@dataclass
class AuthRegistry:
    _provider: AbstractAuthProvider | None = field(default=None, init=False)
    logger: logging.Logger = field(default_factory=get_logger, init=False)

    def register_provider(self, cls: _AuthProviderT) -> _AuthProviderT:
        """
        Register an authentication provider.
        Used as a decorator for institution-provided authentication provider classes.

        Args:
            cls (type[_AuthProviderT]): The authentication provider class to register

        Returns:
            type[_AuthProviderT]: The registered authentication provider class
        """
        if self._provider is not None:
            raise RuntimeError(
                f"Auth provider already registered: {self._provider.name}. Only one provider can be registered."
            )

        self._provider = cls()
        self.logger.info(f"Registered {self._provider.name} as the authz provider")
        return cls

    def _anonymous_principal(self, request: Request) -> Principal:
        """
        Create an anonymous Principal for the request.

        Args:
            request (Request): The request to create an anonymous Principal for

        Returns:
            Principal: The anonymous Principal
        """
        client_ip = client_ip_from_request(request)
        identity = hashlib.sha256(client_ip.encode()).hexdigest()[:32] if client_ip else _UNKNOWN_CLIENT_BUCKET
        return Principal(
            identity=identity,
            is_anonymous=True,
            cutout_limit=CONFIG.cutout_limit.anon_cutout_limit,
        )

    async def resolve_principal(self, request: Request) -> Principal:
        """
        Resolve the Principal for a request.

        When a provider is registered via `register_provider()`, it is consulted first.
        A provider that raises is logged and skipped so a broken auth backend degrades to
        the anonymous limit rather than failing the request. Falls back to an IP-derived
        anonymous Principal when no provider is registered or the provider returns None.
        """
        if self._provider is not None:
            principal = None
            try:
                principal = await self._provider.resolve(request)
            except PrincipalResolutionError as e:
                self.logger.warning(
                    f"{e}. Falling back to anonymous principal.",
                    extra={"event": "auth_provider_error", "provider": self._provider.name, "error": str(e)},
                )

            if principal is not None:
                return principal

        return self._anonymous_principal(request)
