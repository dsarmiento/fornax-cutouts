import logging
from abc import ABC, abstractmethod

from starlette.requests import Request

from fornax_cutouts.models.auth import Principal
from fornax_cutouts.utils.logging import get_logger


class AbstractAuthProvider(ABC):
    """
    Base class for pluggable request-authentication/authorization providers.

    Implementations resolve a :class:`Principal` for a request, which can raise or
    remove the anonymous cutout limit (see ``Principal.cutout_limit``). Register
    a subclass with ``@auth_registry.register_provider`` in a module placed under
    ``CUTOUTS__SOURCE_PATH`` or at ``CUTOUTS__CUTOUT_LIMIT__PRINCIPAL_RESOLVER``; it is
    discovered the same way mission sources are.
    """

    name: str

    @property
    def logger(self) -> logging.Logger:
        return get_logger()

    def __repr__(self):
        return f"AuthProvider(name={self.name})"

    @abstractmethod
    async def resolve(self, request: Request) -> Principal | None:
        """
        Resolve a Principal for the request, or None if this provider doesn't apply.

        Returning None lets the registry fall back to the default anonymous principal
        rather than treating the request as unauthenticated.
        """
        ...
