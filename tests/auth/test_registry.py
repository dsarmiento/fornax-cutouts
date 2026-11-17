from fornax_cutouts.auth.base import AbstractAuthProvider
from fornax_cutouts.auth.registry import AuthRegistry
from fornax_cutouts.models.auth import Principal


class _Provider(AbstractAuthProvider):
    name = "test-provider"
    anon_limit_hit_info = "Send a token to raise this limit"
    authenticated_limit_hit_info = "Retry after the window resets"

    async def resolve(self, request) -> Principal | None:
        return None


def test_limit_hit_info_uses_base_default_without_provider():
    registry = AuthRegistry()
    assert registry.limit_hit_info(is_anonymous=True) == ""
    assert registry.limit_hit_info(is_anonymous=False) == ""


def test_limit_hit_info_uses_registered_provider():
    registry = AuthRegistry()
    registry.register_provider(_Provider)
    assert registry.limit_hit_info(is_anonymous=True) == "Send a token to raise this limit"
    assert registry.limit_hit_info(is_anonymous=False) == "Retry after the window resets"
