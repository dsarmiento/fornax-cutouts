from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path

from fornax_cutouts.auth import auth_registry
from fornax_cutouts.config import CONFIG
from fornax_cutouts.sources import cutout_registry
from fornax_cutouts.utils.logging import get_logger


def _python_files(path: Path) -> list[Path]:
    return sorted(path.glob("**/*.py")) if path.is_dir() else [path]


def _exec_module(path: Path, name_prefix: str):
    spec = spec_from_file_location(f"{name_prefix}_{path.stem}", path.as_posix())
    module = module_from_spec(spec)
    spec.loader.exec_module(module)


def discover_sources():
    logger = get_logger()

    loaded = set()
    for source in _python_files(CONFIG.source_path):
        _exec_module(source, "cutouts_source")
        loaded.add(source.resolve())

    resolver_path = CONFIG.cutout_limit.principal_resolver
    if resolver_path is not None:
        if not resolver_path.exists():
            logger.warning(
                f"Configured principal resolver path does not exist: {resolver_path}",
                extra={"event": "auth_provider_missing", "path": resolver_path.as_posix()},
            )
        else:
            for resolver in _python_files(resolver_path):
                # A resolver living under source_path has already been executed; re-running it
                # would trip the registry's single-provider guard.
                if resolver.resolve() not in loaded:
                    _exec_module(resolver, "cutouts_auth")
                    loaded.add(resolver.resolve())

    provider = auth_registry._provider
    logger.debug(
        f"Registered mission sources: {cutout_registry.get_source_names()}; "
        f"auth provider: {provider.name if provider is not None else 'none'}"
    )
