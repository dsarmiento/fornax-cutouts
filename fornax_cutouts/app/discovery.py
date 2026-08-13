from importlib.util import module_from_spec, spec_from_file_location

from fornax_cutouts.auth import auth_registry
from fornax_cutouts.config import CONFIG
from fornax_cutouts.sources import cutout_registry
from fornax_cutouts.utils.logging import get_logger


def discover_sources():
    for source in CONFIG.source_path.glob("**/*.py"):
        spec = spec_from_file_location(f"cutouts_source_{source.name}", source.as_posix())
        module = module_from_spec(spec)
        spec.loader.exec_module(module)

    logger = get_logger()
    logger.debug(
        f"Registered mission sources: {cutout_registry.get_source_names()}; "
        f"auth providers: {auth_registry.get_provider_names()}"
    )
