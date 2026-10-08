from importlib.metadata import PackageNotFoundError, version


def _package_version() -> str:
    try:
        return version("fornax-cutouts")
    except PackageNotFoundError:
        return "0.1.0+dev"


__version__ = _package_version()

__all__ = ["__version__"]
