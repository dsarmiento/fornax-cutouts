import argparse

from fornax_cutouts.config import CONFIG
from fornax_cutouts.sources import cutout_registry


def run_api(
    host: str = "0.0.0.0",
    port: int = 8000,
    reload: bool = False,
    workers: int = 1,
):
    import uvicorn

    from fornax_cutouts.app.api import main_app

    # reload and workers>1 can't be used together
    if reload and workers != 1:
        raise SystemExit("--reload cannot be combined with --workers > 1")

    uvicorn.run(
        main_app,
        host=host,
        port=port,
        reload=reload,
        workers=workers,
        log_level=CONFIG.log_level,
    )


def run_worker():
    """Start Celery worker."""
    from fornax_cutouts.app.celery_app import celery_app

    celery_app.worker_main([
        "worker",
        f"--loglevel={CONFIG.log_level}"
    ])


def main():
    parser = argparse.ArgumentParser(description="CLI for Fornax Cutouts: launch API or Celery worker.")
    parser.add_argument(
        "command",
        choices=["api", "worker"],
        help="Choose 'api' to start FastAPI service or 'worker' to start Celery worker.",
    )
    args = parser.parse_args()

    cutout_registry.discover_sources()

    if args.command == "api":
        run_api()
    elif args.command == "worker":
        run_worker()
