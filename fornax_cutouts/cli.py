import click

from fornax_cutouts.config import CONFIG
from fornax_cutouts.sources import cutout_registry

_DEFAULT_LOG_LEVEL = CONFIG.log.level
_UVICORN_LOG_LEVEL_DEFAULT = str(_DEFAULT_LOG_LEVEL).lower()
_CELERY_LOG_LEVEL_DEFAULT = str(_DEFAULT_LOG_LEVEL).upper()


@click.group(help="CLI for Fornax Cutouts: launch API or Celery worker.")
def cli():
    # Discover sources before any subcommand runs.
    cutout_registry.discover_sources()


@cli.command("api", help="Start the FastAPI/Uvicorn service.")
@click.option(
    "--host",
    default="0.0.0.0",
    show_default=True,
    help="Uvicorn host interface.",
)
@click.option(
    "--port",
    default=8000,
    type=click.IntRange(1, 65535),
    show_default=True,
    help="Uvicorn port.",
)
@click.option(
    "-w",
    "--workers",
    default=1,
    type=click.IntRange(1, None),
    show_default=True,
    help="Number of Uvicorn worker processes.",
)
@click.option(
    "--reload/--no-reload",
    default=False,
    show_default=True,
    help="Enable Uvicorn auto-reload (development). Not compatible with workers > 1.",
)
@click.option(
    "--log-level",
    type=click.Choice(
        ["critical", "error", "warning", "info", "debug", "trace"], case_sensitive=False
    ),
    default=_UVICORN_LOG_LEVEL_DEFAULT,
    show_default=True,
    help="Uvicorn log level.",
)
def api(
    host: str,
    port: int,
    workers: int,
    reload: bool,
    log_level: str,
):
    """Start the FastAPI service with Uvicorn."""
    # reload and workers>1 can't be used together
    if reload and workers != 1:
        raise click.UsageError("--reload cannot be combined with --workers > 1")

    import uvicorn

    from fornax_cutouts.app.api import main_app


    uvicorn.run(
        main_app,
        host=host,
        port=port,
        reload=reload,
        workers=workers,
        log_level=log_level,
    )


@cli.command("worker", help="Start the Celery worker.")
@click.option(
    "-n",
    "--name",
    type=click.STRING,
    help="Unique name for the celery worker."
)
@click.option(
    "--autoscale",
    metavar="MAX,MIN",
    help="Enable autoscaling with 'MAX,MIN' (e.g., 8,2).",
)
@click.option(
    "-c",
    "--concurrency",
    type=click.IntRange(1, None),
    help="Number of concurrent worker processes/threads.",
)
@click.option(
    "--log-level",
    type=click.Choice(["CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG"], case_sensitive=False),
    default=_CELERY_LOG_LEVEL_DEFAULT,
    show_default=True,
    help="Celery log level.",
)
def worker(
    name: str | None,
    autoscale: str | None,
    concurrency: int | None,
    log_level: str,
):
    """Start Celery worker."""
    from fornax_cutouts.app.celery_app import celery_app

    args = ["worker", f"--loglevel={log_level}"]

    if autoscale:
        # Celery expects "MAX,MIN"
        args.append(f"--autoscale={autoscale}")

    if concurrency:
        args.append(f"--concurrency={concurrency}")

    if name:
        args.append(f"--hostname={name}")

    celery_app.worker_main(args)


def main():
    cli()
