import argparse
import subprocess
import sys

from fornax_cutouts.constants import LOG_LEVEL


def run_api():
    """Start FastAPI using uvicorn."""
    cmd = [
        sys.executable,
        "-m",
        "uvicorn",
        "fornax_cutouts.app.api:main_app",
        "--host",
        "0.0.0.0",
        "--port",
        "8000",
        "--reload",
    ]
    subprocess.run(cmd, check=True)


def run_worker():
    """Start Celery worker."""
    cmd = [
        sys.executable,
        "-m",
        "celery",
        "-A",
        "fornax_cutouts.app.celery_app.celery_app",
        "worker",
        "--loglevel",
        LOG_LEVEL,
    ]
    subprocess.run(cmd, check=True)


def main():
    parser = argparse.ArgumentParser(description="CLI for Fornax Cutouts: launch API or Celery worker.")
    parser.add_argument(
        "command",
        choices=["api", "worker"],
        help="Choose 'api' to start FastAPI service or 'worker' to start Celery worker.",
    )
    args = parser.parse_args()

    if args.command == "api":
        run_api()
    elif args.command == "worker":
        run_worker()
