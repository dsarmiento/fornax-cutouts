---
title: Getting Started
description: Install and run Fornax Cutouts locally
---

# Getting Started

## Prerequisites

- Python 3.10–3.13
- [uv](https://docs.astral.sh/uv/) package manager
- Redis 6.2+ (for job queue and UWS state)
- A mission source file (see [Building a Source](sources/building-a-source.md))

---

## Installation

### From source

```bash
git clone https://github.com/nasa-fornax/fornax-cutouts
cd fornax-cutouts
uv sync
```

### As a library dependency

Add fornax-cutouts to your project's dependencies. If you are developing against a local clone side-by-side with your service:

```toml
# pyproject.toml
[tool.uv.sources]
fornax-cutouts = { path = "../fornax-cutouts", editable = true }
```

Then run:

```bash
uv sync
```

---

## Minimum Configuration

Fornax Cutouts requires at minimum two environment variables to start:

```bash
export CUTOUTS__SOURCE_PATH=/path/to/your/sources
export CUTOUTS__REDIS__HOST=localhost
```

All other settings have sensible defaults. See [Configuration](configuration.md) for the full reference.

### Using a `.env` file

The service reads a `.env` file from the working directory automatically:

```bash
# .env
CUTOUTS__SOURCE_PATH=/path/to/your/sources
CUTOUTS__REDIS__HOST=localhost
CUTOUTS__STORAGE__PREFIX=s3://my-bucket/cutouts
CUTOUTS__LOG_LEVEL=info
```

---

## Starting Redis

If you don't have Redis running locally, the quickest way is via Docker:

```bash
docker run -d -p 6379:6379 redis:7
```

---

## Running the API

```bash
fornax-cutouts api
```

The API will start on `http://localhost:8000` by default. Visit `http://localhost:8000/docs` for the interactive Swagger UI.

For development with auto-reload:

```bash
fornax-cutouts api --reload
```

---

## Running the Worker

In a separate terminal:

```bash
fornax-cutouts worker
```

The worker connects to Redis and begins consuming from the `cutouts` and `high_mem` queues.

---

## Verifying the Setup

Check the health endpoint:

```bash
curl http://localhost:8000/api/health
```

Expected response:

```json
{
  "status": "ok",
  "details": "Redis connection successful",
  "timestamp": "2024-01-01T00:00:00Z",
  "environment": "dev"
}
```

List registered missions (requires at least one source file in `CUTOUTS__SOURCE_PATH`):

```bash
curl http://localhost:8000/api/v0/missions
```

---

## Project Layout

When using fornax-cutouts as a library, a typical project structure looks like:

```
my-cutouts-service/
├── sources/               # Mission source files (CUTOUTS__SOURCE_PATH)
│   └── my_mission.py
├── .env                   # Local environment config
└── pyproject.toml
```

Each `.py` file under `CUTOUTS__SOURCE_PATH` is automatically discovered and executed at startup. Any file that uses `@cutout_registry.register_source(...)` will register its source with the service.

---

## Next Steps

- [Configuration](configuration.md) — tune Redis, worker, and storage settings
- [CLI Reference](cli.md) — all `fornax-cutouts` command options
- [Building a Source](sources/building-a-source.md) — implement your first mission source
- [API Reference](api/index.md) — explore the REST endpoints
