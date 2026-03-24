---
title: CLI Reference
description: fornax-cutouts command-line interface reference
---

# CLI Reference

The `fornax-cutouts` command-line interface provides two subcommands for running the service components.

```
fornax-cutouts [COMMAND] [OPTIONS]
```

---

## `fornax-cutouts api`

Start the FastAPI/Uvicorn HTTP server.

```bash
fornax-cutouts api [OPTIONS]
```

### Options

| Option                                                       | Default                   | Description                                                                                                 |
| ------------------------------------------------------------ | ------------------------- | ----------------------------------------------------------------------------------------------------------- |
| `--host TEXT`                                                | `0.0.0.0`                 | Network interface to bind to. Use `127.0.0.1` to restrict to localhost.                                     |
| `--port INTEGER`                                             | `8000`                    | TCP port to listen on. Must be between 1 and 65535.                                                         |
| `-w, --workers INTEGER`                                      | `1`                       | Number of Uvicorn worker processes. Cannot be combined with `--reload`.                                     |
| `--reload / --no-reload`                                     | `--no-reload`             | Enable auto-reload on code changes. Intended for development only. Mutually exclusive with `--workers > 1`. |
| `--log-level [critical\|error\|warning\|info\|debug\|trace]` | from `CUTOUTS__LOG_LEVEL` | Uvicorn log verbosity.                                                                                      |

### Examples

Start for local development with auto-reload:

```bash
fornax-cutouts api --reload
```

Start with multiple workers for production:

```bash
fornax-cutouts api --workers 4 --port 8080
```

Bind to localhost only:

```bash
fornax-cutouts api --host 127.0.0.1
```

---

## `fornax-cutouts worker`

Start the Celery worker process.

```bash
fornax-cutouts worker [OPTIONS]
```

The worker connects to Redis (configured via `CUTOUTS__REDIS__*`) and begins consuming tasks from the specified queues.

### Options

| Option                                                | Default                   | Description                                                                                                                |
| ----------------------------------------------------- | ------------------------- | -------------------------------------------------------------------------------------------------------------------------- |
| `-n, --name TEXT`                                     | —                         | Unique hostname for this worker instance (e.g. `worker1@hostname`). Useful when running multiple workers on the same host. |
| `--autoscale MAX,MIN`                                 | —                         | Enable Celery autoscaling. Provide as `MAX,MIN` (e.g. `8,2` to scale between 2 and 8 processes).                           |
| `-c, --concurrency INTEGER`                           | —                         | Number of concurrent worker processes or threads. Defaults to the number of CPUs when not set.                             |
| `-Q, --queues TEXT`                                   | `cutouts,high_mem`        | Comma-separated list of queues to consume from.                                                                            |
| `--log-level [CRITICAL\|ERROR\|WARNING\|INFO\|DEBUG]` | from `CUTOUTS__LOG_LEVEL` | Celery log verbosity.                                                                                                      |

### Queues

The service uses two queues by default:

| Queue      | Purpose                                                                                                                                |
| ---------- | -------------------------------------------------------------------------------------------------------------------------------------- |
| `cutouts`  | Individual cutout execution tasks (`execute_cutout`). These are CPU/memory-intensive.                                                  |
| `high_mem` | Job orchestration tasks (`schedule_job`, `batch_cutouts`, `write_results`). These require more memory for file I/O and Parquet writes. |

You can run separate worker pools for each queue to control resource allocation:

```bash
# High-memory orchestration worker (1 process)
fornax-cutouts worker -Q high_mem -c 1 -n orchestrator@%h

# Cutout execution workers (scaled to CPU count)
fornax-cutouts worker -Q cutouts -c 8 -n executor@%h
```

### Examples

Start a worker with default settings:

```bash
fornax-cutouts worker
```

Start with autoscaling between 2 and 8 processes:

```bash
fornax-cutouts worker --autoscale 8,2
```

Start with a named worker consuming only the `cutouts` queue:

```bash
fornax-cutouts worker -n cutouts-worker@myhost -Q cutouts -c 4
```

---

## Environment Variable Defaults

Both commands read `CUTOUTS__LOG_LEVEL` from the environment to set their default log level. This means you can set the log level once in your `.env` file and it applies to both the API and worker without repeating it on the command line.

See [Configuration](configuration.md) for all available environment variables.
