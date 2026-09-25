---
title: Configuration
description: All environment variables and settings for Fornax Cutouts
---

# Configuration

All configuration is managed via environment variables. Fornax Cutouts uses [pydantic-settings](https://docs.pydantic.dev/latest/concepts/pydantic_settings/) with the prefix `CUTOUTS__` and `__` as the nested delimiter.

Settings can be provided as environment variables or in a `.env` file in the working directory.

---

## Core Settings

| Environment Variable              | Type     | Default           | Required | Description                                                                                                                          |
| --------------------------------- | -------- | ----------------- | -------- | ------------------------------------------------------------------------------------------------------------------------------------ |
| `CUTOUTS__SOURCE_PATH`            | `path`   | —                 | **Yes**  | Path to the directory containing mission source `.py` files. All `.py` files under this path are discovered and executed at startup. |
| `CUTOUTS__SERVICE_NAME`           | `string` | `Fornax Cutouts`  | No       | Display name for the service. When `CUTOUTS__WORKER__REDIS_PREFIX` is unset, it defaults to this value lowercased with spaces as hyphens (`fornax-cutouts`). |
| `CUTOUTS__DEPLOYMENT_TYPE`        | `string` | `local`           | No       | Deployment mode: `local` or `aws`.                                                                                                   |
| `CUTOUTS__DEPLOYMENT_ENVIRONMENT` | `string` | `dev`             | No       | Environment label: `sb`, `dev`, `test`, or `prod`. Exposed on the health endpoint when not in an ops-only deployment.                |
| `CUTOUTS__NUM_TRUSTED_PROXIES`    | `int`    | `0`               | No       | Number of trusted reverse proxies in front of the API (used for client IP when cutout limiting is enabled). See [Auth overview](auth/overview.md). |
| `CUTOUTS__SYNC_TTL`               | `int`    | `3600`            | No       | Expiration in seconds for presigned S3 URLs (sync API signing and worker signing when enabled). Default is 1 hour.                   |
| `CUTOUTS__ASYNC_TTL`              | `int`    | `604800`          | No       | Time-to-live in seconds for async job state in Redis, worker presigned URLs for async missions, and the reported UWS destruction time. Default is 1 week. |

---

## Logging Settings

Nested under `CUTOUTS__LOG__`.

| Environment Variable     | Type     | Default | Description                                                                                    |
| ------------------------ | -------- | ------- | ---------------------------------------------------------------------------------------------- |
| `CUTOUTS__LOG__LEVEL`    | `string` | `info`  | Log level for the API and worker. Values: `critical`, `error`, `warning`, `info`, `debug`.   |
| `CUTOUTS__LOG__FORMAT`   | `string` | `text`  | Log format: `text` (human-readable) or `json` (structured JSON for log aggregators).           |
| `CUTOUTS__LOG__NAME`     | `string` | `fornax_cutouts` | Logger name used by the shared logging setup.                                          |

The CLI (`fornax-cutouts api` / `worker`) uses `CUTOUTS__LOG__LEVEL` as the default for `--log-level` when not passed on the command line.

---

## Redis Settings

Nested under `CUTOUTS__REDIS__`.

| Environment Variable         | Type     | Default     | Description                                                                                                                              |
| ---------------------------- | -------- | ----------- | ---------------------------------------------------------------------------------------------------------------------------------------- |
| `CUTOUTS__REDIS__HOST`       | `string` | `localhost` | Redis server hostname or IP address.                                                                                                     |
| `CUTOUTS__REDIS__PORT`       | `int`    | `6379`      | Redis server port.                                                                                                                       |
| `CUTOUTS__REDIS__IS_CLUSTER` | `bool`   | `false`     | Set to `true` to use the Redis Cluster client instead of the standard client.                                                            |
| `CUTOUTS__REDIS__USE_SSL`    | `bool`   | `false`     | Enable TLS for the Redis connection.                                                                                                     |
| `CUTOUTS__REDIS__TIMEOUT`    | `float`  | `15.0`      | Connection timeout in seconds.                                                                                                           |
| `CUTOUTS__REDIS__SEARCH_EN`  | `bool`   | `false`     | Enable RediSearch index for fast filtered job listing by phase and creation time. Requires the RediSearch module on your Redis instance. |

---

## Worker Settings

Nested under `CUTOUTS__WORKER__`.

| Environment Variable                     | Type     | Default          | Description                                                                                                               |
| ---------------------------------------- | -------- | ---------------- | ------------------------------------------------------------------------------------------------------------------------- |
| `CUTOUTS__WORKER__REDIS_PREFIX`          | `string` | `fornax-cutouts` | Prefix for all Redis keys created by the service. Change this to namespace multiple deployments sharing a Redis instance. |
| `CUTOUTS__WORKER__BATCH_SIZE_PER_WORKER` | `int`    | `5`              | Number of cutout tasks dispatched per worker per batch. Tuning this affects memory usage and throughput.                  |
| `CUTOUTS__WORKER__PREFETCH_MULTIPLIER`   | `int`    | `1`              | Celery prefetch multiplier. Keep at `1` for memory-intensive workloads.                                                   |
| `CUTOUTS__WORKER__MAX_TASKS_PER_CHILD`   | `int`    | `50`             | Number of tasks a worker process handles before being recycled. Helps prevent memory leaks in long-running workers.       |
| `CUTOUTS__WORKER__BATCH_WATCHDOG_TIMEOUT_MINUTES` | `int` | `45` | Minutes after a batch chord is dispatched before a watchdog task runs to recover stuck or incomplete batches. |

---

## Storage Settings

Nested under `CUTOUTS__STORAGE__`.

| Environment Variable                    | Type     | Default | Description                                                                                                                |
| --------------------------------------- | -------- | ------- | -------------------------------------------------------------------------------------------------------------------------- |
| `CUTOUTS__STORAGE__PREFIX`              | `string` | `/tmp`  | Root path for storing cutout output files. Use a local path for development or an `s3://bucket/prefix` URI for production. |
| `CUTOUTS__STORAGE__RETURN_SIGNED_URLS`  | `bool`   | `false` | When `true` and storage is S3-backed, workers replace `s3://` result paths with presigned HTTPS URLs before writing job results. Sync endpoints still sign any remaining `s3://` paths in the API layer. TTL uses `CUTOUTS__SYNC_TTL` for sync missions and `CUTOUTS__ASYNC_TTL` otherwise. |

:::{tip} S3 Storage
When `CUTOUTS__STORAGE__PREFIX` starts with `s3://`, the service uses [s3fs](https://filesystem-spec.readthedocs.io/) for all file I/O. Ensure the process has the necessary IAM permissions to write to the target bucket.

For local filesystem storage, sync responses strip the storage prefix from paths so clients receive web-relative paths (for example `/cutouts/sync/...`).
:::

---

## Cutout limit (auth)

Rate limiting for `POST /cutouts/async` is configured under `CUTOUTS__CUTOUT_LIMIT__*` and is **disabled by default**. See [Auth overview](auth/overview.md) for `ENABLED`, `ANON_CUTOUT_LIMIT`, `WINDOW_SECONDS`, and `PRINCIPAL_RESOLVER`.

---

## Deployment Constants

These variables are read directly from the environment (no `CUTOUTS__` prefix) and affect runtime behavior.

| Environment Variable | Default     | Description                                                                                                                                                                                            |
| -------------------- | ----------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `AWS_S3_REGION`      | `us-east-1` | AWS region used for DuckDB S3 access when querying Parquet files stored in S3.                                                                                                                         |
| `S3FS_BLOCK_SIZE`    | —           | Optional. S3 block size in MiB for astrocut file reads. When set, overrides the default fsspec block size via a monkey-patch applied at worker startup. Useful for tuning large FITS file performance. |

---

## Example `.env` File

```bash
# Required
CUTOUTS__SOURCE_PATH=/opt/cutouts/sources

# Redis
CUTOUTS__REDIS__HOST=my-redis-cluster.example.com
CUTOUTS__REDIS__PORT=6379
CUTOUTS__REDIS__IS_CLUSTER=true
CUTOUTS__REDIS__USE_SSL=true

# Worker
CUTOUTS__WORKER__BATCH_SIZE_PER_WORKER=10
CUTOUTS__WORKER__MAX_TASKS_PER_CHILD=100

# Storage (S3)
CUTOUTS__STORAGE__PREFIX=s3://my-cutouts-bucket/results

# TTLs
CUTOUTS__SYNC_TTL=3600
CUTOUTS__ASYNC_TTL=604800

# Logging
CUTOUTS__LOG__LEVEL=info
CUTOUTS__LOG__FORMAT=json

# Storage (presign at worker — optional; sync API signs s3:// paths when false)
CUTOUTS__STORAGE__RETURN_SIGNED_URLS=false

# Deployment
CUTOUTS__DEPLOYMENT_TYPE=aws
CUTOUTS__DEPLOYMENT_ENVIRONMENT=prod
AWS_S3_REGION=us-east-1
```

---

## Configuration Model Reference

The full configuration is defined in [`fornax_cutouts/config.py`](https://github.com/nasa-fornax/fornax-cutouts/blob/main/fornax_cutouts/config.py). The top-level `CONFIG` singleton is instantiated at import time and shared across the API and worker processes.
