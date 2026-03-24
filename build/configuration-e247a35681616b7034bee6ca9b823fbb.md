---
title: Configuration
description: All environment variables and settings for Fornax Cutouts
---

# Configuration

All configuration is managed via environment variables. Fornax Cutouts uses [pydantic-settings](https://docs.pydantic.dev/latest/concepts/pydantic_settings/) with the prefix `CUTOUTS__` and `__` as the nested delimiter.

Settings can be provided as environment variables or in a `.env` file in the working directory.

---

## Core Settings

| Environment Variable   | Type     | Default   | Required | Description                                                                                                                          |
| ---------------------- | -------- | --------- | -------- | ------------------------------------------------------------------------------------------------------------------------------------ |
| `CUTOUTS__SOURCE_PATH` | `path`   | —         | **Yes**  | Path to the directory containing mission source `.py` files. All `.py` files under this path are discovered and executed at startup. |
| `CUTOUTS__LOG_LEVEL`   | `string` | `info`    | No       | Log level for the API and worker. Accepted values: `critical`, `error`, `warning`, `info`, `debug`.                                  |
| `CUTOUTS__SYNC_TTL`    | `int`    | `3600`    | No       | Time-to-live in seconds for presigned S3 URLs returned by sync cutout endpoints. Default is 1 hour.                                  |
| `CUTOUTS__ASYNC_TTL`   | `int`    | `1209600` | No       | Time-to-live in seconds for async job results stored in Redis. Default is 2 weeks.                                                   |

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

---

## Storage Settings

Nested under `CUTOUTS__STORAGE__`.

| Environment Variable       | Type     | Default | Description                                                                                                                |
| -------------------------- | -------- | ------- | -------------------------------------------------------------------------------------------------------------------------- |
| `CUTOUTS__STORAGE__PREFIX` | `string` | `/tmp`  | Root path for storing cutout output files. Use a local path for development or an `s3://bucket/prefix` URI for production. |

:::{tip} S3 Storage
When `CUTOUTS__STORAGE__PREFIX` starts with `s3://`, the service uses [fsspec](https://filesystem-spec.readthedocs.io/) for all file I/O. Ensure the process has the necessary IAM permissions to write to the target bucket.
:::

---

## Deployment Constants

These variables are read directly from the environment (no `CUTOUTS__` prefix) and affect runtime behavior.

| Environment Variable | Default     | Description                                                                                                                                                                                            |
| -------------------- | ----------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `ENVIRONMENT_NAME`   | `dev`       | Deployment environment name. When set to `ops`, the `environment` field is omitted from health check responses. When set to `dev`, the `/api/dev/flushdb` endpoint is available.                       |
| `DEPLOYMENT_TYPE`    | `local`     | Deployment context identifier. Used for logging and observability.                                                                                                                                     |
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
CUTOUTS__ASYNC_TTL=1209600

# Logging
CUTOUTS__LOG_LEVEL=info

# Deployment
ENVIRONMENT_NAME=prod
AWS_S3_REGION=us-east-1
```

---

## Configuration Model Reference

The full configuration is defined in [`fornax_cutouts/config.py`](https://github.com/nasa-fornax/fornax-cutouts/blob/main/fornax_cutouts/config.py). The top-level `CONFIG` singleton is instantiated at import time and shared across the API and worker processes.
