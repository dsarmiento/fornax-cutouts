---
title: API Reference
description: REST API endpoints for Fornax Cutouts
---

# API Reference

Fornax Cutouts exposes a REST API mounted at `/api/v0/`. The API is built with [FastAPI](https://fastapi.tiangolo.com/) and follows the [IVOA UWS 1.1](https://www.ivoa.net/documents/UWS/20161024/REC-UWS-1.1-20161024.html) standard for asynchronous job management.

## Interactive Documentation

When the service is running, full interactive documentation is available at:

| Interface    | URL                                                   | Description                                            |
| ------------ | ----------------------------------------------------- | ------------------------------------------------------ |
| Swagger UI   | [`/docs`](http://localhost:8000/docs)                 | Interactive API explorer with request/response schemas |
| ReDoc        | [`/redoc`](http://localhost:8000/redoc)               | Clean reference documentation                          |
| OpenAPI JSON | [`/openapi.json`](http://localhost:8000/openapi.json) | Raw OpenAPI 3.x schema                                 |

---

## Endpoint Groups

### Health

| Method | Path          | Description                                                                                                                                         |
| ------ | ------------- | --------------------------------------------------------------------------------------------------------------------------------------------------- |
| `GET`  | `/api/health` | Service health check. Pings Redis with a 0.5s timeout. Returns `status`, `details`, `timestamp`, and `environment` (omitted in `ops` environments). |

**Example response:**

```json
{
  "status": "ok",
  "details": "",
  "timestamp": "2026-01-01T00:00:00Z",
  "environment": "dev"
}
```

---

### Missions & Metadata

| Method | Path                          | Description                                                                                                                 |
| ------ | ----------------------------- | --------------------------------------------------------------------------------------------------------------------------- |
| `GET`  | `/api/v0/missions`            | List all registered mission sources with their metadata (name, pixel size, max cutout size, available filters and surveys). |
| `GET`  | `/api/v0/missions/{mission}`  | Metadata for a single mission. Returns 404 if the mission is not registered.                                                |
| `POST` | `/api/v0/filenames`           | Look up source FITS filenames for one or more missions given sky positions.                                                 |
| `POST` | `/api/v0/filenames/{mission}` | File lookup scoped to a specific mission.                                                                                   |

---

### Sync Cutouts

Sync endpoints execute cutouts inline and return results immediately. Suitable for single cutouts or low-latency use cases.

| Method | Path                          | Description                                                                          |
| ------ | ----------------------------- | ------------------------------------------------------------------------------------ |
| `GET`  | `/api/v0/cutouts/sync`        | Redirects to `/sync/single` preserving query parameters.                             |
| `GET`  | `/api/v0/cutouts/sync/single` | Generate a single FITS cutout. Returns a file path or presigned S3 URL.              |
| `GET`  | `/api/v0/cutouts/sync/color`  | Generate a colorized JPEG preview from three FITS files (red, green, blue channels). |

**`/sync/single` query parameters:**

| Parameter         | Type     | Required | Description                                             |
| ----------------- | -------- | -------- | ------------------------------------------------------- |
| `filename`        | `string` | Yes      | Source FITS file path or S3 URI.                        |
| `ra`              | `float`  | Yes      | Right ascension in decimal degrees (0–360).             |
| `dec`             | `float`  | Yes      | Declination in decimal degrees (−90 to 90).             |
| `size`            | `int`    | Yes      | Cutout size in pixels.                                  |
| `include_preview` | `bool`   | No       | Also generate a JPEG preview alongside the FITS cutout. |

For S3 storage backends, sync results are returned as presigned URLs valid for `CUTOUTS__SYNC_TTL` seconds (default: 1 hour).

---

### Async Cutouts (UWS)

The async endpoints implement the [IVOA UWS 1.1](https://www.ivoa.net/documents/UWS/20161024/REC-UWS-1.1-20161024.html) standard. Jobs are queued in Redis and executed by Celery workers. Results are written to Parquet files and queryable before the job finishes.

#### Job Management

| Method   | Path                             | Description                                                                                                                      |
| -------- | -------------------------------- | -------------------------------------------------------------------------------------------------------------------------------- |
| `GET`    | `/api/v0/cutouts/async`          | List jobs. Defaults to the 100 most recent. Filterable by `phase`, `after` (creation time), and `last` (count). Returns UWS XML. |
| `POST`   | `/api/v0/cutouts/async`          | Create and immediately queue a new cutout job. Returns `303 See Other` to the job resource.                                      |
| `GET`    | `/api/v0/cutouts/async/{job_id}` | Full UWS `JobSummary` for a job (XML).                                                                                           |
| `DELETE` | `/api/v0/cutouts/async/{job_id}` | Not implemented (501).                                                                                                           |

#### Job Sub-resources

| Method | Path                                                 | Description                                                                                                   |
| ------ | ---------------------------------------------------- | ------------------------------------------------------------------------------------------------------------- |
| `GET`  | `/api/v0/cutouts/async/{job_id}/phase`               | Current execution phase (e.g. `QUEUED`, `EXECUTING`, `COMPLETED`).                                            |
| `POST` | `/api/v0/cutouts/async/{job_id}/phase`               | Not implemented (501).                                                                                        |
| `GET`  | `/api/v0/cutouts/async/{job_id}/results`             | UWS results list (XML) with links to `summary` and `cutouts` sub-resources.                                   |
| `GET`  | `/api/v0/cutouts/async/{job_id}/results/summary`     | JSON object with task counters: `queued`, `executing`, `completed`, `total`.                                  |
| `GET`  | `/api/v0/cutouts/async/{job_id}/results/cutouts`     | Paginated cutout results. Supports `output_format=xml\|json\|csv\|votable`, `page`, and `limit` query params. |
| `GET`  | `/api/v0/cutouts/async/{job_id}/parameters`          | UWS job parameters (XML).                                                                                     |
| `GET`  | `/api/v0/cutouts/async/{job_id}/parameters/position` | Paginated list of input sky positions.                                                                        |
| `GET`  | `/api/v0/cutouts/async/{job_id}/executionduration`   | Execution duration (always 0; not estimated).                                                                 |
| `GET`  | `/api/v0/cutouts/async/{job_id}/error`               | Error summary if the job failed; empty 200 otherwise.                                                         |
| `GET`  | `/api/v0/cutouts/async/{job_id}/owner`               | Job owner ID.                                                                                                 |
| `GET`  | `/api/v0/cutouts/async/{job_id}/quote`               | Execution time quote (not provided; returns empty string).                                                    |

#### Submitting an Async Job

Jobs are submitted as `multipart/form-data`. Mission-specific parameters are passed as `mission_name.param_name` fields or as a JSON-encoded value under the mission name key.

**Required fields:**

| Field        | Type       | Description                                                                         |
| ------------ | ---------- | ----------------------------------------------------------------------------------- |
| `position[]` | `string[]` | One or more sky positions as `"ra,dec"` decimal strings or resolvable object names. |
| `size`       | `int`      | Cutout size in pixels.                                                              |

**Optional fields:**

| Field               | Type          | Description                                                                     |
| ------------------- | ------------- | ------------------------------------------------------------------------------- |
| `output_format[]`   | `string[]`    | Output formats: `fits` (default), `jpeg`.                                       |
| `RUNID`             | `string`      | Client-provided run identifier (max 64 chars).                                  |
| `{mission}.{param}` | `string`      | Mission-specific parameter (e.g. `ps1.filter=r`).                               |
| `{mission}`         | `JSON string` | All mission params as a JSON object (e.g. `ps1={"filter":"r","survey":"3pi"}`). |

**Example using curl:**

```bash
curl -X POST http://cutouts.mast.stsci.edu/api/v0/cutouts/async \
  -F "position[]=189.997633,-11.623054" \
  -F "position[]=241.516310,55.425480" \
  -F "size=256" \
  -F "output_format[]=fits" \
  -F "ps1.filter=r" \
  -F "ps1.survey=3pi"
```

The response redirects (`303`) to `/api/v0/cutouts/async/{job_id}`.

---

## Development-Only Endpoints

These endpoints are only available when `ENVIRONMENT_NAME=dev`:

| Method | Path               | Description                                                   |
| ------ | ------------------ | ------------------------------------------------------------- |
| `GET`  | `/api/dev/flushdb` | Flush all Redis keys. **Destructive — development use only.** |
