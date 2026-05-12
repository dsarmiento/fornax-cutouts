# Redis Key Reference

All keys in this document are prefixed with `{CONFIG.worker.redis_prefix}:`

## Global Keys

| Key                   | Type    | Description                                                                                                                             |
| --------------------- | ------- | --------------------------------------------------------------------------------------------------------------------------------------- |
| `total_pending_tasks` | Integer | Global sum of pending task queue lengths across all jobs. Used as a metric. Recalculated by scanning all per-job `pending_tasks` lists. |

## Per-Job Keys

Scoped to a single job via `{job_id}`. All keys below are under `jobs:{job_id}:`.

| Key                    | Type                 | Description                                                                                                                                             |
| ---------------------- | -------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `uws`                  | JSON                 | UWS job summary document (phase, job_id, run_id, parameters, timestamps). Indexed by `cutoutJobsIdx` for search queries on `phase` and `creation_time`. |
| `uws:positions`        | List[Position Tuple] | Serialized position strings for the job. Populated at job creation; consumed in batches via `LRANGE`.                                                   |
| `pending_tasks`        | List[JSON]           | JSON-serialized task kwargs awaiting dispatch to Celery. Tasks are right-pushed (`RPUSH`) and left-popped (`LPOP`) in FIFO order.                       |
| `failed_tasks`         | List[JSON]           | JSON-serialized task kwargs for tasks that failed, including an `error_message` field.                                                                  |
| `current_batch_num`    | Integer              | Monotonically increasing counter tracking the current batch number. Incremented via `INCR`.                                                             |
| `queued_task_count`    | Integer              | Number of tasks submitted to the Celery queue but not yet executing. Incremented/decremented as tasks move between states.                              |
| `executing_task_count` | Integer              | Number of tasks currently being executed by Celery workers.                                                                                             |
| `completed_task_count` | Integer              | Number of tasks that completed successfully.                                                                                                            |
| `skipped_task_count`   | Integer              | Number of tasks that were skipped (e.g. duplicate or already processed).                                                                                |
| `total_task_count`     | Integer              | Total number of tasks for the job, set once when tasks are enumerated.                                                                                  |

## Per-Job, Per-Batch Keys

Scoped to a specific batch within a job. All keys below are under `jobs:{job_id}:batch:{batch_num}:`. Batch keys are deleted after the batch completes via `delete_batch_keys`.

| Key              | Type    | Description                                                                                                                      |
| ---------------- | ------- | -------------------------------------------------------------------------------------------------------------------------------- |
| `outstanding` | Integer | Number of tasks in the batch that have not yet finished. Decremented as tasks complete; used to detect batch completion. |
| `descriptors` | JSON    | JSON-encoded list of task descriptor dicts for this batch. Used to reconstruct task context when assembling results.     |
| `results`        | Hash    | Map of `increment_id → result JSON` for completed tasks in this batch. Written by workers as tasks finish.                       |
| `started`        | Hash    | Map of `increment_id → "1"` tracking which tasks in the batch have been started. Used to detect and handle duplicate executions. |

## Search Index

| Name            | Type                   | Indexed Prefix | Fields                                   |
| --------------- | ---------------------- | -------------- | ---------------------------------------- |
| `cutoutJobsIdx` | RedisSearch JSON index | `jobs:*:uws`   | `phase` (Tag), `creation_time` (Numeric) |
