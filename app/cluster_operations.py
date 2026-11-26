"""Read-only cluster attribution based on the scheduler's stored dispatch decisions."""
from collections import defaultdict
from datetime import datetime, timedelta, timezone

from . import db

TASK_WINDOW_HOURS = 24

# The selected candidate stores both the logical cluster and the physical node as
# they were at dispatch. The K3s cluster name and today's membership are different.
SCOPED_ATTEMPTS = """
    SELECT a.id AS attempt_id, a.task_id, a.segment_id,
           json_array_length(candidate.value, '$.pool.reasons') AS admission_reasons,
           json_extract(candidate.value, '$.pool.cluster_id') AS cluster_id,
           json_extract(candidate.value, '$.pool.pool_node_id') AS physical_node_id,
           a.actual_node = a.node_id AS execution_confirmed,
           CASE
             WHEN t.status = 'SUCCEEDED' AND a.status = 'SUCCEEDED'
               THEN CASE WHEN a.actual_node = a.node_id THEN 'succeeded' ELSE 'unverified' END
             WHEN t.status IN ('FAILED','TIMED_OUT') AND s.status NOT IN ('SUCCEEDED','CANCELED')
               AND (a.actual_node IS NULL OR a.actual_node = a.node_id)
               AND (a.status IN ('FAILED','TIMED_OUT') OR
                    (s.active_attempt = a.id AND s.status IN ('FAILED','TIMED_OUT') AND a.status != 'SUCCEEDED'))
               THEN 'failed'
             WHEN t.status NOT IN ('SUCCEEDED','FAILED','TIMED_OUT','CANCELED') AND s.active_attempt = a.id
               THEN CASE
                 WHEN a.actual_node = a.node_id AND a.status IN ('RUNNING','UPLOADING','FINALIZING')
                   THEN 'running'
                 WHEN (a.actual_node IS NULL OR a.actual_node = a.node_id)
                   AND a.status IN ('PENDING','WAITING_SESSION','DISPATCHING','SUBMITTED',
                                    'PULLING','CONTAINER_STARTING','MODEL_LOADING','ACCEPTED')
                   THEN 'waiting'
                 WHEN a.status IN ('RUNNING','UPLOADING','FINALIZING','UNKNOWN','LOST')
                   THEN 'unverified'
               END
           END AS phase
    FROM attempts a
    JOIN tasks t ON t.id = a.task_id
    JOIN segments s ON s.id = a.segment_id AND s.task_id = a.task_id
    LEFT JOIN json_each(CASE WHEN json_valid(a.decision) THEN json_extract(a.decision, '$.candidates') END) candidate
      ON json_extract(CASE WHEN candidate.type = 'object' THEN candidate.value END, '$.node_id') = a.node_id
     AND json_extract(CASE WHEN candidate.type = 'object' THEN candidate.value END, '$.pool.state') = 'BOUND'
"""

TASK_CLUSTER_FILTER = f"""id IN (
    SELECT task_id FROM ({SCOPED_ATTEMPTS})
    WHERE cluster_id = ? AND physical_node_id IS NOT NULL
      AND (execution_confirmed = 1 OR phase IN ('waiting','failed'))
)"""


def cluster_task_statistics(cluster_id: str, now: datetime | None = None) -> dict:
    now = now or datetime.now(timezone.utc)
    window_start = now - timedelta(hours=TASK_WINDOW_HOURS)
    rows = db.query(f"""
        SELECT task_id, segment_id, cluster_id, physical_node_id, phase
        FROM ({SCOPED_ATTEMPTS} WHERE
              t.status NOT IN ('SUCCEEDED','FAILED','TIMED_OUT','CANCELED')
              OR t.finished BETWEEN ? AND ?)
        WHERE phase IS NOT NULL
    """, (window_start.timestamp(), now.timestamp()))
    confirmed = [
        row for row in rows
        if row["cluster_id"] and row["physical_node_id"] and row["phase"] != "unverified"
    ]
    cluster_rows = [row for row in confirmed if row["cluster_id"] == cluster_id]
    coverage = "available"
    if rows and not confirmed:
        coverage = "unavailable"
    elif len(confirmed) < len(rows):
        coverage = "partial"
    task_ids: dict[str, set[str]] = {}
    for row in cluster_rows:
        task_ids.setdefault(row["phase"], set()).add(row["task_id"])
    running = task_ids.get("running", set())
    waiting = task_ids.get("waiting", set())
    active_shards = defaultdict(set)
    for row in cluster_rows:
        if row["phase"] == "running":
            active_shards[row["physical_node_id"]].add(row["segment_id"])
    return {
        "active_shards_by_node": {node: len(shards) for node, shards in active_shards.items()},
        "coverage": coverage,
        "window_hours": TASK_WINDOW_HOURS,
        "window_start": window_start,
        "running_tasks": None if coverage == "unavailable" else len(running),
        "waiting_tasks": None if coverage == "unavailable" else len(waiting - running),
        "succeeded_tasks": None if coverage == "unavailable" else len(task_ids.get("succeeded", set())),
        "failed_tasks": None if coverage == "unavailable" else len(task_ids.get("failed", set())),
        "executing_nodes": None if coverage == "unavailable" else len({
            row["physical_node_id"] for row in cluster_rows if row["phase"] == "running"
        }),
        "waiting_shards": None if coverage == "unavailable" else len({
            row["segment_id"] for row in cluster_rows if row["phase"] == "waiting"
        }),
    }
