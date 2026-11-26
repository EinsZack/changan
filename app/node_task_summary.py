"""Read-only task assignment and execution summary for one physical node."""
from datetime import datetime, timezone
import json

from . import db
from .cluster_operations import SCOPED_ATTEMPTS


def node_task_summary(node_id: str, executor_ids: list[str]) -> dict:
    # Recorded physical identity takes precedence. A current, unique machine binding
    # can resolve unlabelled active work, but cannot prove ownership of historical work.
    rows = db.query(f"""
        WITH scoped AS ({SCOPED_ATTEMPTS}), attributed AS (
            SELECT attempt_id, MAX(phase) AS phase, MAX(physical_node_id) AS physical_node_id,
                   COUNT(DISTINCT physical_node_id) AS node_count
            FROM scoped GROUP BY attempt_id
        ), node_shards AS (
            SELECT t.id AS task_id, t.name, s.id AS shard_id, MAX(a.updated) AS updated_at,
                   MAX(CASE WHEN s.active_attempt = a.id THEN attributed.phase END) AS phase,
                   MAX(CASE WHEN s.active_attempt = a.id AND s.status = 'SUCCEEDED'
                                 AND s.result IS NOT NULL AND a.status = 'SUCCEEDED'
                                 AND a.actual_node = a.node_id THEN 1 ELSE 0 END) AS completed
            FROM attributed
            JOIN attempts a ON a.id = attributed.attempt_id
            JOIN segments s ON s.id = a.segment_id
            JOIN tasks t ON t.id = a.task_id
            WHERE (attributed.node_count = 1 AND attributed.physical_node_id = ?)
               OR (attributed.node_count = 0 AND attributed.phase IN ('waiting','running','unverified')
                   AND s.active_attempt = a.id AND t.status NOT IN ('SUCCEEDED','FAILED','TIMED_OUT','CANCELED')
                   AND a.node_id IN (SELECT value FROM json_each(?)))
            GROUP BY t.id, t.name, s.id
        ), node_tasks AS (
            SELECT task_id, name, MAX(updated_at) AS updated_at, MIN(completed) AS completed,
                   COUNT(*) FILTER (WHERE phase = 'waiting') AS waiting_shards,
                   COUNT(*) FILTER (WHERE phase = 'running') AS running_shards,
                   COUNT(*) FILTER (WHERE phase = 'unverified') AS unverified_shards
            FROM node_shards GROUP BY task_id, name
        )
        SELECT *, (waiting_shards + running_shards + unverified_shards > 0) AS active,
               COUNT(*) OVER () AS assigned_task_count, SUM(completed) OVER () AS completed_task_count,
               SUM(waiting_shards + running_shards + unverified_shards > 0) OVER () AS active_task_count,
               SUM(waiting_shards) OVER () AS total_waiting, SUM(running_shards) OVER () AS total_running,
               SUM(unverified_shards) OVER () AS total_unverified
        FROM node_tasks ORDER BY active DESC, (running_shards > 0) DESC, updated_at DESC, task_id LIMIT 3
    """, (node_id, json.dumps(executor_ids)))

    recent = db.query(f"""
        WITH scoped AS (
            {SCOPED_ATTEMPTS}
            WHERE a.finished IS NOT NULL AND a.status IN ('SUCCEEDED','FAILED','TIMED_OUT','CANCELED')
        ), attributed AS (
            SELECT attempt_id, MAX(physical_node_id) AS physical_node_id
            FROM scoped GROUP BY attempt_id
            HAVING COUNT(DISTINCT physical_node_id) = 1
        )
        SELECT t.id AS task_id, t.name, a.status, a.finished AS finished_at
        FROM attributed
        JOIN attempts a ON a.id = attributed.attempt_id
        JOIN tasks t ON t.id = a.task_id
        JOIN segments s ON s.id = a.segment_id
        WHERE attributed.physical_node_id = ? AND a.actual_node = a.node_id
          AND (a.status != 'SUCCEEDED' OR
               (s.active_attempt = a.id AND s.status = 'SUCCEEDED' AND s.result IS NOT NULL))
        ORDER BY a.finished DESC, a.id DESC LIMIT 1
    """, (node_id,), one=True)
    if recent:
        recent["finished_at"] = datetime.fromtimestamp(recent["finished_at"], timezone.utc)

    counts = rows[0] if rows else {}
    task_fields = ("task_id", "name", "waiting_shards", "running_shards", "unverified_shards")
    return {
        "node_id": node_id, "execution_bound": bool(executor_ids),
        "assigned_task_count": counts.get("assigned_task_count", 0),
        "completed_task_count": counts.get("completed_task_count", 0),
        "active_task_count": counts.get("active_task_count", 0),
        "waiting_shards": counts.get("total_waiting", 0),
        "running_shards": counts.get("total_running", 0),
        "unverified_shards": counts.get("total_unverified", 0),
        "tasks": [{key: row[key] for key in task_fields} for row in rows if row["active"]],
        "recent_result": recent,
    }
