from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from threading import RLock
from uuid import UUID

from ..models.cluster import ClusterEvent, ClusterEventCategory, ClusterRecord, ClusterStatus
from ..models.heartbeat import NodeHeartbeatRecord
from ..models.node import NodeEvent, NodeRecord, NodeRegistrationRecord
from ..models.scheduler_execution import (
    SchedulerEventDisposition,
    SchedulerExecutionEvent,
    SchedulerExecutionProjection,
)
from .base import ClusterConflictError, DeviceFingerprintConflictError, node_deletion_event


class SQLiteNodeStore:
    """SQLite-backed node store preserving identity, authorization and history."""

    def __init__(self, database_path: Path | str) -> None:
        path = (
            str(database_path)
            if str(database_path) == ":memory:"
            else str(Path(database_path).expanduser())
        )
        if path != ":memory:":
            Path(path).resolve().parent.mkdir(parents=True, exist_ok=True)
        self._connection = sqlite3.connect(path, check_same_thread=False)
        self._connection.row_factory = sqlite3.Row
        self._lock = RLock()
        self._initialize()

    def _initialize(self) -> None:
        with self._lock, self._connection:
            if self._connection.execute("PRAGMA journal_mode").fetchone()[0] != "memory":
                self._connection.execute("PRAGMA journal_mode = WAL")
            self._connection.execute("PRAGMA foreign_keys = ON")
            self._connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS nodes (
                    node_id TEXT PRIMARY KEY,
                    device_fingerprint TEXT,
                    cluster_id TEXT,
                    payload TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS clusters (
                    cluster_id TEXT PRIMARY KEY,
                    cluster_name TEXT NOT NULL,
                    version TEXT NOT NULL,
                    status TEXT NOT NULL,
                    enterprise TEXT NOT NULL,
                    project TEXT NOT NULL,
                    management_endpoint TEXT,
                    last_sync_time TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS cluster_events (
                    id TEXT PRIMARY KEY,
                    cluster_id TEXT NOT NULL,
                    category TEXT NOT NULL,
                    occurred_at TEXT NOT NULL,
                    payload TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_cluster_events_cluster_time
                    ON cluster_events(cluster_id, occurred_at DESC, id DESC);

                CREATE TABLE IF NOT EXISTS node_events (
                    id TEXT PRIMARY KEY,
                    node_id TEXT NOT NULL,
                    occurred_at TEXT NOT NULL,
                    payload TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_node_events_node_time
                    ON node_events(node_id, occurred_at DESC);

                CREATE TABLE IF NOT EXISTS node_registrations (
                    id TEXT PRIMARY KEY,
                    node_id TEXT NOT NULL,
                    started_at TEXT NOT NULL,
                    payload TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_node_registrations_node_time
                    ON node_registrations(node_id, started_at DESC);

                CREATE TABLE IF NOT EXISTS node_heartbeats (
                    id TEXT PRIMARY KEY,
                    node_id TEXT NOT NULL,
                    received_at TEXT NOT NULL,
                    payload TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_node_heartbeats_node_time
                    ON node_heartbeats(node_id, received_at DESC);

                CREATE TABLE IF NOT EXISTS scheduler_execution_events (
                    event_id TEXT PRIMARY KEY,
                    attempt_id TEXT NOT NULL,
                    event_version INTEGER NOT NULL,
                    received_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS scheduler_executions (
                    attempt_id TEXT PRIMARY KEY,
                    physical_node_id TEXT NOT NULL,
                    task_id TEXT NOT NULL,
                    event_version INTEGER NOT NULL,
                    status TEXT NOT NULL,
                    occurred_at TEXT NOT NULL,
                    payload TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_scheduler_executions_node
                    ON scheduler_executions(physical_node_id, occurred_at DESC);
                CREATE INDEX IF NOT EXISTS idx_scheduler_executions_task
                    ON scheduler_executions(task_id, occurred_at DESC);
                """
            )
            # cluster_type was removed from the model. Drop the legacy column so
            # SELECT * rows validate cleanly against the current schema.
            cluster_columns = {
                row["name"]
                for row in self._connection.execute("PRAGMA table_info(clusters)")
            }
            if "cluster_type" in cluster_columns:
                self._connection.execute(
                    "ALTER TABLE clusters DROP COLUMN cluster_type"
                )

            columns = {
                row["name"]
                for row in self._connection.execute("PRAGMA table_info(nodes)")
            }
            if "device_fingerprint" not in columns:
                self._connection.execute(
                    "ALTER TABLE nodes ADD COLUMN device_fingerprint TEXT"
                )
            if "cluster_id" not in columns:
                self._connection.execute(
                    "ALTER TABLE nodes ADD COLUMN cluster_id TEXT"
                )

            # A database created by an interrupted/manual pre-index migration
            # may already contain duplicate searchable values.  Keep the first
            # canonical row searchable and leave later rows nullable so startup
            # never fails; administrators can remove those historical records
            # with the permanent-delete API.
            duplicate_fingerprints = self._connection.execute(
                """
                SELECT device_fingerprint FROM nodes
                WHERE device_fingerprint IS NOT NULL
                GROUP BY device_fingerprint HAVING COUNT(*) > 1
                """
            ).fetchall()
            for duplicate in duplicate_fingerprints:
                rows = self._connection.execute(
                    """
                    SELECT rowid FROM nodes
                    WHERE device_fingerprint = ? ORDER BY rowid
                    """,
                    (duplicate["device_fingerprint"],),
                ).fetchall()
                for row in rows[1:]:
                    self._connection.execute(
                        "UPDATE nodes SET device_fingerprint = NULL WHERE rowid = ?",
                        (row["rowid"],),
                    )

            # Existing databases stored the whole model as JSON.  Backfill the
            # searchable column without requiring a destructive migration.
            existing_fingerprints = {
                row["device_fingerprint"]
                for row in self._connection.execute(
                    """
                    SELECT device_fingerprint FROM nodes
                    WHERE device_fingerprint IS NOT NULL
                    """
                )
            }
            rows = self._connection.execute(
                """
                SELECT node_id, payload FROM nodes
                WHERE device_fingerprint IS NULL
                """
            ).fetchall()
            for row in rows:
                try:
                    payload = json.loads(row["payload"])
                except (TypeError, json.JSONDecodeError):
                    continue
                fingerprint = payload.get("device_fingerprint")
                if fingerprint is None and isinstance(payload.get("identity"), dict):
                    fingerprint = payload["identity"].get("device_fingerprint")
                if not isinstance(fingerprint, str) or not fingerprint:
                    continue
                fingerprint = fingerprint.strip().lower()
                if fingerprint in existing_fingerprints:
                    # Keep startup compatible even if a pre-index database was
                    # manually populated with duplicate fingerprints.
                    continue
                self._connection.execute(
                    "UPDATE nodes SET device_fingerprint = ? WHERE node_id = ?",
                    (fingerprint, row["node_id"]),
                )
                existing_fingerprints.add(fingerprint)

            self._connection.execute(
                """
                CREATE UNIQUE INDEX IF NOT EXISTS idx_nodes_device_fingerprint
                ON nodes(device_fingerprint)
                WHERE device_fingerprint IS NOT NULL
                """
            )
            self._migrate_legacy_cluster_relationships()
            self._connection.execute(
                "CREATE INDEX IF NOT EXISTS idx_nodes_cluster_id ON nodes(cluster_id)"
            )

    def _migrate_legacy_cluster_relationships(self) -> None:
        """Promote historical Node.cluster references without losing old rows."""

        now = datetime.now(timezone.utc).isoformat()
        rows = self._connection.execute(
            "SELECT node_id, cluster_id, payload FROM nodes"
        ).fetchall()
        for row in rows:
            try:
                payload = json.loads(row["payload"])
            except (TypeError, json.JSONDecodeError):
                continue

            legacy = payload.get("cluster")
            cluster_id = payload.get("cluster_id") or row["cluster_id"]
            cluster_name: str | None = None
            if isinstance(legacy, dict):
                cluster_id = cluster_id or legacy.get("id")
                cluster_name = legacy.get("name")
            if not isinstance(cluster_id, str) or not cluster_id.strip():
                continue

            cluster_id = cluster_id.strip()
            cluster_name = (
                cluster_name.strip()
                if isinstance(cluster_name, str) and cluster_name.strip()
                else cluster_id
            )
            self._connection.execute(
                "UPDATE nodes SET cluster_id = ? WHERE node_id = ?",
                (cluster_id, row["node_id"]),
            )
            if payload.get("cluster_id") != cluster_id:
                payload["cluster_id"] = cluster_id
                self._connection.execute(
                    "UPDATE nodes SET payload = ? WHERE node_id = ?",
                    (
                        json.dumps(payload, ensure_ascii=False, separators=(",", ":")),
                        row["node_id"],
                    ),
                )

            # Legacy rows predate management-owned clusters. Import each unique
            # historical reference once during schema migration; new Agent
            # registrations are never allowed to create clusters this way.
            self._connection.execute(
                """
                INSERT OR IGNORE INTO clusters(
                    cluster_id, cluster_name, version, status,
                    enterprise, project, management_endpoint, last_sync_time,
                    created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, NULL, NULL, ?, ?)
                """,
                (
                    cluster_id,
                    cluster_name,
                    "legacy",
                    ClusterStatus.UNKNOWN.value,
                    "",
                    "",
                    now,
                    now,
                ),
            )

    def close(self) -> None:
        with self._lock:
            self._connection.close()

    def get_node(self, node_id: UUID) -> NodeRecord | None:
        with self._lock:
            row = self._connection.execute(
                "SELECT payload FROM nodes WHERE node_id = ?", (str(node_id),)
            ).fetchone()
        return NodeRecord.model_validate_json(row["payload"]) if row else None

    def get_node_by_device_fingerprint(
        self, device_fingerprint: str
    ) -> NodeRecord | None:
        with self._lock:
            row = self._connection.execute(
                """
                SELECT payload FROM nodes
                WHERE device_fingerprint = ?
                """,
                (device_fingerprint,),
            ).fetchone()
        return NodeRecord.model_validate_json(row["payload"]) if row else None

    def save_node(self, node: NodeRecord, *, cluster_events: tuple[ClusterEvent, ...] = ()) -> NodeRecord:
        payload = node.model_dump_json()
        try:
            with self._lock, self._connection:
                self._connection.execute(
                    """
                    INSERT INTO nodes(node_id, device_fingerprint, cluster_id, payload)
                    VALUES (?, ?, ?, ?)
                    ON CONFLICT(node_id) DO UPDATE SET
                        device_fingerprint = excluded.device_fingerprint,
                        cluster_id = excluded.cluster_id,
                        payload = excluded.payload
                    """,
                    (
                        str(node.node_id),
                        node.device_fingerprint,
                        node.cluster_id,
                        payload,
                    ),
                )
                self._insert_cluster_events(cluster_events)
        except sqlite3.IntegrityError as exc:
            if node.device_fingerprint is not None:
                raise DeviceFingerprintConflictError(
                    "device_fingerprint is already bound to another node"
                ) from exc
            raise
        return node.model_copy(deep=True)

    def list_nodes(self) -> list[NodeRecord]:
        with self._lock:
            rows = self._connection.execute("SELECT payload FROM nodes").fetchall()
        return [NodeRecord.model_validate_json(row["payload"]) for row in rows]

    def list_nodes_by_cluster_id(self, cluster_id: str) -> list[NodeRecord]:
        with self._lock:
            rows = self._connection.execute(
                "SELECT payload FROM nodes WHERE cluster_id = ?", (cluster_id,)
            ).fetchall()
        return [NodeRecord.model_validate_json(row["payload"]) for row in rows]

    @staticmethod
    def _cluster_from_row(row: sqlite3.Row) -> ClusterRecord:
        return ClusterRecord.model_validate(dict(row))

    def create_cluster(self, cluster: ClusterRecord) -> ClusterRecord:
        try:
            with self._lock, self._connection:
                self._connection.execute(
                    """
                    INSERT INTO clusters(
                        cluster_id, cluster_name, version, status,
                        enterprise, project, management_endpoint, last_sync_time,
                        created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        cluster.cluster_id,
                        cluster.cluster_name,
                        cluster.version,
                        cluster.status.value,
                        cluster.enterprise,
                        cluster.project,
                        cluster.management_endpoint,
                        cluster.last_sync_time.isoformat()
                        if cluster.last_sync_time
                        else None,
                        cluster.created_at.isoformat(),
                        cluster.updated_at.isoformat(),
                    ),
                )
        except sqlite3.IntegrityError as exc:
            raise ClusterConflictError(
                f"cluster {cluster.cluster_id} already exists"
            ) from exc
        return cluster.model_copy(deep=True)

    def create_cluster_with_nodes(
        self, cluster: ClusterRecord, nodes: list[NodeRecord], *, cluster_events: tuple[ClusterEvent, ...] = ()
    ) -> ClusterRecord:
        """Create a Cluster and bind every member in one SQLite transaction."""

        try:
            with self._lock, self._connection:
                self._connection.execute(
                    """
                    INSERT INTO clusters(
                        cluster_id, cluster_name, version, status,
                        enterprise, project, management_endpoint, last_sync_time,
                        created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        cluster.cluster_id,
                        cluster.cluster_name,
                        cluster.version,
                        cluster.status.value,
                        cluster.enterprise,
                        cluster.project,
                        cluster.management_endpoint,
                        cluster.last_sync_time.isoformat()
                        if cluster.last_sync_time
                        else None,
                        cluster.created_at.isoformat(),
                        cluster.updated_at.isoformat(),
                    ),
                )
                for node in nodes:
                    cursor = self._connection.execute(
                        """
                        UPDATE nodes
                        SET cluster_id = ?, payload = ?
                        WHERE node_id = ? AND cluster_id IS NULL
                        """,
                        (
                            cluster.cluster_id,
                            node.model_dump_json(),
                            str(node.node_id),
                        ),
                    )
                    if cursor.rowcount != 1:
                        raise ClusterConflictError(
                            "NODE_ALREADY_ASSIGNED: node "
                            f"{node.node_id} is missing or already belongs to a cluster"
                        )
                self._insert_cluster_events(cluster_events)
        except sqlite3.IntegrityError as exc:
            raise ClusterConflictError(
                f"cluster {cluster.cluster_id} already exists"
            ) from exc
        return cluster.model_copy(deep=True)

    def get_cluster(self, cluster_id: str) -> ClusterRecord | None:
        with self._lock:
            row = self._connection.execute(
                "SELECT * FROM clusters WHERE cluster_id = ?", (cluster_id,)
            ).fetchone()
        return self._cluster_from_row(row) if row else None

    def list_clusters(self) -> list[ClusterRecord]:
        with self._lock:
            rows = self._connection.execute(
                "SELECT * FROM clusters ORDER BY cluster_name COLLATE NOCASE, cluster_id"
            ).fetchall()
        return [self._cluster_from_row(row) for row in rows]

    def update_cluster(self, cluster: ClusterRecord, *, cluster_events: tuple[ClusterEvent, ...] = ()) -> ClusterRecord:
        with self._lock, self._connection:
            cursor = self._connection.execute(
                """
                UPDATE clusters SET
                    cluster_name = ?, version = ?, status = ?,
                    enterprise = ?, project = ?, management_endpoint = ?,
                    last_sync_time = ?, updated_at = ?
                WHERE cluster_id = ?
                """,
                (
                    cluster.cluster_name,
                    cluster.version,
                    cluster.status.value,
                    cluster.enterprise,
                    cluster.project,
                    cluster.management_endpoint,
                    cluster.last_sync_time.isoformat()
                    if cluster.last_sync_time
                    else None,
                    cluster.updated_at.isoformat(),
                    cluster.cluster_id,
                ),
            )
            if cursor.rowcount == 0:
                raise KeyError(cluster.cluster_id)
            self._insert_cluster_events(cluster_events)
        return cluster.model_copy(deep=True)

    def delete_cluster(self, cluster_id: str, *, cluster_events: tuple[ClusterEvent, ...] = ()) -> bool:
        with self._lock, self._connection:
            cursor = self._connection.execute(
                "DELETE FROM clusters WHERE cluster_id = ?", (cluster_id,)
            )
            if cursor.rowcount > 0:
                self._insert_cluster_events(cluster_events)
        return cursor.rowcount > 0

    def _insert_cluster_events(self, events: tuple[ClusterEvent, ...]) -> None:
        if events:
            self._connection.executemany(
                "INSERT INTO cluster_events(id, cluster_id, category, occurred_at, payload) VALUES (?, ?, ?, ?, ?)",
                [(str(event.id), event.cluster_id, event.category, event.occurred_at.isoformat(),
                  event.model_dump_json()) for event in events],
            )

    def add_cluster_event(self, event: ClusterEvent) -> None:
        with self._lock, self._connection:
            self._insert_cluster_events((event,))

    def list_cluster_events(
        self, cluster_id: str, category: ClusterEventCategory | None, limit: int | None
    ) -> list[ClusterEvent]:
        sql = "SELECT payload FROM cluster_events WHERE cluster_id = ?"
        params: list[str | int] = [cluster_id]
        if category is not None:
            sql += " AND category = ?"
            params.append(category)
        sql += " ORDER BY occurred_at DESC, id DESC"
        if limit is not None:
            sql += " LIMIT ?"
            params.append(limit)
        with self._lock:
            rows = self._connection.execute(sql, params).fetchall()
        return [ClusterEvent.model_validate_json(row["payload"]) for row in rows]

    def has_scheduler_event(self, event_id: str) -> bool:
        with self._lock:
            row = self._connection.execute(
                "SELECT 1 FROM scheduler_execution_events WHERE event_id = ?",
                (event_id,),
            ).fetchone()
        return row is not None

    def apply_scheduler_execution_event(
        self, event: SchedulerExecutionEvent
    ) -> tuple[SchedulerEventDisposition, SchedulerExecutionProjection | None]:
        received_at = datetime.now(timezone.utc)
        with self._lock, self._connection:
            inserted = self._connection.execute(
                """
                INSERT OR IGNORE INTO scheduler_execution_events(
                    event_id, attempt_id, event_version, received_at
                ) VALUES (?, ?, ?, ?)
                """,
                (
                    event.event_id,
                    event.attempt_id,
                    event.event_version,
                    received_at.isoformat(),
                ),
            )
            current_row = self._connection.execute(
                "SELECT payload FROM scheduler_executions WHERE attempt_id = ?",
                (event.attempt_id,),
            ).fetchone()
            current = (
                SchedulerExecutionProjection.model_validate_json(current_row["payload"])
                if current_row
                else None
            )

            if inserted.rowcount == 0:
                return SchedulerEventDisposition.DUPLICATE_EVENT, current
            if current is not None and event.event_version <= current.event_version:
                return SchedulerEventDisposition.STALE_VERSION, current

            projection = SchedulerExecutionProjection.from_event(
                event, updated_at=received_at
            )
            self._upsert_scheduler_execution(projection)
            return SchedulerEventDisposition.APPLIED, projection.model_copy(deep=True)

    def _upsert_scheduler_execution(
        self, projection: SchedulerExecutionProjection
    ) -> None:
        self._connection.execute(
            """
            INSERT INTO scheduler_executions(
                attempt_id, physical_node_id, task_id, event_version,
                status, occurred_at, payload
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(attempt_id) DO UPDATE SET
                physical_node_id = excluded.physical_node_id,
                task_id = excluded.task_id,
                event_version = excluded.event_version,
                status = excluded.status,
                occurred_at = excluded.occurred_at,
                payload = excluded.payload
            """,
            (
                projection.attempt_id,
                str(projection.physical_node_id),
                projection.task_id,
                projection.event_version,
                projection.status.value,
                projection.occurred_at.isoformat(),
                projection.model_dump_json(),
            ),
        )

    def list_scheduler_executions(self) -> list[SchedulerExecutionProjection]:
        with self._lock:
            rows = self._connection.execute(
                "SELECT payload FROM scheduler_executions ORDER BY occurred_at DESC"
            ).fetchall()
        return [
            SchedulerExecutionProjection.model_validate_json(row["payload"])
            for row in rows
        ]

    def replace_scheduler_executions(
        self, executions: list[SchedulerExecutionProjection]
    ) -> None:
        with self._lock, self._connection:
            self._connection.execute("DELETE FROM scheduler_executions")
            for execution in executions:
                self._upsert_scheduler_execution(execution)

    def delete_node(self, node_id: UUID, *, audit_at: datetime | None = None) -> bool:
        node_key = str(node_id)
        with self._lock, self._connection:
            deleted = self._connection.execute(
                "DELETE FROM nodes WHERE node_id = ? RETURNING payload", (node_key,)
            ).fetchone()
            self._connection.execute(
                "DELETE FROM node_events WHERE node_id = ?", (node_key,)
            )
            self._connection.execute(
                "DELETE FROM node_registrations WHERE node_id = ?", (node_key,)
            )
            self._connection.execute(
                "DELETE FROM node_heartbeats WHERE node_id = ?", (node_key,)
            )
            if deleted is not None and audit_at is not None:
                event = node_deletion_event(NodeRecord.model_validate_json(deleted["payload"]), audit_at)
                if event is not None:
                    self._insert_cluster_events((event,))
            return deleted is not None

    def add_event(self, event: NodeEvent, *, cluster_event: ClusterEvent | None = None) -> NodeEvent:
        payload = event.model_dump_json()
        with self._lock, self._connection:
            self._connection.execute(
                """
                INSERT INTO node_events(id, node_id, occurred_at, payload)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(id) DO UPDATE SET
                    node_id = excluded.node_id,
                    occurred_at = excluded.occurred_at,
                    payload = excluded.payload
                """,
                (
                    str(event.id),
                    str(event.node_id),
                    event.occurred_at.isoformat(),
                    payload,
                ),
            )
            if cluster_event:
                self._insert_cluster_events((cluster_event,))
        return event.model_copy(deep=True)

    def list_events(self, node_id: UUID | None = None) -> list[NodeEvent]:
        query = "SELECT payload FROM node_events"
        parameters: tuple[str, ...] = ()
        if node_id is not None:
            query += " WHERE node_id = ?"
            parameters = (str(node_id),)
        query += " ORDER BY occurred_at DESC"
        with self._lock:
            rows = self._connection.execute(query, parameters).fetchall()
        return [NodeEvent.model_validate_json(row["payload"]) for row in rows]

    def add_registration(
        self, record: NodeRegistrationRecord
    ) -> NodeRegistrationRecord:
        return self.save_registration(record)

    def save_registration(
        self, record: NodeRegistrationRecord
    ) -> NodeRegistrationRecord:
        payload = record.model_dump_json()
        with self._lock, self._connection:
            self._connection.execute(
                """
                INSERT INTO node_registrations(id, node_id, started_at, payload)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(id) DO UPDATE SET
                    node_id = excluded.node_id,
                    started_at = excluded.started_at,
                    payload = excluded.payload
                """,
                (
                    str(record.id),
                    str(record.node_id),
                    record.started_at.isoformat(),
                    payload,
                ),
            )
        return record.model_copy(deep=True)

    def list_registrations(
        self, node_id: UUID | None = None
    ) -> list[NodeRegistrationRecord]:
        query = "SELECT payload FROM node_registrations"
        parameters: tuple[str, ...] = ()
        if node_id is not None:
            query += " WHERE node_id = ?"
            parameters = (str(node_id),)
        query += " ORDER BY started_at DESC"
        with self._lock:
            rows = self._connection.execute(query, parameters).fetchall()
        return [
            NodeRegistrationRecord.model_validate_json(row["payload"]) for row in rows
        ]

    def add_heartbeat(self, record: NodeHeartbeatRecord) -> NodeHeartbeatRecord:
        payload = record.model_dump_json()
        with self._lock, self._connection:
            self._connection.execute(
                """
                INSERT INTO node_heartbeats(id, node_id, received_at, payload)
                VALUES (?, ?, ?, ?)
                """,
                (
                    str(record.id),
                    str(record.node_id),
                    record.received_at.isoformat(),
                    payload,
                ),
            )
        return record.model_copy(deep=True)

    def list_heartbeats(self, node_id: UUID) -> list[NodeHeartbeatRecord]:
        with self._lock:
            rows = self._connection.execute(
                """
                SELECT payload FROM node_heartbeats
                WHERE node_id = ?
                ORDER BY received_at DESC
                """,
                (str(node_id),),
            ).fetchall()
        return [NodeHeartbeatRecord.model_validate_json(row["payload"]) for row in rows]
