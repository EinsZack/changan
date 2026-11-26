from __future__ import annotations

from datetime import datetime
from threading import RLock
from typing import TypeVar
from uuid import UUID

from ..models.cluster import ClusterEvent, ClusterEventCategory, ClusterRecord
from ..models.heartbeat import NodeHeartbeatRecord
from ..models.node import NodeEvent, NodeRecord, NodeRegistrationRecord
from ..models.scheduler_execution import (
    SchedulerEventDisposition,
    SchedulerExecutionEvent,
    SchedulerExecutionProjection,
)
from .base import ClusterConflictError, DeviceFingerprintConflictError, node_deletion_event

T = TypeVar("T")


class InMemoryNodeStore:
    """Thread-safe first-stage store. Replace through the NodeStore protocol later."""

    def __init__(self) -> None:
        self._nodes: dict[UUID, NodeRecord] = {}
        self._events: dict[UUID, NodeEvent] = {}
        self._registrations: dict[UUID, NodeRegistrationRecord] = {}
        self._heartbeats: dict[UUID, NodeHeartbeatRecord] = {}
        self._clusters: dict[str, ClusterRecord] = {}
        self._cluster_events: list[ClusterEvent] = []
        self._scheduler_event_ids: set[str] = set()
        self._scheduler_executions: dict[str, SchedulerExecutionProjection] = {}
        self._lock = RLock()

    @staticmethod
    def _copy(value: T) -> T:
        model_copy = getattr(value, "model_copy", None)
        return model_copy(deep=True) if model_copy else value

    def get_node(self, node_id: UUID) -> NodeRecord | None:
        with self._lock:
            node = self._nodes.get(node_id)
            return self._copy(node) if node else None

    def get_node_by_device_fingerprint(
        self, device_fingerprint: str
    ) -> NodeRecord | None:
        with self._lock:
            node = next(
                (
                    candidate
                    for candidate in self._nodes.values()
                    if candidate.device_fingerprint == device_fingerprint
                ),
                None,
            )
            return self._copy(node) if node else None

    def save_node(self, node: NodeRecord, *, cluster_events: tuple[ClusterEvent, ...] = ()) -> NodeRecord:
        with self._lock:
            if node.device_fingerprint is not None and any(
                candidate.node_id != node.node_id
                and candidate.device_fingerprint == node.device_fingerprint
                for candidate in self._nodes.values()
            ):
                raise DeviceFingerprintConflictError(
                    "device_fingerprint is already bound to another node"
                )
            self._nodes[node.node_id] = self._copy(node)
            self._cluster_events.extend(self._copy(event) for event in cluster_events)
            return self._copy(node)

    def list_nodes(self) -> list[NodeRecord]:
        with self._lock:
            return [self._copy(node) for node in self._nodes.values()]

    def list_nodes_by_cluster_id(self, cluster_id: str) -> list[NodeRecord]:
        with self._lock:
            return [
                self._copy(node)
                for node in self._nodes.values()
                if node.cluster_id == cluster_id
            ]

    def delete_node(self, node_id: UUID, *, audit_at: datetime | None = None) -> bool:
        with self._lock:
            node = self._nodes.get(node_id)
            event = node_deletion_event(node, audit_at) if node and audit_at is not None else None
            self._nodes.pop(node_id, None)
            self._events = {
                key: value
                for key, value in self._events.items()
                if value.node_id != node_id
            }
            self._registrations = {
                key: value
                for key, value in self._registrations.items()
                if value.node_id != node_id
            }
            self._heartbeats = {
                key: value
                for key, value in self._heartbeats.items()
                if value.node_id != node_id
            }
            if event is not None:
                self._cluster_events.append(event)
            return node is not None

    def add_event(self, event: NodeEvent, *, cluster_event: ClusterEvent | None = None) -> NodeEvent:
        with self._lock:
            self._events[event.id] = self._copy(event)
            if cluster_event:
                self._cluster_events.append(self._copy(cluster_event))
            return self._copy(event)

    def list_events(self, node_id: UUID | None = None) -> list[NodeEvent]:
        with self._lock:
            events = [
                self._copy(event)
                for event in self._events.values()
                if node_id is None or event.node_id == node_id
            ]
        return sorted(events, key=lambda event: event.occurred_at, reverse=True)

    def add_registration(
        self, record: NodeRegistrationRecord
    ) -> NodeRegistrationRecord:
        with self._lock:
            self._registrations[record.id] = self._copy(record)
            return self._copy(record)

    def save_registration(
        self, record: NodeRegistrationRecord
    ) -> NodeRegistrationRecord:
        with self._lock:
            self._registrations[record.id] = self._copy(record)
            return self._copy(record)

    def list_registrations(
        self, node_id: UUID | None = None
    ) -> list[NodeRegistrationRecord]:
        with self._lock:
            records = [
                self._copy(record)
                for record in self._registrations.values()
                if node_id is None or record.node_id == node_id
            ]
        return sorted(records, key=lambda record: record.started_at, reverse=True)

    def add_heartbeat(self, record: NodeHeartbeatRecord) -> NodeHeartbeatRecord:
        with self._lock:
            self._heartbeats[record.id] = self._copy(record)
            return self._copy(record)

    def list_heartbeats(self, node_id: UUID) -> list[NodeHeartbeatRecord]:
        with self._lock:
            records = [
                self._copy(record)
                for record in self._heartbeats.values()
                if record.node_id == node_id
            ]
        return sorted(records, key=lambda record: record.received_at, reverse=True)

    def create_cluster(self, cluster: ClusterRecord) -> ClusterRecord:
        with self._lock:
            if cluster.cluster_id in self._clusters:
                raise ClusterConflictError(
                    f"cluster {cluster.cluster_id} already exists"
                )
            self._clusters[cluster.cluster_id] = self._copy(cluster)
            return self._copy(cluster)

    def create_cluster_with_nodes(
        self, cluster: ClusterRecord, nodes: list[NodeRecord], *, cluster_events: tuple[ClusterEvent, ...] = ()
    ) -> ClusterRecord:
        with self._lock:
            if cluster.cluster_id in self._clusters:
                raise ClusterConflictError(
                    f"cluster {cluster.cluster_id} already exists"
                )
            for node in nodes:
                stored = self._nodes.get(node.node_id)
                if stored is None:
                    raise ClusterConflictError(f"node {node.node_id} was not found")
                if stored.cluster_id is not None:
                    raise ClusterConflictError(
                        f"NODE_ALREADY_ASSIGNED: node {node.node_id} already belongs "
                        f"to cluster {stored.cluster_id}"
                    )

            self._clusters[cluster.cluster_id] = self._copy(cluster)
            for node in nodes:
                self._nodes[node.node_id] = self._copy(node)
            self._cluster_events.extend(self._copy(event) for event in cluster_events)
            return self._copy(cluster)

    def get_cluster(self, cluster_id: str) -> ClusterRecord | None:
        with self._lock:
            cluster = self._clusters.get(cluster_id)
            return self._copy(cluster) if cluster else None

    def list_clusters(self) -> list[ClusterRecord]:
        with self._lock:
            return [self._copy(cluster) for cluster in self._clusters.values()]

    def update_cluster(self, cluster: ClusterRecord, *, cluster_events: tuple[ClusterEvent, ...] = ()) -> ClusterRecord:
        with self._lock:
            if cluster.cluster_id not in self._clusters:
                raise KeyError(cluster.cluster_id)
            self._clusters[cluster.cluster_id] = self._copy(cluster)
            self._cluster_events.extend(self._copy(event) for event in cluster_events)
            return self._copy(cluster)

    def delete_cluster(self, cluster_id: str, *, cluster_events: tuple[ClusterEvent, ...] = ()) -> bool:
        with self._lock:
            if self._clusters.pop(cluster_id, None) is None:
                return False
            self._cluster_events.extend(self._copy(event) for event in cluster_events)
            return True

    def add_cluster_event(self, event: ClusterEvent) -> None:
        with self._lock:
            self._cluster_events.append(self._copy(event))

    def list_cluster_events(
        self, cluster_id: str, category: ClusterEventCategory | None, limit: int | None
    ) -> list[ClusterEvent]:
        with self._lock:
            selected = (
                event for event in self._cluster_events
                if event.cluster_id == cluster_id and (category is None or event.category == category)
            )
            return [self._copy(event) for event in sorted(
                selected, key=lambda event: (event.occurred_at, event.id), reverse=True
            )[:limit]]

    def has_scheduler_event(self, event_id: str) -> bool:
        with self._lock:
            return event_id in self._scheduler_event_ids

    def apply_scheduler_execution_event(
        self, event: SchedulerExecutionEvent
    ) -> tuple[SchedulerEventDisposition, SchedulerExecutionProjection | None]:
        with self._lock:
            current = self._scheduler_executions.get(event.attempt_id)
            if event.event_id in self._scheduler_event_ids:
                return (
                    SchedulerEventDisposition.DUPLICATE_EVENT,
                    self._copy(current) if current else None,
                )

            self._scheduler_event_ids.add(event.event_id)
            if current is not None and event.event_version <= current.event_version:
                return SchedulerEventDisposition.STALE_VERSION, self._copy(current)

            projection = SchedulerExecutionProjection.from_event(event)
            self._scheduler_executions[event.attempt_id] = self._copy(projection)
            return SchedulerEventDisposition.APPLIED, self._copy(projection)

    def list_scheduler_executions(self) -> list[SchedulerExecutionProjection]:
        with self._lock:
            return [self._copy(item) for item in self._scheduler_executions.values()]

    def replace_scheduler_executions(
        self, executions: list[SchedulerExecutionProjection]
    ) -> None:
        with self._lock:
            self._scheduler_executions = {
                item.attempt_id: self._copy(item) for item in executions
            }

    def close(self) -> None:
        """Match the persistent store lifecycle without owning external resources."""
