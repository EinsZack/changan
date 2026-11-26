from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Callable, Literal

from ..models.compute_pool import (
    ClusterResourceSummary,
    CollectionStatus,
    ComputePoolSummary,
    ComputeResourceMetricMap,
    ResourceMetric,
)
from ..models.node import (
    AcceleratorSupport,
    AuthorizationStatus,
    NodeOperationalStatus,
    NodeRecord,
    PoolMembership,
    RegistrationStatus,
    RuntimeStatus,
)
from ..models.scheduler_execution import (
    ACTIVE_ALLOCATION_STATUSES,
    SchedulerExecutionProjection,
)
from ..stores.base import NodeStore
from .node_service import resource_sample_is_stale

ResourceUnit = Literal["Core", "GPU", "TOPS", "Device", "GB"]


@dataclass(frozen=True, slots=True)
class _Contribution:
    capacity: float
    schedulable_capacity: float
    utilization_percent: float
    used: float = 0
    runtime_available: float | None = None
    valid: bool = True


@dataclass(frozen=True, slots=True)
class SchedulerAllocationTotals:
    cpu_cores: float = 0
    gpu_count: float = 0
    vram_gb: float = 0
    memory_gb: float = 0
    npu_tops: float = 0
    dla_count: float = 0


def _round_capacity(value: float, unit: ResourceUnit) -> float:
    return round(value, 2 if unit in {"TOPS", "GB"} else 0)


def _online_capacity(node: NodeRecord, capacity: float) -> float:
    """Capacity currently offerable; offline nodes contribute nothing to available."""
    return capacity if node.operational_status == NodeOperationalStatus.ONLINE else 0


def _metric(
    contributions: list[_Contribution],
    unit: ResourceUnit,
    *,
    allocated: float = 0,
) -> ResourceMetric:
    valid = [item for item in contributions if item.valid]
    issue_count = len(contributions) - len(valid)
    total = _round_capacity(sum(item.capacity for item in valid), unit)
    used = _round_capacity(sum(item.used for item in valid), unit)
    available = _round_capacity(
        sum(
            item.capacity
            if item.runtime_available is None
            else item.runtime_available
            for item in valid
        ),
        unit,
    )
    schedulable = _round_capacity(
        sum(item.schedulable_capacity for item in valid), unit
    )
    weighted_utilization = sum(
        item.capacity * item.utilization_percent for item in valid
    )
    runtime_utilization = (
        round(weighted_utilization / total, 1) if total > 0 else 0.0
    )

    if issue_count == 0:
        collection_status = CollectionStatus.COMPLETE
    elif not valid:
        collection_status = CollectionStatus.ERROR
    else:
        collection_status = CollectionStatus.PARTIAL

    return ResourceMetric(
        total=total,
        used=min(total, used),
        allocated=_round_capacity(allocated, unit),
        available=min(total, available),
        schedulable_available=min(total, schedulable),
        unavailable=max(0, _round_capacity(total - schedulable, unit)),
        utilization_percent=runtime_utilization,
        runtime_utilization_percent=runtime_utilization,
        unit=unit,
        collection_status=collection_status,
        issue_node_count=issue_count,
    )


def _cpu(node: NodeRecord) -> _Contribution:
    report = node.resources
    if report is None:
        return _Contribution(0, 0, 0, valid=False)
    capacity = float(report.cpu.physical_cores or report.cpu.logical_cores)
    return _Contribution(
        capacity=capacity,
        schedulable_capacity=capacity if node.schedulable else 0,
        utilization_percent=report.cpu.utilization_percent,
        runtime_available=_online_capacity(node, capacity),
    )


def _gpu(node: NodeRecord) -> _Contribution:
    report = node.resources
    if report is None or report.gpu.error:
        return _Contribution(0, 0, 0, valid=False)
    if not report.gpu.supported:
        return _Contribution(0, 0, 0)
    if not report.gpu.available:
        return _Contribution(0, 0, 0, valid=False)

    capacity = float(report.gpu.device_count)
    utilization = report.gpu.utilization_percent or 0
    can_schedule = node.schedulable and report.gpu.available
    return _Contribution(
        capacity,
        capacity if can_schedule else 0,
        utilization,
        runtime_available=_online_capacity(node, capacity),
    )


def _video_memory(node: NodeRecord, *, include_shared: bool = False) -> _Contribution:
    report = node.resources
    if report is None or report.gpu.error:
        return _Contribution(0, 0, 0, valid=False)
    if not report.gpu.supported:
        return _Contribution(0, 0, 0)
    if not report.gpu.available:
        return _Contribution(0, 0, 0, valid=False)

    if report.gpu.memory_architecture == "unified":
        return _byte_capacity(node, "memory") if include_shared else _Contribution(0, 0, 0)

    bytes_per_gib = 1024**3
    total = report.gpu.total_vram_bytes / bytes_per_gib
    used = report.gpu.used_vram_bytes / bytes_per_gib
    available = report.gpu.available_vram_bytes / bytes_per_gib
    schedulable = available if node.schedulable else 0
    memory_utilization = (used / total * 100) if total > 0 else 0
    return _Contribution(
        capacity=total,
        schedulable_capacity=schedulable,
        utilization_percent=memory_utilization,
        used=used,
        runtime_available=available,
    )


def _byte_capacity(node: NodeRecord, kind: Literal["memory", "storage"]) -> _Contribution:
    report = node.resources
    if report is None:
        return _Contribution(0, 0, 0, valid=False)
    capacity = getattr(report, kind)
    bytes_per_gib = 1024**3
    total = capacity.total_bytes / bytes_per_gib
    used = capacity.used_bytes / bytes_per_gib
    available = capacity.available_bytes / bytes_per_gib
    return _Contribution(
        capacity=total,
        schedulable_capacity=available if node.schedulable else 0,
        utilization_percent=capacity.utilization_percent,
        used=used,
        runtime_available=available,
    )


def _accelerator(
    node: NodeRecord,
    support: AcceleratorSupport | None,
    capacity: Callable[[AcceleratorSupport], float],
) -> _Contribution:
    if node.resources is None or support is None or support.error:
        return _Contribution(0, 0, 0, valid=False)
    if not support.supported:
        return _Contribution(0, 0, 0)

    amount = float(capacity(support))
    can_schedule = node.schedulable and support.available
    return _Contribution(
        amount,
        amount if can_schedule else 0,
        support.utilization_percent or 0,
        runtime_available=_online_capacity(node, amount),
    )


def _is_pool_member(node: NodeRecord) -> bool:
    return (
        node.registration_status == RegistrationStatus.REGISTERED
        and node.ever_approved
        and node.authorization_status == AuthorizationStatus.APPROVED
        and node.pool_membership == PoolMembership.IN_POOL
    )


def _current_resource_contributors(nodes: list[NodeRecord]) -> list[NodeRecord]:
    """Return nodes that contribute to the current physical resource pool.

    Resource snapshots intentionally survive disconnects for diagnostics.  They
    are historical evidence, not current capacity, so every current aggregate
    must use this stricter set.  Scheduling pause is deliberately not checked:
    an online paused node still owns physical capacity while contributing zero
    schedulable capacity through the per-resource helpers.
    """

    return [
        node
        for node in nodes
        if (
            _is_pool_member(node)
            and node.runtime_status == RuntimeStatus.ONLINE
            and node.operational_status == NodeOperationalStatus.ONLINE
        )
    ]


def scheduler_allocation_totals(
    nodes: list[NodeRecord],
    executions: list[SchedulerExecutionProjection],
) -> SchedulerAllocationTotals:
    """Sum only explicit Scheduler allocations for active projections.

    Missing allocation fields remain zero rather than being inferred from node
    utilization or task presence.
    """

    node_ids = {node.node_id for node in nodes}
    cpu = gpu = vram = memory = npu = dla = 0.0
    for execution in executions:
        if (
            execution.physical_node_id not in node_ids
            or execution.status not in ACTIVE_ALLOCATION_STATUSES
            or execution.allocated_resources is None
        ):
            continue
        resources = execution.allocated_resources
        cpu += resources.cpu_cores or 0
        gpu += resources.gpu_count or 0
        vram += resources.vram_gb or 0
        memory += resources.memory_gb or 0
        npu += resources.npu_tops or 0
        dla += resources.dla_count or 0
    return SchedulerAllocationTotals(
        cpu_cores=cpu,
        gpu_count=gpu,
        vram_gb=vram,
        memory_gb=memory,
        npu_tops=npu,
        dla_count=dla,
    )


def aggregate_compute_pool(
    nodes: list[NodeRecord],
    updated_at: datetime | None = None,
    allocations: SchedulerAllocationTotals | None = None,
) -> ComputePoolSummary:
    contributors = _current_resource_contributors(nodes)
    active_allocations = allocations or SchedulerAllocationTotals()
    return ComputePoolSummary(
        unified_memory_gpu_nodes=sum(
            1
            for node in contributors
            if node.resources is not None and node.resources.gpu.memory_architecture == "unified"
        ),
        resources=ComputeResourceMetricMap(
            cpu=_metric(
                [_cpu(node) for node in contributors],
                "Core",
                allocated=active_allocations.cpu_cores,
            ),
            gpu=_metric(
                [_gpu(node) for node in contributors],
                "GPU",
                allocated=active_allocations.gpu_count,
            ),
            npu=_metric(
                [
                    _accelerator(
                        node,
                        node.resources.npu if node.resources else None,
                        lambda support: support.tops,
                    )
                    for node in contributors
                ],
                "TOPS",
                allocated=active_allocations.npu_tops,
            ),
            dla=_metric(
                [
                    _accelerator(
                        node,
                        node.resources.dla if node.resources else None,
                        lambda support: support.device_count,
                    )
                    for node in contributors
                ],
                "Device",
                allocated=active_allocations.dla_count,
            ),
        ),
        gpu_accessible_memory=_metric(
            [_video_memory(node, include_shared=True) for node in contributors], "GB"
        ),
        video_memory=_metric(
            [_video_memory(node) for node in contributors],
            "GB",
            allocated=active_allocations.vram_gb,
        ),
        memory=_metric(
            [_byte_capacity(node, "memory") for node in contributors],
            "GB",
            allocated=active_allocations.memory_gb,
        ),
        storage=_metric(
            [_byte_capacity(node, "storage") for node in contributors], "GB"
        ),
        updated_at=updated_at or datetime.now(timezone.utc),
    )


def aggregate_cluster_resources(
    nodes: list[NodeRecord],
    updated_at: datetime | None = None,
    allocations: SchedulerAllocationTotals | None = None,
) -> ClusterResourceSummary:
    measured_at = updated_at or datetime.now(timezone.utc)
    active_allocations = allocations or SchedulerAllocationTotals()
    compute = aggregate_compute_pool(nodes, measured_at, active_allocations)
    return ClusterResourceSummary(
        resources=compute.resources,
        video_memory=compute.video_memory,
        gpu_accessible_memory=compute.gpu_accessible_memory,
        memory=compute.memory,
        storage=compute.storage,
        unified_memory_gpu_nodes=compute.unified_memory_gpu_nodes,
        updated_at=measured_at,
    )


class ResourceAggregationService:
    def __init__(self, store: NodeStore) -> None:
        self._store = store

    def get_summary(self) -> ComputePoolSummary:
        nodes = self._store.list_nodes()
        contributors = _current_resource_contributors(nodes)
        allocations = scheduler_allocation_totals(
            contributors, self._store.list_scheduler_executions()
        )
        return aggregate_compute_pool(nodes, allocations=allocations)

    def get_cluster_summary(self, cluster_id: str) -> ClusterResourceSummary:
        nodes = self._store.list_nodes_by_cluster_id(cluster_id)
        contributors = _current_resource_contributors(nodes)
        allocations = scheduler_allocation_totals(
            contributors, self._store.list_scheduler_executions()
        )
        return aggregate_cluster_resources(nodes, allocations=allocations)


def cluster_runtime_load(nodes: list[NodeRecord], max_age: float, now: datetime) -> dict:
    """Use the pool's contribution and weighting rules, with missing telemetry excluded."""
    stale_ids = {
        node.node_id for node in nodes
        if _is_pool_member(node) and node.runtime_status == RuntimeStatus.ONLINE
        and node.resources is not None and resource_sample_is_stale(node, now, max_age)
    }
    fresh = [
        node for node in _current_resource_contributors(nodes)
        if node.resources is not None and node.node_id not in stale_ids
    ]
    reports = [(node, node.resources) for node in fresh if node.resources is not None]

    def utilization(contributions: list[_Contribution], unit: ResourceUnit) -> float | None:
        metric = _metric(contributions, unit)
        return metric.runtime_utilization_percent if metric.total > 0 else None

    def accelerator_state(kind: str) -> str:
        if not reports:
            return "unavailable"
        supports = [getattr(report, kind) for _, report in reports]
        if all(item is not None and not item.supported and not item.error for item in supports):
            return "unsupported"
        return "unreported"

    shared = sum(report.gpu.supported and report.gpu.memory_architecture == "unified"
                 for _, report in reports)
    dedicated = any(report.gpu.supported and report.gpu.memory_architecture != "unified"
                    for _, report in reports)
    return {
        "telemetry": {
            "gpu": accelerator_state("gpu"),
            "npu": accelerator_state("npu"),
            "dla": accelerator_state("dla"),
            "video_memory": "unified" if shared and not dedicated else accelerator_state("gpu"),
        },
        "status": "live" if reports else "stale" if stale_ids else "unavailable",
        "sampled_at": max((report.collected_at for _, report in reports), default=None),
        "reporting_nodes": len(reports),
        "stale_nodes": len(stale_ids),
        "unified_memory_gpu_nodes": sum(
            report.gpu.supported and report.gpu.memory_architecture == "unified"
            for _, report in reports
        ),
        "values": {
            "cpu": utilization([_cpu(node) for node, _ in reports], "Core"),
            "gpu": utilization([
                _gpu(node) for node, report in reports if report.gpu.utilization_percent is not None
            ], "GPU"),
            "memory": utilization([_byte_capacity(node, "memory") for node, _ in reports], "GB"),
            "video_memory": utilization([_video_memory(node) for node, _ in reports], "GB"),
            "npu": utilization([
                _accelerator(node, report.npu, lambda support: support.tops)
                for node, report in reports if report.npu.utilization_percent is not None
            ], "TOPS"),
            "dla": utilization([
                _accelerator(node, report.dla, lambda support: support.device_count)
                for node, report in reports if report.dla.utilization_percent is not None
            ], "Device"),
        },
    }
