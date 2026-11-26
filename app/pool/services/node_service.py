from __future__ import annotations

import asyncio
import ipaddress
import os
import socket
from datetime import datetime, timezone
from functools import wraps
from threading import RLock
from typing import Any, Callable, TypeVar, cast
from urllib.parse import urljoin, urlparse
from uuid import UUID

import httpx
from pydantic import ValidationError

from ..config import Settings
from ..models.cluster import ClusterEvent
from ..models.heartbeat import NodeHeartbeatRecord
from ..models.node import (
    AuthorizationStatus,
    ClusterRef,
    HeartbeatRequest,
    HealthStatus,
    IdentityPayload,
    LinkStatus,
    ManualConnectRequest,
    NodeEvent,
    NodeOperationalStatus,
    NodeRecord,
    NodeReconnectResponse,
    NodeRegistrationResponse,
    NodeRegistrationRecord,
    PoolMembership,
    RegisterRequest,
    RegistrationResultStatus,
    RegistrationStatus,
    ResourceReportRequest,
    RuntimeStatus,
)
from ..stores.base import DeviceFingerprintConflictError, NodeStore



CLOCK_SKEW_TOLERANCE_SECONDS = float(os.getenv("NODE_CLOCK_SKEW_TOLERANCE_SECONDS", "30"))


def _sample_age_seconds(collected_at: datetime, evaluated_at: datetime) -> float:
    if collected_at.tzinfo is None:
        collected_at = collected_at.replace(tzinfo=timezone.utc)
    return (evaluated_at - collected_at).total_seconds()


def resource_sample_is_stale(node: NodeRecord, now: datetime, max_age: float) -> bool:
    return (
        node.registration_status == RegistrationStatus.REGISTERED
        and node.last_resource_at is not None
        and _sample_age_seconds(node.last_resource_at, now) >= max_age
    ) or (
        node.resources is not None
        and _sample_age_seconds(node.resources.collected_at, now) >= max_age + CLOCK_SKEW_TOLERANCE_SECONDS
    )

class NodeNotFoundError(LookupError):
    pass


class InvalidNodeTransitionError(ValueError):
    pass


class ManualConnectError(RuntimeError):
    pass


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


F = TypeVar("F", bound=Callable[..., Any])


def atomic_operation(method: F) -> F:
    """Serialize a complete service-level read/modify/write operation."""

    @wraps(method)
    def wrapped(self: "NodeService", *args: Any, **kwargs: Any) -> Any:
        with self._operation_lock:
            return method(self, *args, **kwargs)

    return cast(F, wrapped)


class NodeService:
    def __init__(self, store: NodeStore, settings: Settings) -> None:
        self._store = store
        self._settings = settings
        self._operation_lock = RLock()

    @atomic_operation
    def register(self, request: RegisterRequest) -> NodeRegistrationResponse:
        now = utc_now()
        requested_node_id = request.identity.node_id
        fingerprint = request.device_fingerprint
        fingerprint_match = (
            self._store.get_node_by_device_fingerprint(fingerprint)
            if fingerprint is not None
            else None
        )
        requested_node = self._store.get_node(requested_node_id)
        existing = fingerprint_match or requested_node

        # A fingerprint match is authoritative.  When a previous fingerprint-less
        # registration (for example manual connect) already created an alias row
        # under the freshly generated node_id, that row duplicates the same
        # physical device.  Remove it exactly like the concurrent-insert path
        # below, so the historical record remains the only surviving row.
        if (
            fingerprint_match is not None
            and requested_node is not None
            and fingerprint_match.node_id != requested_node_id
        ):
            self._store.delete_node(requested_node_id)

        if existing is None:
            node = NodeRecord(
                node_id=requested_node_id,
                device_fingerprint=fingerprint,
                node_name=request.node_name or request.identity.hostname,
                identity=request.identity,
                cluster_id=None,
                cluster=None,
                labels=request.labels,
                registration_status=RegistrationStatus.DISCOVERED,
                ever_approved=False,
                authorization_status=AuthorizationStatus.PENDING,
                pool_membership=PoolMembership.REMOVED,
                runtime_status=RuntimeStatus.ONLINE,
                discovered_at=now,
                last_registered_at=now,
                last_heartbeat_at=now,
            )
            self._event(node, "NODE_DISCOVERED", "INFO", "发现新的算力节点", now)
            node.registration_status = RegistrationStatus.PENDING_APPROVAL
            self._event(node, "PENDING_APPROVAL", "INFO", "节点等待管理员批准接入", now)
            self._store.add_registration(
                NodeRegistrationRecord(
                    node_id=node.node_id,
                    node_name=node.node_name,
                    started_at=now,
                    result="IN_PROGRESS",
                )
            )
            try:
                saved = self._save(node)
            except DeviceFingerprintConflictError as exc:
                # Another Backend worker may have committed this physical
                # device between our lookup and insert.  Remove audit rows made
                # for the losing UUID, then converge through the normal
                # fingerprint-match path instead of returning a raw 500.
                self._store.delete_node(requested_node_id)
                if (
                    fingerprint is None
                    or self._store.get_node_by_device_fingerprint(fingerprint)
                    is None
                ):
                    raise InvalidNodeTransitionError(str(exc)) from exc
                return self.register(request)
            return self._registration_response(
                saved, RegistrationResultStatus.NEW_DEVICE
            )

        if (
            fingerprint is not None
            and existing.device_fingerprint is not None
            and existing.device_fingerprint != fingerprint
        ):
            raise InvalidNodeTransitionError(
                "node_id is already bound to a different physical device"
            )

        registration_result = (
            RegistrationResultStatus.EXISTING_DEVICE
            if existing.node_id != requested_node_id
            else RegistrationResultStatus.EXISTING_NODE
        )

        # A fingerprint match is authoritative.  When a local state file was
        # lost, retain the canonical database UUID in the refreshed identity so
        # every response and subsequent report converges on the historical node.
        refreshed_identity = request.identity.model_copy(deep=True)
        refreshed_identity.node_id = existing.node_id
        refreshed_identity.device_fingerprint = (
            fingerprint or existing.device_fingerprint
        )
        existing.device_fingerprint = refreshed_identity.device_fingerprint
        existing.identity = refreshed_identity

        existing.node_name = (
            request.node_name or existing.node_name or request.identity.hostname
        )
        # Agent-provided Cluster metadata is a backward-compatible hint only.
        # Formal membership is administrator-owned and must survive every
        # registration/reconnect unchanged.
        if existing.cluster_id is not None:
            stored_cluster = self._store.get_cluster(existing.cluster_id)
            if stored_cluster is not None:
                existing.cluster = ClusterRef(
                    id=stored_cluster.cluster_id,
                    name=stored_cluster.cluster_name,
                )
        # Registration-provided labels seed a node before its first approval.
        # Once approved, administrator-managed labels must survive Agent
        # reconnects and Backend restarts.
        if request.labels and not existing.ever_approved:
            existing.labels = request.labels
        existing.last_registered_at = now
        existing.last_heartbeat_at = now

        if registration_result == RegistrationResultStatus.EXISTING_DEVICE:
            self._event(
                existing,
                "DEVICE_IDENTITY_RESTORED",
                "INFO",
                "检测到历史设备身份，已恢复原节点ID",
                now,
            )

        if (
            existing.ever_approved
            and existing.authorization_status == AuthorizationStatus.APPROVED
        ):
            # A stable Node UUID proves this is the previously authorized node.
            # Registration refreshes identity/runtime only; it never overrides an
            # administrator's pool removal decision.
            existing.registration_status = RegistrationStatus.REGISTERED
            if existing.runtime_status in {
                RuntimeStatus.OFFLINE,
                RuntimeStatus.SUSPECT,
                RuntimeStatus.RECONNECTING,
            }:
                existing.runtime_status = RuntimeStatus.RECONNECTING
                existing.schedulable = False
                self._event(
                    existing,
                    "RECONNECT_STARTED",
                    "INFO",
                    "节点已重新注册，开始恢复状态同步",
                    now,
                )
            else:
                existing.runtime_status = RuntimeStatus.ONLINE

        return self._registration_response(
            self._save(existing), registration_result
        )

    @staticmethod
    def _registration_response(
        node: NodeRecord, result: RegistrationResultStatus
    ) -> NodeRegistrationResponse:
        return NodeRegistrationResponse.model_validate(
            {
                **node.model_dump(),
                "status": result,
                "canonical_node_id": node.node_id,
            }
        )

    @atomic_operation
    def heartbeat(self, node_id: UUID, request: HeartbeatRequest) -> NodeRecord:
        node = self._require_node(node_id)
        now = utc_now()
        previous = node.runtime_status
        node.identity.agent_version = request.agent_version
        node.last_agent_timestamp = request.timestamp
        node.last_heartbeat_at = now

        if previous in {RuntimeStatus.OFFLINE, RuntimeStatus.SUSPECT}:
            node.runtime_status = RuntimeStatus.RECONNECTING
            node.schedulable = False
            self._event(
                node,
                "RECONNECT_STARTED",
                "INFO",
                "心跳恢复，节点进入重新接入状态",
                now,
            )
        elif previous == RuntimeStatus.RECONNECTING:
            node.runtime_status = RuntimeStatus.ONLINE
            self._event(
                node, "NODE_RECOVERED", "INFO", "节点状态同步完成并恢复在线", now
            )
        else:
            node.runtime_status = RuntimeStatus.ONLINE

        saved = self._save(node)
        self._store.add_heartbeat(
            NodeHeartbeatRecord(
                node_id=node.node_id,
                received_at=now,
                agent_timestamp=request.timestamp,
                agent_version=request.agent_version,
            )
        )
        return saved

    @atomic_operation
    def report_resources(
        self, node_id: UUID, report: ResourceReportRequest
    ) -> NodeRecord:
        node = self._require_node(node_id)
        node.resources = report
        node.last_resource_at = utc_now()
        return self._save(node)

    def list_nodes(self, include_removed: bool = True) -> list[NodeRecord]:
        return sorted(
            (
                node
                for node in self._store.list_nodes()
                if node.ever_approved
                and (
                    include_removed
                    or node.pool_membership == PoolMembership.IN_POOL
                )
            ),
            key=lambda node: (node.node_name.casefold(), str(node.node_id)),
        )

    def list_pending(self) -> list[NodeRecord]:
        pending = {
            RegistrationStatus.DISCOVERED,
            RegistrationStatus.PENDING_APPROVAL,
            RegistrationStatus.REGISTERING,
        }
        return sorted(
            (
                node
                for node in self._store.list_nodes()
                if node.registration_status in pending
            ),
            key=lambda node: node.discovered_at,
        )

    def get_node(self, node_id: UUID) -> NodeRecord:
        return self._require_node(node_id)

    @atomic_operation
    def request_reconnect(self, node_id: UUID) -> NodeReconnectResponse:
        node = self._require_authorized(node_id)
        now = utc_now()
        self._event(
            node,
            "RECONNECT_GUIDANCE_REQUESTED",
            "INFO",
            "管理员已查看节点服务恢复指引；平台未远程修改节点状态",
            now,
        )
        return NodeReconnectResponse(
            node_id=node.node_id,
            node_name=node.node_name,
            current_status=node.operational_status,
            registration_status=node.registration_status,
            pool_membership=node.pool_membership,
            schedulable=node.schedulable,
            requested_at=now,
            guidance=[
                "请在节点设备上确认节点服务正在运行；管理平台无法远程启动已停止的节点服务。",
                f"保留持久化 Node ID {node.node_id}，不要删除或重新生成节点状态文件。",
                "确认节点能够访问管理平台配置的 Backend 地址和端口，然后重启节点服务。",
                "等待心跳和资源上报恢复；平台会识别历史授权，不会要求再次审批。",
            ],
        )

    @atomic_operation
    def update_labels(self, node_id: UUID, labels: list[str]) -> NodeRecord:
        node = self._require_node(node_id)
        if node.labels == labels:
            return node

        node.labels = list(labels)
        self._event(
            node,
            "NODE_LABELS_UPDATED",
            "INFO",
            "节点标签已更新",
            utc_now(),
        )
        return self._save(node)

    @atomic_operation
    def approve(self, node_id: UUID) -> NodeRecord:
        node = self._require_node(node_id)
        if node.registration_status not in {
            RegistrationStatus.DISCOVERED,
            RegistrationStatus.PENDING_APPROVAL,
            RegistrationStatus.REGISTERING,
        }:
            raise InvalidNodeTransitionError(
                f"cannot approve node in {node.registration_status.value} status"
            )

        now = utc_now()
        node.registration_status = RegistrationStatus.REGISTERING
        self._event(node, "REGISTERING", "INFO", "节点开始完成接入注册", now)
        node.registration_status = RegistrationStatus.REGISTERED
        node.ever_approved = True
        node.authorization_status = AuthorizationStatus.APPROVED
        node.pool_membership = PoolMembership.IN_POOL
        node.registered_at = now
        node.scheduling_enabled = True
        node.removed_at = None
        node.removed_reason = None
        self._finish_latest_registration(node, success=True, now=now)
        self._event(node, "REGISTERED", "INFO", "节点已批准接入", now)
        return self._save(node)

    @atomic_operation
    def reject(self, node_id: UUID, reason: str | None = None) -> NodeRecord:
        node = self._require_node(node_id)
        if node.registration_status not in {
            RegistrationStatus.DISCOVERED,
            RegistrationStatus.PENDING_APPROVAL,
            RegistrationStatus.REGISTERING,
        }:
            raise InvalidNodeTransitionError(
                f"cannot reject node in {node.registration_status.value} status"
            )

        now = utc_now()
        node.registration_status = RegistrationStatus.REJECTED
        node.authorization_status = AuthorizationStatus.REVOKED
        node.pool_membership = PoolMembership.REMOVED
        node.scheduling_enabled = False
        node.schedulable = False
        failure_reason = reason or "管理员拒绝节点接入"
        self._finish_latest_registration(
            node,
            success=False,
            now=now,
            failure_reason=failure_reason,
        )
        self._event(node, "REGISTRATION_REJECTED", "WARNING", failure_reason, now)
        return self._save(node)

    @atomic_operation
    def disable_scheduling(
        self, node_id: UUID, reason: str | None = None
    ) -> NodeRecord:
        node = self._require_registered(node_id)
        node.scheduling_enabled = False
        node.schedulable = False
        self._event(
            node,
            "SCHEDULING_DISABLED",
            "WARNING",
            reason or "管理员已暂停节点调度",
            utc_now(),
        )
        return self._save(node)

    @atomic_operation
    def enable_scheduling(self, node_id: UUID, reason: str | None = None) -> NodeRecord:
        node = self._require_registered(node_id)
        node.scheduling_enabled = True
        saved = self._save(node)
        self._event(
            saved,
            "SCHEDULING_ENABLED",
            "INFO",
            reason or "管理员已恢复节点调度资格检查",
            utc_now(),
        )
        return self._save(saved)

    @atomic_operation
    def remove(self, node_id: UUID, reason: str | None = None) -> NodeRecord:
        node = self._require_authorized(node_id)
        if node.pool_membership == PoolMembership.REMOVED:
            return node
        now = utc_now()
        node.pool_membership = PoolMembership.REMOVED
        node.scheduling_enabled = False
        node.schedulable = False
        node.removed_at = now
        node.removed_reason = reason or "管理员将节点移出资源池"
        self._event(node, "NODE_REMOVED", "WARNING", node.removed_reason, now)
        return self._save(node)

    @atomic_operation
    def delete(self, node_id: UUID) -> None:
        """Permanently remove one node and all node-owned history."""

        self._require_node(node_id)
        if not self._store.delete_node(node_id, audit_at=utc_now()):
            raise NodeNotFoundError(f"node {node_id} was not found")

    @atomic_operation
    def join_pool(self, node_id: UUID) -> NodeRecord:
        node = self._require_authorized(node_id)
        if node.pool_membership == PoolMembership.IN_POOL:
            return node

        now = utc_now()
        node.registration_status = RegistrationStatus.REGISTERED
        node.pool_membership = PoolMembership.IN_POOL
        node.scheduling_enabled = True
        node.removed_at = None
        node.removed_reason = None
        self._event(
            node,
            "NODE_JOINED_POOL",
            "INFO",
            "历史授权节点已重新加入资源池",
            now,
        )
        return self._save(node)

    @atomic_operation
    def retry_registration(self, node_id: UUID) -> NodeRecord:
        node = self._require_node(node_id)
        if node.registration_status not in {
            RegistrationStatus.REJECTED,
            RegistrationStatus.FAILED,
        }:
            raise InvalidNodeTransitionError(
                f"cannot retry node in {node.registration_status.value} status"
            )
        now = utc_now()
        node.registration_status = RegistrationStatus.PENDING_APPROVAL
        node.ever_approved = False
        node.authorization_status = AuthorizationStatus.PENDING
        node.pool_membership = PoolMembership.REMOVED
        node.scheduling_enabled = False
        node.schedulable = False
        node.removed_at = None
        node.removed_reason = None
        self._store.add_registration(
            NodeRegistrationRecord(
                node_id=node.node_id,
                node_name=node.node_name,
                started_at=now,
                result="IN_PROGRESS",
                reconnect_status="PENDING",
            )
        )
        self._event(node, "REGISTRATION_RETRY", "INFO", "节点已重新进入待批准队列", now)
        return self._save(node)

    async def manual_connect(self, request: ManualConnectRequest) -> NodeRecord:
        base_url = await self._validated_agent_base_url(request.agent_address)
        identity_url = urljoin(f"{base_url.rstrip('/')}/", "api/v1/identity")
        try:
            async with httpx.AsyncClient(
                timeout=self._settings.manual_connect_timeout_seconds,
                follow_redirects=False,
                trust_env=False,
            ) as client:
                response = await client.get(identity_url)
                response.raise_for_status()
                body = response.json()
        except (httpx.HTTPError, ValueError) as exc:
            raise ManualConnectError(f"无法验证节点接入服务：{exc}") from exc

        payload = body.get("identity", body) if isinstance(body, dict) else body
        try:
            identity = IdentityPayload.model_validate(payload)
        except ValidationError as exc:
            raise ManualConnectError("节点身份响应不符合接口契约") from exc

        return self.register(
            RegisterRequest(
                identity=identity,
                node_name=request.node_name,
                cluster=request.cluster,
                labels=request.labels,
            )
        )

    def list_events(self, node_id: UUID | None = None) -> list[NodeEvent]:
        return self._store.list_events(node_id)

    def list_registrations(
        self, node_id: UUID | None = None
    ) -> list[NodeRegistrationRecord]:
        return self._store.list_registrations(node_id)

    def list_heartbeats(self, node_id: UUID) -> list[NodeHeartbeatRecord]:
        self._require_node(node_id)
        return self._store.list_heartbeats(node_id)

    @atomic_operation
    def evaluate_heartbeat_timeouts(
        self, now: datetime | None = None
    ) -> list[NodeRecord]:
        evaluated_at = now or utc_now()
        changed: list[NodeRecord] = []

        for node in self._store.list_nodes():
            if node.registration_status in {
                RegistrationStatus.REJECTED,
            }:
                continue
            if node.last_heartbeat_at is None:
                continue
            age = (evaluated_at - node.last_heartbeat_at).total_seconds()
            target: RuntimeStatus | None = None
            if age >= self._settings.offline_after_seconds:
                target = RuntimeStatus.OFFLINE
            elif age >= self._settings.suspect_after_seconds:
                target = RuntimeStatus.SUSPECT

            runtime_changed = target is not None and node.runtime_status != target
            if runtime_changed:
                node.runtime_status = target
                node.schedulable = False
                if target == RuntimeStatus.OFFLINE:
                    self._event(
                        node,
                        "NODE_OFFLINE",
                        "ERROR",
                        "节点心跳超时，已转为离线",
                        evaluated_at,
                    )
                else:
                    self._event(
                        node,
                        "NODE_SUSPECT",
                        "WARNING",
                        "节点心跳延迟，已转为疑似离线",
                        evaluated_at,
                    )

            next_operational = self._compute_operational_status(node, evaluated_at)
            operational_changed = node.operational_status != next_operational
            if not runtime_changed and not operational_changed:
                continue
            changed.append(self._save(node, evaluated_at))

        return changed

    def _require_node(self, node_id: UUID) -> NodeRecord:
        node = self._store.get_node(node_id)
        if node is None:
            raise NodeNotFoundError(f"node {node_id} was not found")
        return node

    def _require_registered(self, node_id: UUID) -> NodeRecord:
        node = self._require_node(node_id)
        if not (
            node.registration_status == RegistrationStatus.REGISTERED
            and node.ever_approved
            and node.authorization_status == AuthorizationStatus.APPROVED
            and node.pool_membership == PoolMembership.IN_POOL
        ):
            raise InvalidNodeTransitionError(
                "only authorized resource-pool members support scheduling actions"
            )
        return node

    def _require_authorized(self, node_id: UUID) -> NodeRecord:
        node = self._require_node(node_id)
        if not (
            node.ever_approved
            and node.authorization_status == AuthorizationStatus.APPROVED
        ):
            raise InvalidNodeTransitionError(
                "node does not have an active historical authorization"
            )
        return node

    def _save(
        self, node: NodeRecord, evaluated_at: datetime | None = None
    ) -> NodeRecord:
        previous_operational = node.operational_status
        occurred_at = evaluated_at or utc_now()
        node.operational_status = self._compute_operational_status(
            node, occurred_at
        )
        node.schedulable = self._compute_schedulable(node)
        saved = self._store.save_node(node)
        if (
            previous_operational != saved.operational_status
            and saved.operational_status == NodeOperationalStatus.ABNORMAL
        ):
            self._event(
                saved,
                "NODE_ABNORMAL",
                "ERROR",
                "节点仍可通信，但资源或运行状态异常",
                occurred_at,
            )
        elif (
            previous_operational == NodeOperationalStatus.ABNORMAL
            and saved.operational_status == NodeOperationalStatus.ONLINE
        ):
            self._event(
                saved,
                "NODE_RECOVERED",
                "INFO",
                "节点资源与运行状态已恢复正常",
                occurred_at,
            )
        return saved

    def _compute_operational_status(
        self, node: NodeRecord, evaluated_at: datetime
    ) -> NodeOperationalStatus:
        if node.runtime_status == RuntimeStatus.OFFLINE:
            return NodeOperationalStatus.OFFLINE
        if node.runtime_status in {
            RuntimeStatus.SUSPECT,
            RuntimeStatus.RECONNECTING,
        }:
            return NodeOperationalStatus.SUSPECT
        if node.health_status != HealthStatus.HEALTHY:
            return NodeOperationalStatus.ABNORMAL

        report = node.resources
        if node.registration_status == RegistrationStatus.REGISTERED and report is None:
            return NodeOperationalStatus.ABNORMAL
        if resource_sample_is_stale(node, evaluated_at, self._settings.resource_stale_after_seconds):
            # last_resource_at is the receive time; an old sample delivered late is still stale.
            return NodeOperationalStatus.ABNORMAL
        if report is not None and (
            report.network.link_status != LinkStatus.UP
            or report.gpu.error is not None
            or report.npu.error is not None
            or report.dla.error is not None
        ):
            return NodeOperationalStatus.ABNORMAL
        return NodeOperationalStatus.ONLINE

    @staticmethod
    def _compute_schedulable(node: NodeRecord) -> bool:
        if not (
            node.registration_status == RegistrationStatus.REGISTERED
            and node.ever_approved
            and node.authorization_status == AuthorizationStatus.APPROVED
            and node.pool_membership == PoolMembership.IN_POOL
            and node.scheduling_enabled
            and node.runtime_status == RuntimeStatus.ONLINE
            and node.operational_status == NodeOperationalStatus.ONLINE
            and node.health_status == HealthStatus.HEALTHY
            and node.resources is not None
            and node.resources.cpu.logical_cores > 0
        ):
            return False
        return node.resources.network.link_status != LinkStatus.DOWN

    def _event(
        self,
        node: NodeRecord,
        event_type: str,
        level: str,
        message: str,
        occurred_at: datetime,
    ) -> None:
        event = NodeEvent(
                node_id=node.node_id,
                node_name=node.node_name,
                event_type=event_type,
                level=level,
                message=message,
                occurred_at=occurred_at,
        )
        cluster_event = None
        if node.cluster_id and event_type in {"NODE_OFFLINE", "RECONNECT_STARTED", "NODE_RECOVERED"}:
            cluster_event = ClusterEvent(
                id=event.id, cluster_id=node.cluster_id, category="recovery",
                event_type=event_type, result="observed", target_id=str(node.node_id),
                target_name=node.node_name, occurred_at=occurred_at, reason=message,
            )
        self._store.add_event(event, cluster_event=cluster_event)

    def _finish_latest_registration(
        self,
        node: NodeRecord,
        *,
        success: bool,
        now: datetime,
        failure_reason: str | None = None,
    ) -> None:
        records = self._store.list_registrations(node.node_id)
        current = next(
            (record for record in records if record.result == "IN_PROGRESS"), None
        )
        if current is None:
            current = NodeRegistrationRecord(
                node_id=node.node_id,
                node_name=node.node_name,
                started_at=node.discovered_at,
                result="IN_PROGRESS",
            )
        current.completed_at = now
        current.duration_seconds = max(0, (now - current.started_at).total_seconds())
        current.result = "SUCCESS" if success else "FAILED"
        current.failure_reason = failure_reason
        self._store.save_registration(current)

    async def _validated_agent_base_url(self, address: str) -> str:
        candidate = address.strip()
        if "://" not in candidate:
            candidate = f"http://{candidate}"
        parsed = urlparse(candidate)
        if parsed.scheme not in {"http", "https"}:
            raise ManualConnectError("节点接入地址只支持 http 或 https")
        if not parsed.hostname or parsed.username or parsed.password:
            raise ManualConnectError("节点接入地址格式无效")

        try:
            port = parsed.port or (443 if parsed.scheme == "https" else 80)
        except ValueError as exc:
            raise ManualConnectError("节点接入地址端口无效") from exc
        try:
            addresses = await asyncio.to_thread(
                socket.getaddrinfo,
                parsed.hostname,
                port,
                type=socket.SOCK_STREAM,
            )
        except socket.gaierror as exc:
            raise ManualConnectError("无法解析节点接入地址") from exc

        resolved = {item[4][0] for item in addresses}
        if not resolved:
            raise ManualConnectError("节点接入地址没有可用 IP")
        for value in resolved:
            ip = ipaddress.ip_address(value)
            if not (ip.is_private or ip.is_loopback or ip.is_link_local):
                raise ManualConnectError("手动接入仅允许访问局域网或本机节点服务")
        return candidate.rstrip("/")
