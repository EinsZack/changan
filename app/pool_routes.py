"""Platform routes for the resource-pool pages, backed by the task database."""
from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone
from typing import Literal
from uuid import UUID

from fastapi import APIRouter, HTTPException, Query, Request

from . import pool_bridge
from .cluster_operations import TASK_WINDOW_HOURS, cluster_task_statistics
from .cluster_recovery import cluster_recovery
from .pool.services.cluster_service import ClusterNotFoundError
from .pool.services.resource_aggregation import cluster_runtime_load
from .pool_bridge import admission_mode

router = APIRouter(tags=['resource-pool-platform'])


def bridge(request: Request) -> pool_bridge.PoolBridge:
    value = getattr(request.app.state, 'pool_bridge', None)
    if value is None:
        raise HTTPException(503, '资源池未启用')
    return value


def utcnow():
    return datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z')


@router.get('/api/v1/clusters/{cluster_id}/operations')
def cluster_operations(request: Request, cluster_id: str):
    b = bridge(request)
    try:
        members = b.cluster_service.list_nodes(cluster_id)
    except ClusterNotFoundError as exc:
        raise HTTPException(404, '集群不存在') from exc
    now = datetime.now(timezone.utc)
    tasks = cluster_task_statistics(cluster_id, now)
    window_start = now - timedelta(hours=TASK_WINDOW_HOURS)
    active = tasks.pop('active_shards_by_node')
    recovery = cluster_recovery(b, cluster_id, now, window_start, members=members, active=active)
    if tasks['coverage'] == 'unavailable':
        for member in recovery['members'].values():
            member['current_shards'] = None
    tasks['executing_members'] = sum(str(node.node_id) in active for node in members) if tasks['executing_nodes'] is not None else None
    tasks['total_members'] = len(members)
    return {
        'cluster_id': cluster_id,
        'tasks': tasks,
        'recovery': recovery,
        'load': cluster_runtime_load(members, b.settings.resource_stale_after_seconds, now),
        'updated_at': now,
    }


@router.get('/api/v1/clusters/{cluster_id}/recovery-events')
def cluster_recovery_events(
    request: Request, cluster_id: str,
    category: Literal['lifecycle', 'node_recovery', 'task_recovery', 'sync'] | None = None,
    limit: int = Query(50, ge=1, le=200),
):
    now = datetime.now(timezone.utc)
    try:
        rows = cluster_recovery(bridge(request), cluster_id, now, now - timedelta(hours=TASK_WINDOW_HOURS))
    except ClusterNotFoundError as exc:
        raise HTTPException(404, '集群不存在') from exc
    return [row for row in rows if category is None or row['category'] == category][:limit]


@router.get('/api/v1/scheduler/executions')
def executions(request: Request, physical_node_id: UUID | None = None, task_id: str | None = None,
               active_only: bool = False, limit: int = Query(300, ge=1, le=2000)):
    b = bridge(request)
    items = pool_bridge.projections(b.execution_binding_map(), active_only=active_only, task_id=task_id,
                                    physical_node_id=str(physical_node_id) if physical_node_id else None,
                                    cluster_id=os.getenv('CHANGAN_CLUSTER_ID', 'changan'), limit=limit)
    return {'items': items, 'total': len(items), 'updated_at': utcnow(), 'source': 'task_database'}


@router.get('/api/v1/platform/bindings')
def bindings(request: Request):
    """Execution node ↔ pool node binding with current admission reasons."""
    b = bridge(request)
    engine = request.app.state.engine
    nodes = b.annotate([dict(n) for n in engine.nodes() if n.get('enabled')])
    rows = []
    for n in nodes:
        s = n.get('snapshot') or {}
        pool = n.get('pool') or {}
        rows.append({'execution_node_id': n['id'], 'kube_node': s.get('kube_node'), 'k3s_ready': bool(s.get('healthy')),
                     'online': n.get('online'), 'enabled': bool(n.get('enabled')), 'gpu_slots': s.get('gpu_slots'),
                     'binding_state': pool.get('state', 'OFF'), 'pool_node_id': pool.get('pool_node_id'),
                     'pool_node_name': pool.get('node_name'), 'cluster_id': pool.get('cluster_id'),
                     'admission_reasons': pool.get('reasons', []), 'active_sessions': n.get('reserved', 0)})
    return {'mode': admission_mode(), 'items': rows, 'updated_at': utcnow()}


@router.get('/api/v1/platform/summary')
def summary(request: Request):
    """Task-level statistics and reliability for the pool dashboard (authoritative: task DB)."""
    b = bridge(request)
    reliability = pool_bridge.node_reliability()
    binding = b.execution_binding_map()
    return {
        'tasks': pool_bridge.task_statistics(),
        'reliability': {k: reliability[k] for k in ('incident_count', 'open_incidents', 'mean_recovery_seconds')},
        'execution_nodes': len(binding), 'bound_nodes': sum(1 for v in binding.values() if v['state'] == 'BOUND'),
        'admission_mode': admission_mode(), 'updated_at': utcnow(),
    }


@router.get('/api/v1/events')
def recent_events(request: Request, limit: int = Query(8, ge=1, le=100)):
    """Recent task lifecycle events for the pool dashboard."""
    bridge(request)
    items = pool_bridge.recent_task_events(limit)
    return {'items': items, 'total': len(items), 'updated_at': utcnow(), 'source': 'task_database'}


@router.get('/api/v1/platform/nodes/{pool_node_id}/reliability')
def reliability(request: Request, pool_node_id: UUID):
    b = bridge(request)
    binding = {k: v for k, v in b.execution_binding_map().items() if v.get('pool_node_id') == str(pool_node_id)}
    if not binding:
        return {'execution_node_id': None, 'binding_state': 'UNBOUND', 'incidents': [], 'attempts': None,
                'incident_count': 0, 'open_incidents': 0, 'mean_recovery_seconds': None}
    exec_id = next(iter(binding))
    data = pool_bridge.node_reliability(exec_id)
    return {'execution_node_id': exec_id, 'binding_state': 'BOUND', 'incidents': data['incidents'],
            'attempts': data['attempts'].get(exec_id), 'incident_count': data['incident_count'],
            'open_incidents': data['open_incidents'], 'mean_recovery_seconds': data['mean_recovery_seconds']}
