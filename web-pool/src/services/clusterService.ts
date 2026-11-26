import { isAxiosError } from 'axios';
import type {
  ClusterInput,
  ClusterListResponse,
  ClusterRecord,
  ClusterStatus,
} from '../types/cluster';
import type { NodeDetail } from '../types/node';
import { apiClient } from './apiClient';
import type {
  ApiClusterListResponse,
  ApiClusterNodeListResponse,
  ApiClusterRecord,
} from './clusterApiTypes';
import type { ApiComputePoolResourceMetric } from './computePoolApiTypes';
import { API_ENDPOINTS } from './endpoints';
import { mapApiNodeDetail } from './nodeMapper';
import { getNodeDataMode, getNodeInventoryDetails } from './nodeService';
import type { ApiNodeListResponse, ApiNodeRecord } from './nodeApiTypes';

function mapResourceMetric(metric: ApiComputePoolResourceMetric) {
  return {
    total: metric.total,
    used: metric.used ?? metric.allocated,
    allocated: metric.allocated,
    available: metric.available,
    schedulableAvailable: metric.schedulable_available,
    unavailable: metric.unavailable,
    utilizationPercent: metric.utilization_percent,
    runtimeUtilizationPercent: metric.runtime_utilization_percent,
    unit: metric.unit,
    collectionStatus: metric.collection_status,
    issueNodeCount: metric.issue_node_count,
  };
}

function mapCluster(cluster: ApiClusterRecord): ClusterRecord {
  return {
    id: cluster.cluster_id,
    name: cluster.cluster_name,
    version: cluster.version,
    status: cluster.status.toLowerCase() as ClusterStatus,
    enterprise: cluster.enterprise,
    project: cluster.project,
    ...(cluster.management_endpoint ? { managementEndpoint: cluster.management_endpoint } : {}),
    ...(cluster.last_sync_time ? { lastSyncTime: cluster.last_sync_time } : {}),
    createdAt: cluster.created_at,
    updatedAt: cluster.updated_at,
    nodeCount: cluster.node_count,
    resourceSummary: {
      resources: {
        cpu: mapResourceMetric(cluster.resource_summary.resources.cpu),
        gpu: mapResourceMetric(cluster.resource_summary.resources.gpu),
        npu: mapResourceMetric(cluster.resource_summary.resources.npu),
        dla: mapResourceMetric(cluster.resource_summary.resources.dla),
      },
      videoMemory: mapResourceMetric(cluster.resource_summary.gpu_accessible_memory ?? cluster.resource_summary.video_memory),
      memory: mapResourceMetric(cluster.resource_summary.memory),
      storage: mapResourceMetric(cluster.resource_summary.storage),
      updatedAt: cluster.resource_summary.updated_at,
    },
  };
}

function toUpdatePayload(input: ClusterInput) {
  return {
    cluster_name: input.name.trim(),
    version: input.version.trim(),
    enterprise: input.enterprise.trim(),
    project: input.project.trim(),
    management_endpoint: input.managementEndpoint?.trim() || null,
  };
}

function toCreatePayload(input: ClusterInput) {
  return {
    ...toUpdatePayload(input),
    node_ids: input.nodeIds ?? [],
  };
}

export async function getClusters(): Promise<ClusterListResponse> {
  if (getNodeDataMode() === 'mock') {
    const { createComputePoolMock } = await import('../mocks/computePoolMock');
    const pool = createComputePoolMock();
    const updatedAt = pool.updatedAt;
    const zeroVideoMemory = {
      total: 0,
      used: 0,
      allocated: 0,
      available: 0,
      schedulableAvailable: 0,
      unavailable: 0,
      utilizationPercent: 0,
      runtimeUtilizationPercent: 0,
      unit: 'GB' as const,
      collectionStatus: 'complete' as const,
      issueNodeCount: 0,
    };
    const items: ClusterRecord[] = pool.clusters.map((cluster) => ({
      id: cluster.cluster.id,
      name: cluster.cluster.name,
      version: 'mock',
      status: cluster.schedulableNodeCount > 0 ? 'online' : 'offline',
      enterprise: '演示环境',
      project: 'Simulator',
      createdAt: updatedAt,
      updatedAt,
      nodeCount: cluster.nodeCount,
      resourceSummary: {
        resources: cluster.resources,
        videoMemory: cluster.supportingResources.videoMemory ?? zeroVideoMemory,
        memory: cluster.supportingResources.memory,
        storage: cluster.supportingResources.storage,
        updatedAt,
      },
    }));
    return { items, total: items.length, updatedAt };
  }
  const response = await apiClient.get<ApiClusterListResponse>(API_ENDPOINTS.clusters);
  return {
    items: response.data.items.map(mapCluster),
    total: response.data.total,
    updatedAt: response.data.updated_at,
  };
}

export async function getCluster(clusterId: string, signal?: AbortSignal): Promise<ClusterRecord> {
  if (getNodeDataMode() === 'mock') {
    const cluster = (await getClusters()).items.find((item) => item.id === clusterId);
    if (!cluster) throw new Error(`cluster ${clusterId} was not found`);
    return cluster;
  }
  const response = await apiClient.get<ApiClusterRecord>(API_ENDPOINTS.clusterDetail(clusterId), { signal });
  return mapCluster(response.data);
}

export async function createCluster(input: ClusterInput): Promise<ClusterRecord> {
  if (getNodeDataMode() === 'mock') throw new Error('Mock 模式不支持集群变更。');
  const response = await apiClient.post<ApiClusterRecord>(
    API_ENDPOINTS.clusters,
    toCreatePayload(input),
  );
  return mapCluster(response.data);
}

export async function updateCluster(
  clusterId: string,
  input: ClusterInput,
): Promise<ClusterRecord> {
  if (getNodeDataMode() === 'mock') throw new Error('Mock 模式不支持集群变更。');
  const response = await apiClient.patch<ApiClusterRecord>(
    API_ENDPOINTS.clusterDetail(clusterId),
    toUpdatePayload(input),
  );
  return mapCluster(response.data);
}

/**
 * Return only Backend-owned Nodes that may be newly assigned to a Cluster.
 * This intentionally bypasses simulator/mixed providers: membership mutations
 * can only reference Nodes that exist in the real Backend database.
 */
export async function getEligibleClusterNodes(): Promise<NodeDetail[]> {
  if (getNodeDataMode() === 'mock') return [];
  const response = await apiClient.get<ApiNodeListResponse>(API_ENDPOINTS.nodes, {
    params: { include_removed: false },
  });
  return response.data.items
    .map(mapApiNodeDetail)
    .filter(
      (node) =>
        node.registrationStatus === 'registered' &&
        node.authorizationStatus === 'approved' &&
        node.everApproved &&
        node.poolMembership === 'in_pool' &&
        node.runtimeStatus === 'online' &&
        node.clusterId == null,
    )
    .sort((left, right) => left.name.localeCompare(right.name, 'zh-CN'));
}

export async function deleteCluster(clusterId: string): Promise<void> {
  if (getNodeDataMode() === 'mock') throw new Error('Mock 模式不支持集群变更。');
  await apiClient.delete(API_ENDPOINTS.clusterDetail(clusterId));
}

export async function getClusterNodes(clusterId: string, signal?: AbortSignal): Promise<NodeDetail[]> {
  if (getNodeDataMode() === 'mock') {
    const inventory = await getNodeInventoryDetails();
    return inventory.items.filter((node) => node.cluster?.id === clusterId);
  }
  const response = await apiClient.get<ApiClusterNodeListResponse>(
    API_ENDPOINTS.clusterNodes(clusterId), { signal },
  );
  return response.data.items.map(mapApiNodeDetail);
}

export async function assignNodeToCluster(clusterId: string, nodeId: string): Promise<NodeDetail> {
  if (getNodeDataMode() === 'mock') throw new Error('Mock 模式不支持集群变更。');
  const response = await apiClient.post<ApiNodeRecord>(
    API_ENDPOINTS.clusterNode(clusterId, nodeId),
  );
  return mapApiNodeDetail(response.data);
}

export async function unassignNodeFromCluster(
  clusterId: string,
  nodeId: string,
): Promise<NodeDetail> {
  if (getNodeDataMode() === 'mock') throw new Error('Mock 模式不支持集群变更。');
  const response = await apiClient.delete<ApiNodeRecord>(
    API_ENDPOINTS.clusterNode(clusterId, nodeId),
  );
  return mapApiNodeDetail(response.data);
}

export async function moveNodeToCluster(clusterId: string, nodeId: string): Promise<NodeDetail> {
  if (getNodeDataMode() === 'mock') throw new Error('Mock 模式不支持集群变更。');
  const response = await apiClient.post<ApiNodeRecord>(
    API_ENDPOINTS.clusterNodeMove(clusterId, nodeId),
  );
  return mapApiNodeDetail(response.data);
}

export interface ClusterOperations {
  cluster_id: string;
  updated_at: string;
  tasks: {
    coverage: 'available' | 'partial' | 'unavailable';
    window_hours: number;
    window_start: string;
    running_tasks: number | null;
    waiting_tasks: number | null;
    succeeded_tasks: number | null;
    failed_tasks: number | null;
    executing_nodes: number | null;
    executing_members?: number | null;
    total_members?: number;
    waiting_shards: number | null;
  };
  load: {
    status: 'live' | 'stale' | 'unavailable';
    sampled_at: string | null;
    reporting_nodes: number;
    stale_nodes: number;
    unified_memory_gpu_nodes: number;
    telemetry?: Partial<Record<'cpu' | 'gpu' | 'memory' | 'video_memory' | 'npu' | 'dla',
      'unified' | 'unsupported' | 'unreported' | 'unavailable'>>;
    values: Record<'cpu' | 'gpu' | 'memory' | 'video_memory' | 'npu' | 'dla', number | null>;
  };
  recovery?: {
    nodes: {
      online_members: number;
      total_members: number;
      losses: number;
      reconnections: number;
      retained_memberships: number;
      scheduling_restored: number | null;
      mean_recovery_seconds: number | null;
      last_loss_at: string | null;
      last_recovered_at: string | null;
    };
    tasks: {
      affected_tasks: number;
      affected_shards: number;
      waiting_shards: number;
      redispatched_shards: number;
      pending_reconciliation_shards: number;
      confirmed_results: number;
      last_reconciled_at: string | null;
      last_redispatch: { at: string; attempt_id: string; status: string; task_id: string; segment_id: string } | null;
    };
    members: Record<string, {
      scheduling_status: 'eligible' | 'paused' | 'ineligible' | 'recovering' | 'unknown';
      current_shards: number | null;
      last_recovered_at: string | null;
    }>;
  };
}

export type ClusterEventCategory = 'lifecycle' | 'sync' | 'recovery';

export interface ClusterEvent {
  id: string;
  cluster_id: string;
  category: ClusterEventCategory;
  event_type: string;
  target_id: string;
  target_name: string;
  occurred_at: string;
  result: 'success' | 'rejected' | 'observed';
  before: Record<string, string | null>;
  after: Record<string, string | null>;
  duration_seconds: number | null;
  reason: string | null;
}

export async function getClusterOperations(clusterId: string, signal: AbortSignal) {
  if (getNodeDataMode() === 'mock') return null;
  const { data } = await apiClient.get<ClusterOperations>(
    API_ENDPOINTS.clusterDetail(clusterId) + '/operations', { signal },
  );
  return data;
}

export async function getClusterEvents(
  clusterId: string, category: ClusterEventCategory | undefined, signal: AbortSignal,
) {
  if (getNodeDataMode() === 'mock') return null;
  const { data } = await apiClient.get<ClusterEvent[]>(
    API_ENDPOINTS.clusterDetail(clusterId) + '/events', { params: { category, limit: 50 }, signal },
  );
  return data;
}

export type ClusterRecoveryEventCategory = 'lifecycle' | 'node_recovery' | 'task_recovery' | 'sync';

export interface ClusterRecoveryEvent extends Omit<ClusterEvent, 'category' | 'result' | 'before' | 'after'> {
  category: ClusterRecoveryEventCategory;
  status: string;
  before?: ClusterEvent['before'];
  after?: ClusterEvent['after'];
  node_id?: string;
  task_id?: string;
  segment_id?: string;
  attempt_id?: string;
}

export async function getClusterRecoveryEvents(
  clusterId: string, category: ClusterRecoveryEventCategory | undefined, signal: AbortSignal,
) {
  if (getNodeDataMode() === 'mock') return null;
  try {
    const { data } = await apiClient.get<ClusterRecoveryEvent[]>(
      API_ENDPOINTS.clusterDetail(clusterId) + '/recovery-events', { params: { category, limit: 50 }, signal },
    );
    return data;
  } catch (error) {
    if (!isAxiosError(error) || error.response?.status !== 404) throw error;
    if (category === 'task_recovery') return [];
    const events = await getClusterEvents(clusterId, category === 'node_recovery' ? 'recovery' : category, signal);
    const observed: Record<string, string> = {
      NODE_OFFLINE: '已离线', RECONNECT_STARTED: '恢复中', NODE_RECOVERED: '已恢复',
    };
    return events?.map<ClusterRecoveryEvent>((event) => ({
      ...event,
      category: event.category === 'recovery' ? 'node_recovery' : event.category,
      status: event.result === 'success' ? '操作成功' : event.result === 'rejected' ? '操作失败'
        : observed[event.event_type] ?? '已记录',
      ...(event.category === 'recovery' ? { node_id: event.target_id } : {}),
    })) ?? null;
  }
}
