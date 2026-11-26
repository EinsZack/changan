import { consolePath } from '../routes/config';
import { apiClient } from './apiClient';

/** Task-level statistics owned by the scheduling platform (same process, task database). */
export interface PlatformTaskStatistics {
  total: number;
  waiting: number;
  running: number;
  succeeded: number;
  failed: number;
  canceled: number;
  completionRate: number | null;
  segmentsTotal: number;
  segmentsSucceeded: number;
  segmentRetries: number;
  definition: string;
}

export interface PlatformSummary {
  tasks: PlatformTaskStatistics;
  reliability: {
    incidentCount: number;
    openIncidents: number;
    meanRecoverySeconds: number | null;
  };
  executionNodes: number;
  boundNodes: number;
  admissionMode: 'off' | 'observe' | 'enforce';
  updatedAt: string;
}

export interface PlatformEvent {
  id: string;
  eventType: string;
  level: string;
  message: string;
  occurredAt: string;
  targetId?: string;
  targetType: 'task';
  taskStatus?: string;
}

interface ApiPlatformEvent {
  id: string;
  event_type: string;
  level: string;
  message: string;
  occurred_at: string;
  target_id?: string | null;
  target_type: 'task';
  task_status?: string | null;
}

export interface ExecutionBinding {
  executionNodeId: string;
  kubeNode?: string;
  k3sReady: boolean;
  online: boolean;
  gpuSlots?: number;
  bindingState: 'BOUND' | 'UNBOUND' | 'CONFLICT' | 'NO_MACHINE_ID' | 'UNAVAILABLE' | 'OFF';
  poolNodeId?: string;
  poolNodeName?: string;
  admissionReasons: string[];
  activeSessions: number;
}

export interface NodeIncident {
  id: string;
  detectedAt: number;
  lastSeenAt?: number;
  linkRestoredAt?: number;
  taskReconciledAt?: number;
  reason: string;
  affectedCount: number;
  recoverySeconds?: number | null;
}

export interface NodeReliability {
  executionNodeId: string | null;
  bindingState: string;
  incidents: NodeIncident[];
  attempts: { total: number; succeeded: number; failed: number } | null;
  incidentCount: number;
  openIncidents: number;
  meanRecoverySeconds: number | null;
}

/* eslint-disable @typescript-eslint/no-explicit-any */
export async function getPlatformSummary(): Promise<PlatformSummary> {
  const { data } = await apiClient.get<any>('/api/v1/platform/summary');
  const t = data.tasks;
  return {
    tasks: {
      total: t.total,
      waiting: t.waiting,
      running: t.running,
      succeeded: t.succeeded,
      failed: t.failed,
      canceled: t.canceled,
      completionRate: t.completion_rate,
      segmentsTotal: t.segments_total,
      segmentsSucceeded: t.segments_succeeded,
      segmentRetries: t.segment_retries,
      definition: t.definition,
    },
    reliability: {
      incidentCount: data.reliability.incident_count,
      openIncidents: data.reliability.open_incidents,
      meanRecoverySeconds: data.reliability.mean_recovery_seconds,
    },
    executionNodes: data.execution_nodes,
    boundNodes: data.bound_nodes,
    admissionMode: data.admission_mode,
    updatedAt: data.updated_at,
  };
}

/** Recent task lifecycle events owned by the scheduling platform. */
export async function getPlatformEvents(limit = 8): Promise<PlatformEvent[]> {
  const { data } = await apiClient.get<{ items: ApiPlatformEvent[] }>('/api/v1/events', {
    params: { limit },
  });
  return data.items.map((item) => ({
    id: item.id,
    eventType: item.event_type,
    level: item.level,
    message: item.message,
    occurredAt: item.occurred_at,
    ...(item.target_id ? { targetId: item.target_id } : {}),
    targetType: item.target_type,
    ...(item.task_status ? { taskStatus: item.task_status } : {}),
  }));
}

export async function getExecutionBindings(): Promise<{ mode: string; items: ExecutionBinding[] }> {
  const { data } = await apiClient.get<any>('/api/v1/platform/bindings');
  return {
    mode: data.mode,
    items: data.items.map((item: any) => ({
      executionNodeId: item.execution_node_id,
      kubeNode: item.kube_node ?? undefined,
      k3sReady: item.k3s_ready,
      online: item.online,
      gpuSlots: item.gpu_slots ?? undefined,
      bindingState: item.binding_state,
      poolNodeId: item.pool_node_id ?? undefined,
      poolNodeName: item.pool_node_name ?? undefined,
      admissionReasons: item.admission_reasons ?? [],
      activeSessions: item.active_sessions ?? 0,
    })),
  };
}

interface NodeShardCounts {
  waiting_shards: number;
  running_shards: number;
  unverified_shards: number;
}

interface NodeTaskReference {
  task_id: string;
  name: string;
}

export interface NodeTaskSummary extends NodeShardCounts {
  node_id: string;
  execution_bound: boolean;
  assigned_task_count: number;
  completed_task_count: number;
  active_task_count: number;
  tasks: (NodeTaskReference & NodeShardCounts)[];
  recent_result: (NodeTaskReference & {
    status: 'SUCCEEDED' | 'FAILED' | 'TIMED_OUT' | 'CANCELED';
    finished_at: string;
  }) | null;
}

export async function getNodeTaskSummary(nodeId: string, signal: AbortSignal): Promise<NodeTaskSummary> {
  const { data } = await apiClient.get<NodeTaskSummary>(
    '/api/v1/platform/nodes/' + encodeURIComponent(nodeId) + '/task-summary',
    { signal },
  );
  return data;
}

export async function getNodeReliability(poolNodeId: string): Promise<NodeReliability> {
  const { data } = await apiClient.get<any>(
    `/api/v1/platform/nodes/${encodeURIComponent(poolNodeId)}/reliability`,
  );
  return {
    executionNodeId: data.execution_node_id,
    bindingState: data.binding_state,
    incidents: (data.incidents ?? []).map((row: any) => ({
      id: row.id,
      detectedAt: row.detected_at,
      lastSeenAt: row.last_seen_at ?? undefined,
      linkRestoredAt: row.link_restored_at ?? undefined,
      taskReconciledAt: row.task_reconciled_at ?? undefined,
      reason: row.reason,
      affectedCount: Array.isArray(row.affected) ? row.affected.length : 0,
      recoverySeconds: row.recovery_seconds,
    })),
    attempts: data.attempts,
    incidentCount: data.incident_count,
    openIncidents: data.open_incidents,
    meanRecoverySeconds: data.mean_recovery_seconds,
  };
}

/** Link back to the task console served at the site root. */
export const TASK_CONSOLE_URL = '/';
export const taskDetailUrl = (taskId: string) => consolePath('detail', taskId);
