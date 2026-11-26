import { useCallback, useEffect, useState } from 'react';
import type { ReactNode } from 'react';
import { AuditOutlined, DashboardOutlined, RightOutlined, SyncOutlined } from '@ant-design/icons';
import { Progress, Segmented, Table, Tooltip } from 'antd';
import type { ColumnsType } from 'antd/es/table';
import dayjs from 'dayjs';
import { Link } from 'react-router-dom';
import { buildNodeDetailPath, consolePath } from '../../../routes/config';
import { getClusterOperations, getClusterRecoveryEvents } from '../../../services/clusterService';
import type { ClusterRecoveryEvent, ClusterRecoveryEventCategory } from '../../../services/clusterService';
import { formatExecutionNodes, resourceLoadText, recoveryStatusTone } from './clusterRecoveryPresentation';

export const formatTime = (timestamp?: string | null) =>
  timestamp ? dayjs(timestamp).format('YYYY-MM-DD HH:mm:ss') : '—';

function duration(seconds?: number | null) {
  if (seconds == null) return '—';
  return seconds < 1 ? Math.round(seconds * 1000) + ' ms' : seconds.toFixed(1) + ' s';
}

function useClusterQuery<T>(read: (signal: AbortSignal) => Promise<T | null>, refreshKey: number) {
  const [data, setData] = useState<T | null>(null);
  const [status, setStatus] = useState<'loading' | 'ready' | 'unavailable' | 'error'>('loading');
  useEffect(() => {
    const controller = new AbortController();
    let timer: ReturnType<typeof setTimeout>;
    async function update() {
      try {
        const value = await read(controller.signal);
        if (!controller.signal.aborted) {
          setData(value);
          setStatus(value === null ? 'unavailable' : 'ready');
        }
      } catch {
        if (!controller.signal.aborted) {
          setData(null);
          setStatus('error');
        }
      } finally {
        if (!controller.signal.aborted) timer = setTimeout(() => void update(), 15_000);
      }
    }
    timer = setTimeout(() => void update(), 0);
    return () => {
      controller.abort();
      clearTimeout(timer);
    };
  }, [read, refreshKey]);
  return { data, status };
}

export function useClusterOperations(clusterId: string, refreshKey: number) {
  const read = useCallback((signal: AbortSignal) => clusterId ? getClusterOperations(clusterId, signal) : Promise.resolve(null), [clusterId]);
  const { data, status } = useClusterQuery(read, refreshKey);
  return { data: data?.cluster_id === clusterId ? data : null, status };
}

function Heading({ icon, children, extra }: { icon: ReactNode; children: ReactNode; extra?: ReactNode }) {
  return (
    <header className="cluster-detail-panel__heading">
      <span className="cluster-detail-panel__heading-icon" aria-hidden="true">{icon}</span>
      <strong>{children}</strong>
      {extra && <span className="cluster-operations__action">{extra}</span>}
    </header>
  );
}

type Metric = readonly [label: string, value: ReactNode, tip?: string];

function Metrics({ items, compact = false }: { items: readonly Metric[]; compact?: boolean }) {
  return (
    <dl className={compact ? 'cluster-operations__state-grid' : 'cluster-operations__task-grid'}>
      {items.map(([label, value, tip]) => (
        <div key={label}>
          <dt>{tip ? <Tooltip title={tip}><span tabIndex={0}>{label}</span></Tooltip> : label}</dt>
          <dd>{value ?? '—'}</dd>
        </div>
      ))}
    </dl>
  );
}

const LOAD_METRICS = [
  ['cpu', 'CPU'], ['gpu', 'GPU'], ['memory', '内存'], ['video_memory', '显存'],
  ['npu', 'NPU'], ['dla', 'DLA'],
] as const;

type Operations = ReturnType<typeof useClusterOperations>;

export function ClusterRuntimePanels({ clusterId, nodeCount, operations }: {
  clusterId: string; nodeCount: number; operations: Operations;
}) {
  const { data, status } = operations;
  const tasks = data?.tasks;
  const load = data?.load;
  const recovery = data?.recovery;
  const nodes = recovery?.nodes;
  const faults = recovery?.tasks;
  const taskPath = consolePath('tasks') + '&' + new URLSearchParams({ cluster_id: clusterId });
  const retry = faults?.last_redispatch;
  const taskScope = tasks?.coverage === 'partial' ? '部分执行记录缺少可靠归属。' : '';
  return (
    <>
      <section className="cluster-detail-panel cluster-operations" aria-label="集群运行态势">
        <Heading icon={<DashboardOutlined />} extra={<Link to={taskPath}>查看任务 <RightOutlined /></Link>}>
          集群运行态势
        </Heading>
        <div className="cluster-operations__grid">
          <article className="cluster-operations__subpanel">
            <div className="cluster-operations__subheading">
              <h3>任务运行统计</h3>
              <Tooltip title={'按派单时的集群归属统计，同一任务只计一次。' + taskScope}>
                <span className="cluster-operations__note" tabIndex={0}>完成及失败：近{tasks?.window_hours ?? 24}小时</span>
              </Tooltip>
            </div>
            <Metrics items={[
              ['运行中任务', tasks?.running_tasks], ['待执行任务', tasks?.waiting_tasks],
              ['已完成任务', tasks?.succeeded_tasks], ['失败 / 超时任务', tasks?.failed_tasks],
              ['执行节点', formatExecutionNodes(tasks?.executing_members ?? (tasks?.total_members === undefined ? tasks?.executing_nodes : null), tasks?.total_members ?? nodeCount),
                '当前有实际执行的集群成员／集群成员总数。'],
              ['待执行分片', tasks?.waiting_shards],
            ]} />
            {status === 'error' && <p className="cluster-operations__note">暂时无法读取集群任务统计</p>}
          </article>
          <article className="cluster-operations__subpanel">
            <h3>实时资源负载</h3>
            <p className="cluster-operations__note">
              {load?.status === 'live'
                ? <>{load.reporting_nodes} 个在线成员有效上报 · {formatTime(load.sampled_at)}{load.stale_nodes > 0 && ' · 已排除过期样本'}</>
                : load?.status === 'stale' ? '数据已过期' : '暂无实时数据'}
            </p>
            <ul className="cluster-operations__loads">
              {LOAD_METRICS.map(([key, label]) => {
                const percent = load?.status === 'live' ? load.values[key] : null;
                const unavailable = resourceLoadText(load?.status, load?.telemetry?.[key],
                  key === 'video_memory' && (load?.unified_memory_gpu_nodes ?? 0) > 0 && !load?.telemetry);
                const metric = key === 'video_memory'
                  ? <Tooltip title="显存利用率由有效独立显存上报计算。统一内存负载已计入内存。"><span tabIndex={0}>{label}</span></Tooltip>
                  : label;
                return (
                  <li key={key}>
                    <span>{metric}</span>
                    {percent != null ? <>
                      <Progress percent={percent} showInfo={false} strokeColor="var(--management-accent)" size="small" />
                      <strong>{percent.toFixed(1)}%</strong>
                    </> : <small>{unavailable}</small>}
                  </li>
                );
              })}
            </ul>
          </article>
        </div>
      </section>
      <section className="cluster-detail-panel cluster-operations" aria-label="集群自治与任务恢复">
        <Heading icon={<SyncOutlined />} extra={<Tooltip title="事件类指标统计近24小时，在线成员、等待恢复和待核对分片反映当前状态。"><span tabIndex={0}>近24小时</span></Tooltip>}>
          集群自治与任务恢复
        </Heading>
        <div className="cluster-operations__grid">
          <article className="cluster-operations__subpanel">
            <h3>节点自治恢复</h3>
            <Metrics items={[
              ['在线成员', formatExecutionNodes(nodes?.online_members, nodes?.total_members ?? nodeCount), '当前在线成员／成员总数。'],
              ['节点失联次数', nodes?.losses],
              ['自动重连接入', nodes?.reconnections, '同一离线恢复过程内的重新注册和心跳恢复合计一次。'],
              ['原集群归属恢复', nodes?.retained_memberships, '以同一节点身份在同一集群中完成重连为依据。'],
              ['恢复调度资格', nodes?.scheduling_restored, '以恢复后通过资源池准入并确认接单的记录为依据。缺少这项证据时显示 —。'],
              ['平均恢复耗时', duration(nodes?.mean_recovery_seconds), '同一恢复过程从离线到恢复在线的耗时。'],
            ]} />
            <Metrics compact items={[
              ['最近失联时间', formatTime(nodes?.last_loss_at)], ['最近恢复时间', formatTime(nodes?.last_recovered_at)],
            ]} />
          </article>
          <article className="cluster-operations__subpanel">
            <h3>任务容错与恢复</h3>
            <Metrics items={[
              ['受影响任务', faults?.affected_tasks], ['受影响分片', faults?.affected_shards],
              ['等待恢复分片', faults?.waiting_shards, '当前仍在等待原执行端恢复的分片。'],
              ['超时重派分片', faults?.redispatched_shards, '因失联超时并已产生新派单的分片，按分片去重。'],
              ['待核对分片', faults?.pending_reconciliation_shards, '当前通信已恢复、在途执行仍待核对的分片。'],
              ['已确认有效结果', faults?.confirmed_results, '以通过现有结果校验的有效尝试为准。'],
            ]} />
            <Metrics compact items={[
              ['最近任务核对时间', formatTime(faults?.last_reconciled_at)],
              ['最近重派状态', retry ? <Link title={formatTime(retry.at)}
                to={consolePath('detail', retry.task_id) + '&' + new URLSearchParams({ cluster_id: clusterId })}>{retry.status}</Link> : '—'],
            ]} />
          </article>
        </div>
      </section>
    </>
  );
}

const EVENT_LABELS: Record<string, string> = {
  REGISTRY_CREATED: '创建集群登记', REGISTRY_UPDATED: '更新登记信息',
  REGISTRY_VERSION_CHANGED: '更新登记版本', REGISTRY_DELETED: '删除集群登记',
  MEMBER_JOINED: '登记成员加入', MEMBER_REMOVED: '登记成员移出',
  MEMBER_MOVED_IN: '登记成员迁入', MEMBER_MOVED_OUT: '登记成员迁出',
  MEMBER_RECORD_DELETED: '成员节点记录删除',
  NODE_OFFLINE: '成员节点失联', RECONNECT_STARTED: '成员节点重新接入', NODE_RECOVERED: '成员节点恢复',
  MEMBERSHIP_RETAINED: '原集群归属确认', SCHEDULING_RESTORED: '恢复调度资格',
  NODE_RUNTIME_SYNC: '节点运行状态同步', NODE_RESOURCE_SYNC: '节点资源状态恢复',
  EXECUTION_LINK_LOST: '执行节点失联', EXECUTION_LINK_RESTORED: '执行节点链路恢复',
  ATTEMPT_TIMED_OUT: '任务分片超时', SHARD_REDISPATCHED: '任务分片重新派发',
  SHARD_RETRY_COMPLETED: '任务分片重试完成',
  EXECUTION_RECONCILED: '任务状态核对', RECOVERY_RESULT_CONFIRMED: '有效结果确认',
};

const FIELD_LABELS: Record<string, string> = {
  name: '名称', cluster_name: '名称', version: '版本', enterprise: '企业', project: '项目',
  management_endpoint: '管理端点', last_sync_time: '登记时间', cluster_id: '归属集群',
};
const describeFields = (fields: Record<string, string | null> = {}) =>
  Object.entries(fields).map(([key, value]) => (FIELD_LABELS[key] ?? key) + ' ' + (value ?? '—')).join(' · ');
const shorten = (id: string) => id.length > 16 ? id.slice(0, 15) + '…' : id;

const EVENT_COLUMNS: ColumnsType<ClusterRecoveryEvent> = [
  { title: '事件时间', dataIndex: 'occurred_at', width: 166, render: formatTime },
  { title: '事件类型', dataIndex: 'event_type', render: (type: string) => EVENT_LABELS[type] ?? type },
  { title: '关联对象', dataIndex: 'target_name', width: 235, render: (name, event) => event.task_id ? (
    <div className="cluster-operations__reference" title={[event.task_id, event.segment_id, event.attempt_id].filter(Boolean).join(' / ')}>
      <Link to={consolePath('detail', event.task_id) + '&' + new URLSearchParams({ cluster_id: event.cluster_id })}>{shorten(event.task_id)}</Link>
      <small>{[event.segment_id, event.attempt_id].filter((id): id is string => !!id).map(shorten).join(' · ')}</small>
    </div>
  ) : event.node_id ? <Link title={event.target_id} to={buildNodeDetailPath(event.node_id)}>{name}</Link>
    : <span title={event.target_id}>{name}</span> },
  { title: '事件状态', dataIndex: 'status', render: (value: string) => (
    <span className="cluster-operations__result" data-result={recoveryStatusTone(value)}>{value}</span>
  ) },
  { title: '耗时', dataIndex: 'duration_seconds', width: 86, render: duration },
  { title: '说明', dataIndex: 'reason', render: (reason, event) => {
    const before = describeFields(event.before);
    const after = describeFields(event.after);
    return before || after ? <Tooltip title={[reason, before && '变更前 ' + before, after && '变更后 ' + after].filter(Boolean).join(' · ')}>
      <span tabIndex={0}>{reason || '查看变更'}</span>
    </Tooltip> : reason ?? '—';
  } },
];

type Filter = 'all' | ClusterRecoveryEventCategory;
const EVENT_FILTERS = [
  { value: 'all', label: '全部记录' }, { value: 'lifecycle', label: '生命周期操作' },
  { value: 'node_recovery', label: '节点自治恢复' }, { value: 'task_recovery', label: '任务容错恢复' },
  { value: 'sync', label: '状态同步' },
] as const;

export function ClusterEventPanel({ clusterId, refreshKey }: { clusterId: string; refreshKey: number }) {
  const [filter, setFilter] = useState<Filter>('all');
  const read = useCallback((signal: AbortSignal) =>
    getClusterRecoveryEvents(clusterId, filter === 'all' ? undefined : filter, signal), [clusterId, filter]);
  const { data, status } = useClusterQuery(read, refreshKey);
  const events = data?.filter((event) => event.cluster_id === clusterId && (filter === 'all' || event.category === filter));
  return (
    <section className="cluster-detail-panel cluster-operations" aria-label="生命周期与异常恢复记录">
      <Heading icon={<AuditOutlined />}>生命周期与异常恢复记录</Heading>
      <div className="cluster-operations__body">
        <div className="cluster-operations__filters">
          <Segmented<Filter> options={[...EVENT_FILTERS]} value={filter} onChange={setFilter} />
          <span className="cluster-operations__note">每类最近 50 条</span>
        </div>
        <Table<ClusterRecoveryEvent>
          className="cluster-member-table" columns={EVENT_COLUMNS} dataSource={events ?? []} rowKey="id"
          size="small" scroll={{ x: 1010 }} pagination={{ pageSize: 8, showSizeChanger: false }}
          loading={status === 'loading'}
          locale={{ emptyText: status === 'error' ? '暂时无法读取事件记录' : filter === 'sync' ? '暂无同步记录' : '暂无事件记录' }}
        />
      </div>
    </section>
  );
}
