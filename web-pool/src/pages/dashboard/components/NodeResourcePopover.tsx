import dayjs from 'dayjs';
import vehicleImage from '../../../assets/fleet-node-suv-v2.png';
import type { NodeResourceContribution, ResourceMetric } from '../../../types/computePool';
import type { NodeDetail, NodeOperationalStatus, NodeSummary } from '../../../types/node';
import { getNodeResourceSnapshot } from '../dashboardModel';

interface NodeResourcePopoverProps {
  node: NodeSummary;
  displayName: string;
  resource?: NodeResourceContribution;
  detail?: NodeDetail;
}

const STATUS_TEXT: Record<NodeOperationalStatus, string> = {
  online: '在线',
  suspect: '可疑',
  offline: '离线',
  abnormal: '异常',
};

interface ResourceRowProps {
  label: string;
  value: string;
  percent?: number;
  unsupported?: boolean;
}

const formatNumber = (value: number) =>
  Number.isInteger(value) ? String(value) : value.toFixed(1);

const formatPercent = (value?: number) => (value === undefined ? '--' : `${formatNumber(value)}%`);

const formatCapacity = (value?: number) =>
  value === undefined ? '--' : `${formatNumber(value)} GB`;

const runtimeUtilization = (metric?: ResourceMetric) =>
  metric?.runtimeUtilizationPercent ?? metric?.utilizationPercent;

const clampPercent = (value?: number) =>
  value === undefined ? 0 : Math.min(100, Math.max(0, value));

function ResourceRow({ label, value, percent, unsupported = false }: ResourceRowProps) {
  return (
    <div className={`node-resource-popover__resource${unsupported ? ' is-unsupported' : ''}`}>
      <div>
        <span>{label}</span>
        <strong>{value}</strong>
      </div>
      <span className="node-resource-popover__meter" aria-hidden="true">
        <i style={{ width: `${clampPercent(percent)}%` }} />
      </span>
      <small>{unsupported ? '不支持' : `利用率 ${formatPercent(percent)}`}</small>
    </div>
  );
}

export function NodeResourcePopover({
  node,
  displayName,
  resource,
  detail,
}: NodeResourcePopoverProps) {
  const snapshot = getNodeResourceSnapshot(node, resource);
  const gpuMemoryMetric =
    snapshot.gpuMemoryArchitecture === 'unified' ? resource?.memory : resource?.videoMemory;
  const detailGpuMemory =
    snapshot.gpuMemoryArchitecture === 'unified' ? detail?.memory : detail?.videoMemory;
  const gpuMemoryTotal = snapshot.gpuMemoryGb ?? detailGpuMemory?.total;
  const gpuMemoryUsed = gpuMemoryMetric?.used ?? detailGpuMemory?.used;
  const gpuMemoryPercent = runtimeUtilization(gpuMemoryMetric) ?? detailGpuMemory?.utilization;
  const memoryTotal = snapshot.memoryTotalGb ?? detail?.memory.total;
  const memoryUsed = resource?.memory.used ?? detail?.memory.used;
  const memoryPercent = snapshot.memoryUtilization ?? detail?.memory.utilization;
  const historical = node.operationalStatus !== 'online';
  const resourceLabel = historical ? '最后一次上报' : '实时资源';
  const reportedAt = detail?.lastResourceAt ?? detail?.agentMetrics?.collectedAt ??
    node.lastResourceAt ?? (historical ? undefined : snapshot.lastReportedAt);

  return (
    <div className="node-resource-popover" aria-label={`${displayName} 数字档案与${resourceLabel}`}>
      <header className="node-resource-popover__header">
        <div>
          <strong>{displayName}</strong>
          <span title={node.name}>{node.name}</span>
        </div>
        <span className="node-resource-popover__status" data-status={node.operationalStatus}>
          <i aria-hidden="true" />
          {STATUS_TEXT[node.operationalStatus]}
        </span>
      </header>

      {historical && (
        <p className="node-resource-popover__history">
          {reportedAt ? <>最后一次上报 · {dayjs(reportedAt).format('YYYY-MM-DD HH:mm:ss')}（历史）</> : '暂无历史资源上报'}
        </p>
      )}

      <section className="node-resource-popover__identity" aria-label="节点身份">
        <img src={vehicleImage} alt="" draggable={false} />
        <dl>
          <div>
            <dt>Node ID</dt>
            <dd title={node.id}>{node.id}</dd>
          </div>
          <div>
            <dt>平台 / 架构</dt>
            <dd>
              {node.platform} / {node.architecture.toUpperCase()}
            </dd>
          </div>
          <div>
            <dt>所属集群</dt>
            <dd>{node.cluster?.name ?? '未加入集群'}</dd>
          </div>
        </dl>
      </section>

      {(!historical || reportedAt) && <section className="node-resource-popover__resources" aria-label={resourceLabel}>
        <ResourceRow
          label="CPU"
          value={`${formatNumber(snapshot.cpuCores)} Core`}
          percent={snapshot.cpuUtilization ?? detail?.resources.cpu.runtimeUtilization}
        />
        <ResourceRow
          label="GPU"
          value={`${formatNumber(snapshot.gpuCount)} GPU`}
          percent={
            snapshot.gpuUtilization ??
            detail?.resources.gpu?.runtimeUtilization
          }
        />
        <ResourceRow
          label={snapshot.gpuMemoryArchitecture === 'unified' ? '显存（统一内存）' : '显存'}
          value={
            gpuMemoryTotal === undefined
              ? '--'
              : `${formatCapacity(gpuMemoryUsed)} / ${formatCapacity(gpuMemoryTotal)}`
          }
          percent={gpuMemoryPercent}
        />
        <ResourceRow
          label="内存"
          value={
            memoryTotal === undefined
              ? '--'
              : `${formatCapacity(memoryUsed)} / ${formatCapacity(memoryTotal)}`
          }
          percent={memoryPercent}
        />
        <ResourceRow
          label="NPU"
          value={snapshot.npuSupported ? `${formatNumber(snapshot.npuTops)} TOPS` : '--'}
          percent={snapshot.npuUtilization ?? detail?.resources.npu?.runtimeUtilization}
          unsupported={!snapshot.npuSupported}
        />
        <ResourceRow
          label="DLA"
          value={snapshot.dlaSupported ? `${formatNumber(snapshot.dlaDevices)} Device` : '--'}
          percent={snapshot.dlaUtilization ?? detail?.resources.dla?.runtimeUtilization}
          unsupported={!snapshot.dlaSupported}
        />
      </section>}

      <footer className="node-resource-popover__access">
        <div>
          <span>IP</span>
          <strong>{node.ipAddress || detail?.network?.ipv4Address || '--'}</strong>
        </div>
        {!historical && <div>
          <span>最近上报</span>
          <strong>{reportedAt ? dayjs(reportedAt).format('MM-DD HH:mm:ss') : '暂无资源上报'}</strong>
        </div>}
      </footer>
    </div>
  );
}
