import { useCallback, useEffect, useState } from 'react';
import {
  ApartmentOutlined,
  ArrowLeftOutlined,
  CloudServerOutlined,
  ControlOutlined,
  DatabaseOutlined,
  DeleteOutlined,
  DeploymentUnitOutlined,
  EditOutlined,
  HddOutlined,
  PlusOutlined,
  ReloadOutlined,
  SwapOutlined,
  ThunderboltOutlined,
  UserDeleteOutlined,
} from '@ant-design/icons';
import {
  Alert,
  App,
  Button,
  Card,
  Modal,
  Result,
  Select,
  Space,
  Table,
  Tooltip,
} from 'antd';
import type { ColumnsType } from 'antd/es/table';
import dayjs from 'dayjs';
import { Link, useNavigate, useOutletContext, useParams } from 'react-router-dom';
import { StatusBadge } from '../../components/StatusBadge';
import type { MainLayoutOutletContext } from '../../layouts/MainLayout';
import { buildNodeDetailPath, ROUTES } from '../../routes/config';
import {
  assignNodeToCluster,
  deleteCluster,
  getCluster,
  getClusterNodes,
  getClusters,
  getEligibleClusterNodes,
  moveNodeToCluster,
  unassignNodeFromCluster,
  updateCluster,
} from '../../services/clusterService';
import type { ClusterInput, ClusterRecord } from '../../types/cluster';
import type { NodeDetail } from '../../types/node';
import { ClusterFormModal } from './components/ClusterFormModal';
import { ClusterMemberScene } from './components/ClusterMemberScene';
import { ClusterNodeOption } from './components/ClusterNodeOption';
import { ClusterEventPanel, ClusterRuntimePanels, formatTime, useClusterOperations } from './components/ClusterOperationsPanels';
import { sortClusterMembers } from './components/clusterMemberLayout';
import './ClusterPage.css';

const formatNumber = (value: number) =>
  Number.isInteger(value) ? String(value) : value.toFixed(1);

export function ClusterDetailPage() {
  const { clusterId = '' } = useParams();
  const navigate = useNavigate();
  const { message, modal } = App.useApp();
  const { setHeaderUpdatedAt } = useOutletContext<MainLayoutOutletContext>();
  const [cluster, setCluster] = useState<ClusterRecord | null>(null);
  const [members, setMembers] = useState<NodeDetail[]>([]);
  const [candidateNodes, setCandidateNodes] = useState<NodeDetail[]>([]);
  const [clusters, setClusters] = useState<ClusterRecord[]>([]);
  const [loading, setLoading] = useState(true);
  const [actionNodeId, setActionNodeId] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [editing, setEditing] = useState(false);
  const [saving, setSaving] = useState(false);
  const [assignOpen, setAssignOpen] = useState(false);
  const [selectedNodeId, setSelectedNodeId] = useState<string>();
  const [deleting, setDeleting] = useState(false);
  const [refreshKey, setRefreshKey] = useState(0);
  const operations = useClusterOperations(cluster?.id === clusterId ? clusterId : '', refreshKey);
  const memberRuntime = operations.data?.recovery?.members;
  const numberedMembers = new Map(sortClusterMembers(members).map((node, index) => [
    node.id, '节点' + String(index + 1).padStart(2, '0'),
  ]));

  const load = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const [record, clusterNodes, clusterList, candidates] = await Promise.all([
        getCluster(clusterId),
        getClusterNodes(clusterId),
        getClusters(),
        getEligibleClusterNodes(),
      ]);
      setCluster(record);
      setMembers(clusterNodes);
      setClusters(clusterList.items);
      setCandidateNodes(candidates);
      setHeaderUpdatedAt(record.resourceSummary.updatedAt);
      setRefreshKey((key) => key + 1);
    } catch {
      setError('无法加载集群详情，集群可能不存在或 Backend 暂时不可用。');
    } finally {
      setLoading(false);
    }
  }, [clusterId, setHeaderUpdatedAt]);

  useEffect(() => {
    const timer = globalThis.setTimeout(() => void load(), 0);
    return () => globalThis.clearTimeout(timer);
  }, [load]);

  useEffect(() => {
    if (loading || cluster?.id !== clusterId) return;
    const controller = new AbortController();
    let timer: number;
    async function refreshMembers() {
      try {
        const [record, items] = await Promise.all([
          getCluster(clusterId, controller.signal),
          getClusterNodes(clusterId, controller.signal),
        ]);
        if (controller.signal.aborted) return;
        setCluster(record);
        setMembers(items);
        setHeaderUpdatedAt(record.resourceSummary.updatedAt);
        setError(null);
      } catch {
        if (!controller.signal.aborted) setError('成员和容量暂未更新，当前显示最后一次读取的结果。');
      } finally {
        if (!controller.signal.aborted) timer = globalThis.setTimeout(() => void refreshMembers(), 15_000);
      }
    }
    timer = globalThis.setTimeout(() => void refreshMembers(), 15_000);
    return () => {
      controller.abort();
      globalThis.clearTimeout(timer);
    };
  }, [cluster?.id, clusterId, loading, setHeaderUpdatedAt]);

  const saveCluster = async (values: ClusterInput) => {
    setSaving(true);
    try {
      await updateCluster(clusterId, values);
      message.success('集群信息已更新');
      setEditing(false);
      await load();
    } catch {
      message.error('集群信息保存失败');
    } finally {
      setSaving(false);
    }
  };

  const assign = async (nodeId: string) => {
    setActionNodeId(nodeId);
    try {
      await assignNodeToCluster(clusterId, nodeId);
      message.success('节点已加入集群');
      setAssignOpen(false);
      setSelectedNodeId(undefined);
      await load();
    } catch {
      message.error('成员关系更新失败');
    } finally {
      setActionNodeId(null);
    }
  };

  const move = async (nodeId: string, targetClusterId: string) => {
    setActionNodeId(nodeId);
    try {
      await moveNodeToCluster(targetClusterId, nodeId);
      message.success('节点已迁移到目标集群');
      await load();
    } catch (moveError) {
      message.error(moveError instanceof Error ? moveError.message : '节点迁移失败');
    } finally {
      setActionNodeId(null);
    }
  };

  const confirmDelete = () => {
    if (!cluster) return;
    if (cluster.nodeCount > 0) {
      modal.info({
        title: '无法删除非空集群',
        content: `该集群当前仍包含 ${cluster.nodeCount} 个节点，请先将节点移出或迁移至其它集群后再删除。`,
        okText: '知道了',
      });
      return;
    }
    modal.confirm({
      title: '删除集群',
      content: (
        <div className="cluster-delete-confirmation">
          <p>集群名称：{cluster.name}</p>
          <p>
            Cluster ID：<code>{cluster.id}</code>
          </p>
          <p>当前节点数：{cluster.nodeCount}</p>
          <p>所属企业：{cluster.enterprise || '--'}</p>
          <p>所属项目：{cluster.project || '--'}</p>
          <p>删除后将移除该集群管理记录，该操作不会删除任何节点。</p>
        </div>
      ),
      okText: '确认删除',
      okButtonProps: { danger: true },
      cancelText: '取消',
      onOk: async () => {
        setDeleting(true);
        try {
          await deleteCluster(cluster.id);
          message.success('空集群已删除');
          navigate(ROUTES.clusters, { replace: true });
        } catch (deleteError) {
          message.error(deleteError instanceof Error ? deleteError.message : '集群删除失败');
        } finally {
          setDeleting(false);
        }
      },
    });
  };

  const unassign = (node: NodeDetail) => {
    modal.confirm({
      title: `将 ${node.name} 移出当前集群？`,
      content: '节点、审批状态、资源池成员关系和调度开关均会保留。',
      okText: '确认移出',
      cancelText: '取消',
      onOk: async () => {
        setActionNodeId(node.id);
        try {
          await unassignNodeFromCluster(clusterId, node.id);
          message.success('节点已移出集群');
          await load();
        } catch {
          message.error('节点移出失败');
        } finally {
          setActionNodeId(null);
        }
      },
    });
  };

  const columns: ColumnsType<NodeDetail> = [
    {
      title: '节点名称',
      dataIndex: 'name',
      render: (name, node) => (
        <div className="cluster-operations__reference">
          <Link className="cluster-detail__node-link" to={buildNodeDetailPath(node.id)}>
            {numberedMembers.get(node.id) ?? name}
          </Link>
          <small>{name}</small>
        </div>
      ),
    },
    {
      title: 'Node ID',
      dataIndex: 'id',
      width: 250,
      render: (value) => (
        <Tooltip title={value}>
          <code className="node-id cluster-detail__node-id">{value}</code>
        </Tooltip>
      ),
    },
    {
      title: 'IP',
      width: 145,
      render: (_, node) => node.ipAddress ?? node.network.ipv4Address ?? '--',
    },
    {
      title: '在线状态',
      dataIndex: 'operationalStatus',
      render: (status) => <StatusBadge status={status} />,
    },
    {
      title: '调度状态',
      render: (_, node) => {
        const value = memberRuntime?.[node.id]?.scheduling_status ?? 'unknown';
        const labels = { eligible: '可调度', paused: '暂停调度', ineligible: '不可调度', recovering: '恢复中', unknown: '—' };
        const tones = { eligible: 'success', paused: 'waiting', ineligible: 'observed', recovering: 'waiting', unknown: 'observed' };
        return <span className="cluster-operations__result" data-result={tones[value]}>{labels[value]}</span>;
      },
    },
    { title: 'CPU', render: (_, node) => `${node.resources.cpu.total} Core` },
    { title: 'GPU', render: (_, node) => `${node.resources.gpu?.total ?? 0} Device` },
    { title: '显存', render: (_, node) => `${node.videoMemory?.total ?? 0} GB` },
    { title: 'NPU', render: (_, node) => `${node.resources.npu?.total ?? 0} TOPS` },
    { title: 'DLA', render: (_, node) => (node.resources.dla?.total ?? 0) + ' Device' },
    { title: '内存', render: (_, node) => formatNumber(node.memory.total) + ' GB' },
    { title: '当前执行分片', render: (_, node) => memberRuntime?.[node.id]?.current_shards ?? '—' },
    { title: '最近恢复时间', render: (_, node) => formatTime(memberRuntime?.[node.id]?.last_recovered_at) },
    {
      title: '最后上报时间',
      dataIndex: 'lastResourceAt',
      render: (value) => (value ? dayjs(value).format('YYYY-MM-DD HH:mm:ss') : '--'),
    },
    {
      title: '成员管理',
      key: 'actions',
      width: 300,
      render: (_, node) => (
        <Space>
          <Select
            size="small"
            className="cluster-detail__move-select"
            popupClassName="cluster-detail-select-dropdown"
            placeholder="迁移到..."
            suffixIcon={<SwapOutlined />}
            options={clusters
              .filter((item) => item.id !== clusterId)
              .map((item) => ({ label: item.name, value: item.id }))}
            onChange={(target) => void move(node.id, target)}
          />
          <Button
            size="small"
            danger
            icon={<UserDeleteOutlined />}
            loading={actionNodeId === node.id}
            onClick={() => unassign(node)}
          >
            移出
          </Button>
        </Space>
      ),
    },
  ];

  if (cluster?.id !== clusterId && loading) {
    return (
      <main className="cluster-page">
        <Card loading />
      </main>
    );
  }

  if (cluster?.id !== clusterId) {
    return (
      <main className="cluster-page">
        <Result
          status="404"
          title="集群不可用"
          subTitle={error ?? '没有找到该集群。'}
          extra={<Button onClick={() => navigate(ROUTES.clusters)}>返回集群列表</Button>}
        />
      </main>
    );
  }

  const summary = cluster.resourceSummary;
  const resourceCards = [
    { key: 'cpu', title: 'CPU 总量', value: summary.resources.cpu.total, unit: 'Core', icon: <ThunderboltOutlined /> },
    { key: 'gpu', title: 'GPU 总量', value: summary.resources.gpu.total, unit: 'Device', icon: <ControlOutlined /> },
    { key: 'vram', title: '显存总量', value: summary.videoMemory.total, unit: 'GB', icon: <CloudServerOutlined /> },
    { key: 'npu', title: 'NPU 总量', value: summary.resources.npu.total, unit: 'TOPS', icon: <DeploymentUnitOutlined /> },
    { key: 'dla', title: 'DLA 总量', value: summary.resources.dla.total, unit: 'Device', icon: <ApartmentOutlined /> },
    { key: 'memory', title: '内存', value: summary.memory.total, unit: 'GB', icon: <DatabaseOutlined /> },
    { key: 'storage', title: '存储', value: summary.storage.total, unit: 'GB', icon: <HddOutlined /> },
  ] as const;

  return (
    <main className="cluster-page cluster-detail-page">
      <div className="cluster-detail-toolbar">
        <div className="cluster-detail-toolbar__title">
          <ArrowLeftOutlined aria-hidden="true" />
          <strong>集群详情</strong>
        </div>
        <Space className="cluster-page__header-actions" size={8} wrap>
          <Button icon={<ArrowLeftOutlined />} onClick={() => navigate(ROUTES.clusters)}>
            返回列表
          </Button>
          <Button icon={<EditOutlined />} onClick={() => setEditing(true)}>
            编辑
          </Button>
          <Button icon={<ReloadOutlined />} loading={loading} onClick={() => void load()}>
            刷新
          </Button>
          <Button danger icon={<DeleteOutlined />} loading={deleting} onClick={confirmDelete}>
            删除集群
          </Button>
          <Button type="primary" icon={<PlusOutlined />} onClick={() => setAssignOpen(true)}>
            添加节点
          </Button>
        </Space>
      </div>
      {error && <Alert className="cluster-detail-page__alert" type="warning" showIcon message={error} />}

      <section className="cluster-detail-overview">
        <article className="cluster-detail-panel cluster-basic-panel">
          <header className="cluster-detail-panel__heading">
            <span className="cluster-detail-panel__heading-icon" aria-hidden="true"><ApartmentOutlined /></span>
            <strong>集群基础信息</strong>
          </header>
          <dl className="cluster-basic-grid">
            <div><dt>集群名称</dt><dd>{cluster.name}</dd></div>
            <div><dt>成员运行状态</dt><dd><StatusBadge status={cluster.status} /></dd></div>
            <div><dt>Cluster ID</dt><dd><code title={cluster.id}>{cluster.id}</code></dd></div>
            <div><dt>节点数量</dt><dd>{cluster.nodeCount} 个</dd></div>
            <div><dt>版本</dt><dd>{cluster.version}</dd></div>
            <div><dt>所属企业</dt><dd>{cluster.enterprise || '--'}</dd></div>
            <div><dt>所属项目</dt><dd>{cluster.project || '--'}</dd></div>
            <div><dt>管理端点</dt><dd title={cluster.managementEndpoint}>{cluster.managementEndpoint || '--'}</dd></div>
          </dl>
        </article>

        <section className="cluster-resource-grid" aria-label="集群资源汇总">
          {resourceCards.map((item, index) => (
            <article
              key={item.key}
              className={`cluster-resource-card cluster-resource-card--${item.key}${index < 3 ? ' cluster-resource-card--top' : ''}`}
            >
              <span className="cluster-resource-card__icon" aria-hidden="true">{item.icon}</span>
              <div>
                <span className="cluster-resource-card__title">{item.title}</span>
                <strong>{item.unit === 'GB' ? item.value.toFixed(1) : formatNumber(item.value)}</strong>
                <small>{item.unit}</small>
              </div>
            </article>
          ))}
        </section>
      </section>

      <ClusterMemberScene clusterId={cluster.id} members={members} loading={loading} />
      <ClusterRuntimePanels clusterId={cluster.id} nodeCount={cluster.nodeCount} operations={operations} />

      <section className="cluster-detail-panel cluster-member-table-panel">
        <header className="cluster-detail-panel__heading">
          <span className="cluster-detail-panel__heading-icon" aria-hidden="true"><DeploymentUnitOutlined /></span>
          <strong>成员节点</strong>
          <em>{members.length}</em>
        </header>
        <div className="cluster-member-table-panel__body">
          <Table
            className="cluster-member-table"
            rowKey="id"
            columns={columns}
            dataSource={members}
            loading={loading}
            scroll={{ x: 1900 }}
            pagination={false}
            locale={{ emptyText: '该集群尚未分配节点' }}
          />
        </div>
      </section>

      <ClusterEventPanel key={'events-' + cluster.id} clusterId={cluster.id} refreshKey={refreshKey} />

      <Modal
        rootClassName="cluster-detail-modal"
        title="添加节点"
        open={assignOpen}
        width={720}
        okText="确认加入"
        cancelText="取消"
        confirmLoading={selectedNodeId ? actionNodeId === selectedNodeId : false}
        okButtonProps={{ disabled: !selectedNodeId }}
        onCancel={() => {
          setAssignOpen(false);
          setSelectedNodeId(undefined);
        }}
        onOk={() => selectedNodeId && void assign(selectedNodeId)}
      >
        <Alert
          type="info"
          showIcon
          message="仅显示可直接加入的未分配节点"
          description="候选节点必须已注册、已批准、仍在资源池、当前 ONLINE；是否允许调度不会影响集群归属。已有归属的节点请使用成员行内的“迁移到”操作。"
          className="cluster-detail__modal-alert"
        />
        <Select
          showSearch
          virtual={false}
          className="cluster-detail__node-select"
          popupClassName="cluster-node-select-dropdown"
          placeholder="选择节点"
          filterOption={(input, option) => {
            const node = candidateNodes.find((item) => item.id === option?.value);
            if (!node) return false;
            return `${node.name} ${node.hostname ?? ''} ${node.ipAddress ?? ''} ${node.network.ipv4Address} ${node.id}`
              .toLocaleLowerCase()
              .includes(input.toLocaleLowerCase());
          }}
          value={selectedNodeId}
          options={candidateNodes.map((node) => ({
            value: node.id,
            label: node.name,
          }))}
          optionRender={(option) => {
            const node = candidateNodes.find((item) => item.id === option.value);
            return node ? <ClusterNodeOption node={node} /> : option.label;
          }}
          onChange={setSelectedNodeId}
        />
      </Modal>

      <ClusterFormModal
        open={editing}
        loading={saving}
        cluster={cluster}
        candidateNodes={[]}
        onCancel={() => setEditing(false)}
        onSubmit={saveCluster}
      />
    </main>
  );
}
