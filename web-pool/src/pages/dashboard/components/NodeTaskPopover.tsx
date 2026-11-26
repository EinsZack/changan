import { useEffect, useState } from 'react';
import dayjs from 'dayjs';
import { Link } from 'react-router-dom';
import { getNodeTaskSummary, taskDetailUrl } from '../../../services/platformService';
import type { NodeTaskSummary } from '../../../services/platformService';

const RESULT_TEXT = { SUCCEEDED: '已完成', FAILED: '失败', TIMED_OUT: '超时', CANCELED: '已取消' } as const;

export function NodeTaskPopover({ nodeId, enabled }: { nodeId: string; enabled: boolean }) {
  const [summary, setSummary] = useState<NodeTaskSummary>();
  const [failed, setFailed] = useState(false);

  useEffect(() => {
    if (!enabled) return;
    const controller = new AbortController();
    let timer: ReturnType<typeof setTimeout>;
    async function load() {
      try {
        const response = await getNodeTaskSummary(nodeId, controller.signal);
        if (!controller.signal.aborted) {
          setSummary(response);
          setFailed(false);
        }
      } catch {
        if (!controller.signal.aborted) {
          setSummary(undefined);
          setFailed(true);
        }
      } finally {
        if (!controller.signal.aborted) timer = setTimeout(() => void load(), 8_000);
      }
    }

    void load();
    return () => {
      controller.abort();
      clearTimeout(timer);
    };
  }, [enabled, nodeId]);

  const recent = summary?.recent_result;
  const count = summary?.active_task_count ?? 0;
  const remaining = count - (summary?.tasks.length ?? 0);
  const message = !enabled ? '暂无任务数据'
    : failed ? '暂时无法读取任务状态'
    : !summary ? '正在读取任务…'
    : !summary.execution_bound && summary.assigned_task_count === 0 ? '暂无可关联的执行记录' : undefined;

  return (
    <section className="node-task-popover" aria-label="任务分配与执行">
      <header>
        <strong>任务分配与执行</strong>
      </header>
      <dl className="node-task-popover__metrics">
        <div><dt>累计分配</dt><dd>{summary?.assigned_task_count ?? '—'}</dd></div>
        <div><dt>当前在办</dt><dd>{summary?.active_task_count ?? '—'}</dd></div>
        <div title="本节点承担的该任务分片均已成功回传"><dt>已完成</dt><dd>{summary?.completed_task_count ?? '—'}</dd></div>
      </dl>
      {message && <p>{message}</p>}
      {summary && count > 0 && (
        <>
          <div className="node-task-popover__counts">
            <span>待执行 {summary.waiting_shards} 片</span>
            <span>执行中 {summary.running_shards} 片</span>
            {summary.unverified_shards > 0 && <span>待核对 {summary.unverified_shards} 片</span>}
          </div>
          <ul className="node-task-popover__list">
            {summary.tasks.map((task) => (
              <li key={task.task_id}>
                <Link to={taskDetailUrl(task.task_id)} title={task.name}>{task.name}</Link>
                <span>{[
                  task.running_shards > 0 && '执行中',
                  task.waiting_shards > 0 && '待执行',
                  task.unverified_shards > 0 && '待核对',
                ].filter(Boolean).join(' · ')}</span>
              </li>
            ))}
          </ul>
          {remaining > 0 && <small>另有 {remaining} 个已分配任务</small>}
        </>
      )}
      {recent && (
        <div className="node-task-popover__recent">
          <span>最近分片结果</span>
          <Link to={taskDetailUrl(recent.task_id)} title={recent.name}>{recent.name}</Link>
          <span title={dayjs(recent.finished_at).format('YYYY-MM-DD HH:mm:ss')}>{RESULT_TEXT[recent.status]}</span>
        </div>
      )}
    </section>
  );
}
