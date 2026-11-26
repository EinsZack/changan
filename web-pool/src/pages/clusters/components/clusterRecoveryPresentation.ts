export const formatExecutionNodes = (executing: number | null | undefined, total: number) =>
  (executing ?? '—') + ' / ' + total;

export function resourceLoadText(status?: string, telemetry?: string, sharedFallback = false) {
  if (status !== 'live') return '暂无数据';
  if (telemetry === 'unified' || sharedFallback) return '统一内存';
  if (telemetry === 'unsupported') return '不支持';
  return '未上报';
}

export function recoveryStatusTone(status: string) {
  if (['已恢复', '已确认', '结果已确认', '核对完成', '操作成功', '已完成'].includes(status)) return 'success';
  if (['已离线', '已超时', '操作失败'].includes(status)) return 'rejected';
  if (['恢复中', '等待恢复', '待核对'].includes(status)) return 'waiting';
  return 'observed';
}
