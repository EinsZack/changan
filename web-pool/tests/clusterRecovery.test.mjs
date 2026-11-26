import assert from 'node:assert/strict';
import test from 'node:test';
import {
  formatExecutionNodes, recoveryStatusTone, resourceLoadText,
} from '../src/pages/clusters/components/clusterRecoveryPresentation.ts';

test('cluster operation metrics distinguish zero from unknown', () => {
  assert.equal(formatExecutionNodes(0, 4), '0 / 4');
  assert.equal(formatExecutionNodes(null, 4), '— / 4');
  assert.equal(resourceLoadText('live', 'unsupported'), '不支持');
  assert.equal(resourceLoadText('live', 'unreported'), '未上报');
  assert.equal(resourceLoadText('live', 'unified'), '统一内存');
  assert.equal(resourceLoadText('stale', 'unified'), '暂无数据');
});

test('cluster recovery history distinguishes waiting, timeouts and verified results', () => {
  assert.equal(recoveryStatusTone('待核对'), 'waiting');
  assert.equal(recoveryStatusTone('已超时'), 'rejected');
  assert.equal(recoveryStatusTone('结果已确认'), 'success');
  assert.equal(recoveryStatusTone('已派单'), 'observed');
});
