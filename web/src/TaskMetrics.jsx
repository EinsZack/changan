import React from 'react';
import {ArrowDownToLine, Activity, Database, Files} from 'lucide-react';
import {taskMeasurements, metricValue, byteValue, durationValue} from './taskMetrics';
import './task-metrics.css';

function MeasurementCard({title, value, caption, Icon}) {
    return <div className="metric"><div><span>{title}</span><Icon size={18}/></div><strong>{value}</strong><small>{caption}</small></div>;
}

export default function TaskMetrics({task, Badge}) {
    const metrics = taskMeasurements(task);
    const observedAt = value => value ? new Date(value * 1000).toLocaleString('zh-CN', {hour12: false}) : '—';
    return <div className="task-metrics">
        <div className="metrics measurement-cards">
            <MeasurementCard title="已记录输入量" value={byteValue(metrics.inputBytes)} caption={`含输入量记录的有效片段：${metrics.inputCount} / ${metrics.total}`} Icon={Database}/>
            <MeasurementCard title="片段平均接收速率" value={metricValue(metrics.receiveMbps, 'Mbit/s')} caption={`按下载耗时加权 · ${metrics.transferCount} 个片段`} Icon={ArrowDownToLine}/>
            <MeasurementCard title="片段平均处理吞吐" value={metricValue(metrics.processingFps, '帧/s')} caption={`按处理耗时加权 · ${metrics.processingCount} 个片段`} Icon={Activity}/>
            <MeasurementCard title="有效结果片段" value={`${metrics.resultCount} / ${metrics.total}`} caption="每片仅采用当前有效尝试的成功结果" Icon={Files}/>
        </div>
        <section className="panel">
            <div className="panel-title"><h2>片段执行实测</h2><span>执行器回传结果</span></div>
            <p className="note">输入方向为管理服务 → 执行器；接收速率按片段字节数和下载耗时计算，包含请求与落盘耗时。结果回传方向尚未单独计量。</p>
            <p className="note">汇总使用有效片段的数据量或采样帧数除以各片段对应耗时之和。并行片段耗时累加，不代表任务并发总带宽或端到端吞吐；失败尝试的传输量不计入。</p>
            {metrics.resultCount === 0 && <p className="measurement-empty" role="status">暂无有效片段测量，片段成功并回传结果后显示；未记录的指标保留“—”。</p>}
            {metrics.rows.length > 0 && <div className="measurement-scroll" role="region" aria-label="片段执行实测明细" tabIndex={0}>
                <table className="measurement-table">
                    <thead><tr><th>片段 / 状态</th><th>实际节点</th><th>输入量</th><th>下载耗时</th><th>平均接收速率</th><th>处理吞吐</th></tr></thead>
                    <tbody>{metrics.rows.map(row => <tr key={row.id}>
                        <td><strong>片段 {row.ordinal + 1}</strong><Badge s={row.status}/></td>
                        <td>{row.nodeId || '—'}</td>
                        <td title={row.inputBytes !== null ? row.inputBytes + ' 字节' : undefined}>{byteValue(row.inputBytes)}</td>
                        <td>{durationValue(row.transferSeconds)}</td>
                        <td>{metricValue(row.receiveMbps, 'Mbit/s')}</td>
                        <td>{metricValue(row.processingFps, '帧/s')}</td>
                    </tr>)}</tbody>
                </table>
            </div>}
            <details className="measurement-method"><summary>统计口径与单位</summary>
                <p>1 KiB = 1024 B，1 MiB = 1024² B，1 Mbit/s = 1,000,000 bit/s。接收速率 = 输入字节数 × 8 ÷ 下载秒数 ÷ 1,000,000。</p>
                <p>处理吞吐来自视频解码、采样推理及结果整理过程。为与执行器口径一致，计算速率时将小于 1 毫秒的正耗时按 1 毫秒处理；零耗时、缺失或无效值不参与速率汇总。</p>
                <p>当前默认环境为同一服务器上的 K3s 逻辑节点，传输经过容器网络；这些观测不代表真实车端无线链路性能。</p>
            </details>
        </section>
        <section className="panel">
            <div className="panel-title"><h2>调度时网络观测</h2><span>每次分配保存的节点快照</span></div>
            <p className="note">展示调度当时选中节点的网络观测及本片传输估计。容器下载速率使用近 5 分钟内最多 8 个成功分片，按下载耗时加权；无有效样本时不估算。样本时间保守使用服务器记录的分片开始时间；历史决策不追溯修改。HTTP 往返仅在单独采集时展示，当前 K3s 路径未采集。</p>
            {metrics.snapshots.length ? <div className="measurement-scroll" role="region" aria-label="调度时网络观测明细" tabIndex={0}>
                <table className="measurement-table snapshot-measurements">
                    <thead><tr><th>片段 / 执行尝试</th><th>目标节点</th><th>快照时间</th><th>HTTP往返</th><th>调度用传输速率</th><th>预计传输耗时</th><th>观测来源</th></tr></thead>
                    <tbody>{metrics.snapshots.map(row => <tr key={row.id}>
                        <td><strong>{row.ordinal == null ? '片段未知' : '片段 ' + (row.ordinal + 1)}</strong><small title={row.id}>{row.id}</small><Badge s={row.status}/></td>
                        <td>{row.nodeId}</td><td>{observedAt(row.observedAt)}{row.networkObservedAt && <small>传输样本：{observedAt(row.networkObservedAt)}</small>}</td>
                        <td>{metricValue(row.rttMs, 'ms')}</td><td>{metricValue(row.bandwidthMbps, 'Mbit/s')}</td>
                        <td>{metricValue(row.transferSeconds, 's', 3)}</td><td className="measurement-source">{row.source}{row.networkSampleCount && <small>{row.networkSampleCount} 个分片样本</small>}{row.networkExpiresAt && <small>有效至：{observedAt(row.networkExpiresAt)}</small>}</td>
                    </tr>)}</tbody>
                </table>
            </div> : <p className="measurement-empty">尚未产生节点分配，暂无调度网络快照。</p>}
        </section>
    </div>;
}
