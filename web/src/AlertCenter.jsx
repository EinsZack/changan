import React, {useCallback, useEffect, useState} from 'react';
import {AlertCircle, Bell, Check, CheckCheck, ChevronLeft, ChevronRight, ExternalLink, RefreshCw} from 'lucide-react';
import './alerts.css';

const formatTime = value => value ? new Date(value * 1000).toLocaleString('zh-CN', {hour12: false}) : '—';
const PAGE_SIZE = 20;

export function AlertBanner({alerts, open}) {
    if (!alerts?.summary.unread) return null;
    const latest = alerts.items[0];
    return <div className="alert-banner" role="status" aria-live="polite">
        <AlertCircle size={20}/>
        <div><strong>{alerts.summary.unread} 条未读告警</strong>
            {latest && <p>{latest.task_name}：{latest.title}</p>}</div>
        <button onClick={open}>查看告警 <ChevronRight size={15}/></button>
    </div>;
}

export default function AlertCenter({request, openTask, onChanged}) {
    const [result, setResult] = useState(null);
    const [rules, setRules] = useState([]);
    const [status, setStatus] = useState('open');
    const [readFilter, setReadFilter] = useState('');
    const [rule, setRule] = useState('');
    const [offset, setOffset] = useState(0);
    const [revision, setRevision] = useState(0);
    const [busy, setBusy] = useState(false);
    const [error, setError] = useState('');
    const [message, setMessage] = useState('');

    useEffect(() => {
        let current = true;
        request('/api/alerts/rules').then(value => {if (current) setRules(value);})
            .catch(failure => {if (current) setError(failure.message);});
        return () => {current = false;};
    }, [request]);

    useEffect(() => {
        let current = true;
        let loading = false;
        const refresh = async () => {
            if (loading) return;
            loading = true;
            const query = new URLSearchParams({limit: PAGE_SIZE, offset});
            if (status) query.set('status', status);
            if (readFilter) query.set('unread', readFilter);
            if (rule) query.set('rule', rule);
            try {
                const value = await request('/api/alerts?' + query);
                if (current) {
                    setResult(value);
                    setError('');
                    if (!value.items.length && offset > 0) setOffset(Math.max(0, offset - PAGE_SIZE));
                }
            } catch (failure) {
                if (current) setError(failure.message);
            } finally {
                loading = false;
            }
        };
        refresh();
        const timer = setInterval(refresh, 2000);
        return () => {current = false; clearInterval(timer);};
    }, [request, status, readFilter, rule, offset, revision]);

    const update = useCallback(async (alertId, operation) => {
        setBusy(true);
        try {
            await request('/api/alerts/' + alertId + '/' + operation, {method: 'POST'});
            setMessage(operation === 'read' ? '已标为已读' : '告警已确认');
            setRevision(value => value + 1);
            await onChanged();
        } catch (failure) {
            setError(failure.message);
        } finally {
            setBusy(false);
        }
    }, [request, onChanged]);

    const filter = (setter, value) => {setter(value); setOffset(0); setResult(null);};
    const summary = result?.summary;
    return <>
        <div className="page-title"><div><span className="eyebrow">ALERTS</span>
            <h1>站内告警</h1><p>跟踪未按时完成和执行失败的任务，查看原因并确认处理。</p></div>
            <button onClick={() => setRevision(value => value + 1)}><RefreshCw size={15}/>刷新告警</button>
        </div>
        <div className="metrics alert-metrics">
            {[['待确认', summary?.open], ['未读告警', summary?.unread], ['累计告警', summary?.total]].map(([label, value]) =>
                <div className="metric" key={label}><div><span>{label}</span><Bell size={17}/></div>
                    <strong>{value ?? '—'}</strong><small>{label === '待确认' ? '需要查看和处理的任务异常' : label === '未读告警' ? '查看后可标为已读' : '按任务保存的告警记录'}</small></div>)}
        </div>
        <details className="panel alert-rules"><summary>触发规则与处理说明</summary>
            {rules.map(item => <p key={item.id}><strong>{item.name}：</strong>{item.condition} {item.action}</p>)}
            <p>确认告警表示已知晓并处理该提醒，不会把原任务改为成功。需要再次执行时，可进入任务详情重新提交。</p>
        </details>
        <div className="filters alert-filters">
            <label>处理状态<select aria-label="处理状态" value={status} onChange={event => filter(setStatus, event.target.value)}>
                <option value="open">待确认</option><option value="acknowledged">已确认</option><option value="">全部状态</option>
            </select></label>
            <label>阅读状态<select aria-label="阅读状态" value={readFilter} onChange={event => filter(setReadFilter, event.target.value)}>
                <option value="">全部</option><option value="true">未读</option><option value="false">已读</option>
            </select></label>
            <label>告警类型<select aria-label="告警类型" value={rule} onChange={event => filter(setRule, event.target.value)}>
                <option value="">全部类型</option><option value="task_timeout">任务超期</option><option value="task_failed">任务执行失败</option>
            </select></label>
        </div>
        {error && <div className="error" role="alert">{error}</div>}
        {message && <p className="alert-feedback" role="status">{message}</p>}
        {!result ? <div className="empty">正在读取告警…</div> : !result.items.length ?
            <section className="panel empty"><CheckCheck size={28}/><h2>当前筛选下没有告警</h2><p>新的任务异常会自动显示在这里。</p></section> :
            <div className="alert-list">{result.items.map(item =>
                <article className={'panel alert-card severity-' + item.severity + (item.read_at ? '' : ' unread')} key={item.id} data-alert-id={item.id}>
                    <div className="alert-card-heading"><AlertCircle size={22}/><div>
                        <div className="alert-card-tags"><span>{item.severity === 'error' ? '错误' : '警告'}</span>
                            {!item.read_at && <span className="alert-unread">未读</span>}
                            <span>{item.acknowledged_at ? '已确认' : '待确认'}</span></div>
                        <h2>{item.title}</h2><button className="link alert-task-link" onClick={() => openTask(item.task_id)}>{item.task_name}<ExternalLink size={13}/></button>
                    </div><time>{formatTime(item.created)}</time></div>
                    <p className="alert-reason">{item.message}</p>
                    <dl className="alert-facts">
                        <div><dt>任务编号</dt><dd>{item.task_id}</dd></div>
                        <div><dt>任务截止时间</dt><dd>{formatTime(item.deadline_at)}</dd></div>
                        <div><dt>触发时已完成片段</dt><dd>{item.completed} / {item.total}</dd></div>
                        {item.acknowledged_at && <div><dt>确认时间</dt><dd>{formatTime(item.acknowledged_at)}</dd></div>}
                    </dl>
                    <div className="button-row alert-actions">
                        <button onClick={() => openTask(item.task_id)}>查看任务</button>
                        {!item.read_at && <button disabled={busy} onClick={() => update(item.id, 'read')}><Check size={14}/>标为已读</button>}
                        {!item.acknowledged_at && <button className="primary" disabled={busy} onClick={() => update(item.id, 'acknowledge')}><CheckCheck size={14}/>确认告警</button>}
                    </div>
                </article>)}</div>}
        {result && result.total > 0 && <div className="alert-pagination">
            <span>共 {result.total} 条，第 {Math.floor(offset / PAGE_SIZE) + 1} / {Math.ceil(result.total / PAGE_SIZE)} 页</span>
            <div className="button-row"><button disabled={offset === 0} onClick={() => setOffset(value => Math.max(0, value - PAGE_SIZE))}><ChevronLeft size={14}/>上一页</button>
                <button disabled={offset + PAGE_SIZE >= result.total} onClick={() => setOffset(value => value + PAGE_SIZE)}>下一页<ChevronRight size={14}/></button></div>
        </div>}
    </>;
}
