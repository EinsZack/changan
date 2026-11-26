"""Read-only recovery projections for the cluster detail page."""
from collections import defaultdict
from datetime import datetime, timezone

from . import db
from .cluster_operations import SCOPED_ATTEMPTS
from .config import TERMINAL
from .pool.models.node import NodeOperationalStatus, RuntimeStatus
from .pool.services.node_service import resource_sample_is_stale


MEMBERSHIP_EVENTS = {"MEMBER_JOINED", "MEMBER_REMOVED", "MEMBER_MOVED_OUT", "MEMBER_MOVED_IN", "MEMBER_RECORD_DELETED"}
RESOURCE_RECOVERED = "节点资源与运行状态已恢复正常"


def _time(value):
    return datetime.fromtimestamp(value, timezone.utc) if value is not None else None


def _row(event_id, cluster_id, category, event_type, at, state, *, target_id,
         target_name, reason=None, seconds=None, **references):
    return {
        "id": str(event_id), "cluster_id": cluster_id, "category": category,
        "event_type": event_type, "occurred_at": at, "status": state,
        "target_id": target_id, "target_name": target_name, "reason": reason,
        "duration_seconds": seconds, **references,
    }


def _node_history(cluster_id, history, start, now):
    pending, episodes, recovered, linked_events = {}, [], {}, set()
    for event in sorted(history, key=lambda e: (e.occurred_at, e.id)):
        if event.occurred_at > now:
            continue
        node = event.target_id
        if event.category == "lifecycle" and event.event_type in MEMBERSHIP_EVENTS and event.result == "success":
            pending.pop(node, None)
            if node in recovered:
                recovered.pop(node)
            for episode in reversed(episodes):
                if episode["loss"].target_id == node and episode.get("end") is None:
                    episode["end"] = event.occurred_at
                    break
        elif event.event_type == "NODE_OFFLINE":
            if node not in pending:
                episode = {"loss": event}
                episodes.append(episode)
                pending[node] = episode
        elif event.event_type == "RECONNECT_STARTED" and node in pending:
            linked_events.add(str(event.id))
            pending[node].setdefault("reconnect", event)
        elif event.event_type == "NODE_RECOVERED" and event.reason != RESOURCE_RECOVERED:
            episode = pending.pop(node, None)
            if episode and episode.get("reconnect"):
                episode["recovered"] = event
                recovered[node] = event.occurred_at

    completed = [e for e in episodes if "recovered" in e and e["recovered"].occurred_at >= start]
    admission = db.query(f"""
        SELECT DISTINCT a.created, sa.physical_node_id
        FROM ({SCOPED_ATTEMPTS}) sa JOIN attempts a ON a.id = sa.attempt_id
        WHERE sa.cluster_id = ? AND sa.execution_confirmed = 1 AND a.created BETWEEN ? AND ?
          AND sa.admission_reasons = 0
        ORDER BY a.created
    """, (cluster_id, start.timestamp(), now.timestamp())) if completed else []
    admitted_at = defaultdict(list)
    for row in admission:
        admitted_at[row["physical_node_id"]].append(_time(row["created"]))

    rows, durations, qualified = [], [], 0
    for index, episode in enumerate(episodes):
        loss = episode["loss"]
        reconnect = episode.get("reconnect")
        recovery = episode.get("recovered")
        node = loss.target_id
        target = {"target_id": node, "target_name": loss.target_name, "node_id": node}
        rows.append(_row(loss.id, cluster_id, "node_recovery", "NODE_OFFLINE", loss.occurred_at,
                         "已离线", reason=loss.reason, **target))
        if reconnect:
            rows.append(_row(reconnect.id, cluster_id, "node_recovery", "RECONNECT_STARTED",
                             reconnect.occurred_at, "恢复中", reason=reconnect.reason, **target))
        if not recovery:
            continue
        elapsed = (recovery.occurred_at - loss.occurred_at).total_seconds()
        rows.append(_row(recovery.id, cluster_id, "node_recovery", "NODE_RECOVERED",
                         recovery.occurred_at, "已恢复", seconds=elapsed, reason=recovery.reason, **target))
        rows.append(_row(f"{recovery.id}:cluster", cluster_id, "node_recovery", "MEMBERSHIP_RETAINED",
                         recovery.occurred_at, "已确认", **target))
        rows.append(_row(f"{recovery.id}:runtime", cluster_id, "sync", "NODE_RUNTIME_SYNC",
                         recovery.occurred_at, "已恢复", **target))
        if episode in completed:
            durations.append(elapsed)
            next_loss = next((e["loss"].occurred_at for e in episodes[index + 1:] if e["loss"].target_id == node), None)
            end = min((at for at in (next_loss, episode.get("end")) if at is not None), default=None)
            proof = next((at for at in admitted_at[node] if recovery.occurred_at <= at and (end is None or at < end)), None)
            if proof is not None:
                qualified += 1
                rows.append(_row(f"{recovery.id}:admission", cluster_id, "node_recovery",
                                 "SCHEDULING_RESTORED", proof, "已确认", reason="恢复后通过资源池准入并确认接单", **target))

    represented = {row["id"] for row in rows} | linked_events
    for event in history:
        if event.occurred_at > now or str(event.id) in represented:
            continue
        target = {"target_id": event.target_id, "target_name": event.target_name}
        if event.category == "lifecycle":
            rows.append(_row(event.id, cluster_id, "lifecycle", event.event_type, event.occurred_at,
                             {"success": "操作成功", "rejected": "操作失败"}.get(event.result, "已记录"),
                             reason=event.reason, seconds=event.duration_seconds, before=event.before,
                             after=event.after, **target))
        elif event.event_type == "NODE_RECOVERED" and event.reason == RESOURCE_RECOVERED:
            rows.append(_row(event.id, cluster_id, "sync", "NODE_RESOURCE_SYNC", event.occurred_at,
                             "已恢复", reason=event.reason, node_id=event.target_id, **target))
        elif event.event_type in {"RECONNECT_STARTED", "NODE_RECOVERED"}:
            rows.append(_row(event.id, cluster_id, "node_recovery", event.event_type, event.occurred_at,
                             "恢复中" if event.event_type == "RECONNECT_STARTED" else "已恢复",
                             reason=event.reason, node_id=event.target_id, **target))
        elif event.category == "sync":
            rows.append(_row(event.id, cluster_id, "sync", event.event_type, event.occurred_at,
                             "操作失败" if event.result == "rejected" else "已记录",
                             reason=event.reason, seconds=event.duration_seconds, **target))

    return {
        "losses": sum(e["loss"].occurred_at >= start for e in episodes),
        "reconnections": sum(e.get("reconnect") is not None and e["reconnect"].occurred_at >= start for e in episodes),
        "retained_memberships": len(completed),
        "scheduling_restored": qualified if qualified or not completed else None,
        "mean_recovery_seconds": sum(durations) / len(durations) if durations else None,
        "last_loss_at": max((e["loss"].occurred_at for e in episodes), default=None),
        "last_recovered_at": max((e["recovered"].occurred_at for e in episodes if "recovered" in e), default=None),
    }, recovered, rows


def _task_history(cluster_id, start, now):
    # Each incident is attributed by the affected attempt's dispatch decision,
    # even when the executor later changes membership or the retry runs elsewhere.
    affected = db.query(f"""
        WITH scope AS ({SCOPED_ATTEMPTS}), losses AS (
            SELECT json_extract(j.value, '$.attempt_id') AS attempt_id,
                   i.id AS incident_id, i.detected_at AS lost_at, i.link_restored_at,
                   json_extract(j.value, '$.reconciled_at') AS reconciled_at, i.reason
            FROM node_incidents i JOIN json_each(i.affected) j
            UNION ALL
            SELECT a.id, 'timeout:' || a.id, a.finished, NULL, NULL, '执行会话或连接超时'
            FROM attempts a
            WHERE a.status = 'TIMED_OUT'
              AND (a.error LIKE '会话心跳超时%' OR a.error LIKE '失联超时%')
              AND NOT EXISTS (
                  SELECT 1 FROM node_incidents i JOIN json_each(i.affected) j
                  WHERE i.node_id = a.node_id AND json_extract(j.value, '$.attempt_id') = a.id
              )
        )
        SELECT DISTINCT a.id, a.task_id, a.segment_id, a.status, a.finished,
               s.active_attempt, s.status AS segment_status, t.status AS task_status,
               l.*, replacement.id AS replacement_id, replacement.created AS replacement_at,
               replacement.execution_id AS replacement_execution, replacement.status AS replacement_status,
               winner.id AS winner_id, winner.finished AS winner_finished
        FROM losses l
        JOIN scope ON scope.attempt_id = l.attempt_id
        JOIN attempts a ON a.id = l.attempt_id
        JOIN segments s ON s.id = a.segment_id AND s.task_id = a.task_id
        JOIN tasks t ON t.id = a.task_id
        LEFT JOIN attempts replacement ON replacement.id = (
            SELECT r.id FROM attempts r
            WHERE r.segment_id = a.segment_id AND r.task_id = a.task_id AND r.id <> a.id
              AND r.created >= a.finished AND r.created > a.created
            ORDER BY r.created, r.id LIMIT 1
        )
        LEFT JOIN attempts winner ON winner.id = s.active_attempt AND s.status = 'SUCCEEDED'
             AND s.result IS NOT NULL AND winner.status = 'SUCCEEDED' AND winner.result IS NOT NULL
             AND winner.actual_node = winner.node_id
        WHERE scope.cluster_id = ? AND scope.physical_node_id IS NOT NULL
          AND l.lost_at IS NOT NULL AND l.lost_at <= ?
    """, (cluster_id, now.timestamp()))
    lower, upper = start.timestamp(), now.timestamp()

    def recent(at):
        return at is not None and lower <= at <= upper

    impacted_tasks, impacted, waiting, pending, redispatched, valid = (set() for _ in range(6))
    rows, last_check, latest_retry = {}, None, None
    for record in affected:
        task, shard, attempt = record["task_id"], record["segment_id"], record["id"]
        target = {"target_id": task, "target_name": task, "task_id": task, "segment_id": shard}

        def emit(key, category, kind, at, state, ref=attempt, reason=None, seconds=None):
            if at is not None and at <= upper:
                rows[key] = _row(key, cluster_id, category, kind, _time(at), state,
                                 attempt_id=ref, reason=reason, seconds=seconds, **target)

        episode = f"{record['incident_id']}:{attempt}"
        lost = record["lost_at"]
        if recent(lost):
            impacted_tasks.add(task)
            impacted.add(shard)
        if not record["incident_id"].startswith("timeout:"):
            emit(f"{episode}:loss", "task_recovery", "EXECUTION_LINK_LOST", lost,
                 "等待恢复", reason=record["reason"])
        current = (record["task_status"] not in TERMINAL and record["active_attempt"] == attempt
                   and record["status"] not in TERMINAL)
        if current and record["reconciled_at"] is None:
            if record["link_restored_at"] is not None:
                pending.add(shard)
            elif record["status"] == "UNKNOWN":
                waiting.add(shard)
        if record["link_restored_at"] is not None:
            emit(f"{episode}:link", "task_recovery", "EXECUTION_LINK_RESTORED",
                 record["link_restored_at"], "待核对")
        check = record["reconciled_at"]
        if check is not None and check <= upper:
            last_check = check if last_check is None else max(last_check, check)
            link_restored = record["link_restored_at"]
            elapsed = check - link_restored if link_restored is not None and check >= link_restored else None
            emit(f"{episode}:check", "sync", "EXECUTION_RECONCILED", check, "核对完成", seconds=elapsed)
        if record["status"] == "TIMED_OUT":
            emit(f"{attempt}:timeout", "task_recovery", "ATTEMPT_TIMED_OUT", record["finished"], "已超时")
            retry_at = record["replacement_at"]
            if retry_at is not None and retry_at <= upper:
                if recent(retry_at):
                    redispatched.add(shard)
                state = {
                    "SUCCEEDED": "已完成", "FAILED": "执行失败", "TIMED_OUT": "已超时",
                    "CANCELED": "已取消", "RUNNING": "执行中", "UPLOADING": "回传中",
                    "FINALIZING": "核验中", "UNKNOWN": "待核对",
                }.get(record["replacement_status"], "已接收" if record["replacement_execution"] else "已派单")
                replacement_id = record["replacement_id"]
                emit(f"{replacement_id}:redispatch", "task_recovery", "SHARD_REDISPATCHED",
                     retry_at, "已重派", replacement_id)
                if latest_retry is None or (retry_at, replacement_id) > (latest_retry[0], latest_retry[1]):
                    latest_retry = (retry_at, replacement_id, state, task, shard)
        finished = record["winner_finished"]
        winner = record["winner_id"]
        if finished is not None and finished >= lost and (
            (winner == attempt and check is not None and check <= upper)
            or (winner != attempt and record["status"] in {"TIMED_OUT", "FAILED"}
                and record["finished"] is not None and finished >= record["finished"])
        ):
            if recent(finished):
                valid.add(shard)
            if winner != attempt:
                emit(f"{winner}:retry-completed", "task_recovery", "SHARD_RETRY_COMPLETED",
                     finished, "已完成", winner)
            emit(f"{winner}:validated", "sync", "RECOVERY_RESULT_CONFIRMED", finished, "结果已确认", winner)

    return {
        "affected_tasks": len(impacted_tasks), "affected_shards": len(impacted),
        "waiting_shards": len(waiting - pending), "redispatched_shards": len(redispatched),
        "pending_reconciliation_shards": len(pending), "confirmed_results": len(valid),
        "last_reconciled_at": _time(last_check),
        "last_redispatch": ({
            "at": _time(latest_retry[0]), "attempt_id": latest_retry[1], "status": latest_retry[2],
            "task_id": latest_retry[3], "segment_id": latest_retry[4],
        } if latest_retry else None),
    }, list(rows.values())


def _member_runtime(bridge, members, recovered, active, now):
    executions = db.query("SELECT id, enabled, error, observed, snapshot FROM nodes")
    for row in executions:
        row["snapshot"] = db.loads(row["snapshot"], {})
    bindings = bridge.bindings(executions)
    executors = defaultdict(list)
    for row in executions:
        binding = bindings[row["id"]]
        if binding["record"] is not None:
            executors[str(binding["record"].node_id)].append((row, binding))

    result = {}
    for node in members:
        node_id = str(node.node_id)
        candidates = executors[node_id]
        healthy = any(
            row["enabled"] and not row["error"] and row["observed"] is not None
            and 0 <= now.timestamp() - row["observed"] < 8 and row["snapshot"].get("healthy")
            and not bridge.evaluate(binding)
            for row, binding in candidates
        )
        if not node.scheduling_enabled:
            status = "paused"
        elif node.runtime_status == RuntimeStatus.RECONNECTING:
            status = "recovering"
        elif not candidates and node.schedulable:
            status = "unknown"
        elif healthy and not resource_sample_is_stale(node, now, bridge.settings.resource_stale_after_seconds):
            status = "eligible"
        else:
            status = "ineligible"
        result[node_id] = {
            "scheduling_status": status, "current_shards": active.get(node_id, 0),
            "last_recovered_at": recovered.get(node_id),
        }
    return result


def cluster_recovery(bridge, cluster_id, now, start, *, members=None, active=None):
    history = bridge.cluster_service.list_events(cluster_id, None, None)
    node, recovered, node_rows = _node_history(cluster_id, history, start, now)
    task, task_rows = _task_history(cluster_id, start, now)
    if members is not None:
        node["online_members"] = sum(n.operational_status == NodeOperationalStatus.ONLINE for n in members)
        node["total_members"] = len(members)
        return {"nodes": node, "tasks": task, "members": _member_runtime(bridge, members, recovered, active or {}, now)}
    return sorted(node_rows + task_rows, key=lambda row: (row["occurred_at"], row["id"]), reverse=True)
