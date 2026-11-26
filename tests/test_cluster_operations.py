from datetime import datetime, timedelta, timezone

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from app import db
from app.main import list_tasks
from app.pool.models.node import HeartbeatRequest, NodeOperationalStatus, RuntimeStatus
from app.pool_bridge import PoolBridge
from app.pool_routes import router
from test_resource_aggregation import online_nodes


@pytest.fixture
def platform(tmp_path):
    pool = PoolBridge(tmp_path / "pool.db")
    app = FastAPI()
    pool.install(app)
    app.include_router(router)
    app.get("/api/tasks")(list_tasks)
    yield TestClient(app), pool
    pool.store.close()


def create_cluster(client, cluster_id, nodes=()):
    response = client.post("/api/v1/clusters", json={
        "cluster_id": cluster_id, "cluster_name": cluster_id,
        "node_ids": [str(node.node_id) for node in nodes],
    })
    assert response.status_code == 201, response.text


def task(task_id, status, finished=None):
    now = datetime.now(timezone.utc).timestamp()
    db.execute("INSERT INTO tasks(id,name,source,status,created,updated,finished,params) VALUES(?,?,?,?,?,?,?,?)",
               (task_id, task_id, "demo", status, now, now, finished, "{}"))


def attempt(task_id, ordinal, status, node, cluster_id, *, actual=True, active=True, retry=0):
    now = datetime.now(timezone.utc).timestamp()
    segment_id = f"{task_id}-{ordinal}"
    attempt_id = f"{segment_id}-{retry}"
    executor = f"executor-{node.node_name}"
    pool = {"state": "BOUND", "cluster_id": cluster_id, "pool_node_id": str(node.node_id)}
    decision = {"target_node": executor, "candidates": [
        {"node_id": "other-executor", "pool": {**pool, "cluster_id": "other-candidate"}},
        {"node_id": executor, "pool": pool},
    ]}
    db.execute("INSERT OR IGNORE INTO segments(id,task_id,ordinal,start,end,path) VALUES(?,?,?,?,?,?)",
               (segment_id, task_id, ordinal, 0, 1, "segment"))
    db.execute("""INSERT INTO attempts(id,task_id,segment_id,node_id,token,status,created,updated,actual_node,decision)
                  VALUES(?,?,?,?,?,?,?,?,?,?)""",
               (attempt_id, task_id, segment_id, executor, "fixture", status, now, now,
                executor if actual else None, db.dumps(decision)))
    if active:
        db.execute("UPDATE segments SET active_attempt=? WHERE id=?", (attempt_id, segment_id))
    return attempt_id


def operations(client, cluster_id):
    response = client.get(f"/api/v1/clusters/{cluster_id}/operations")
    assert response.status_code == 200, response.text
    return response.json()


def test_stepped_resource_scope_and_unknown_utilization(platform, online_nodes):
    client, pool = platform
    first, second = online_nodes
    pool.store.save_node(first)
    pool.store.save_node(second)
    create_cluster(client, "alpha", online_nodes)

    initial = operations(client, "alpha")
    assert initial["tasks"]["coverage"] == "available"
    assert initial["tasks"]["running_tasks"] == 0
    assert initial["load"]["values"]["cpu"] == 39.1

    def offline(node):
        return node.model_copy(update={
            "runtime_status": RuntimeStatus.OFFLINE,
            "operational_status": NodeOperationalStatus.OFFLINE,
        })

    pool.store.save_node(offline(pool.store.get_node(second.node_id)))
    partial = operations(client, "alpha")
    assert partial["load"]["values"]["cpu"] == 10
    detail = client.get("/api/v1/clusters/alpha").json()
    assert detail["node_count"] == 2
    assert detail["resource_summary"]["resources"]["cpu"]["total"] == 14

    pool.store.save_node(offline(pool.store.get_node(first.node_id)))
    assert operations(client, "alpha")["load"]["status"] == "unavailable"
    assert operations(client, "alpha")["load"]["values"]["cpu"] is None
    detail = client.get("/api/v1/clusters/alpha").json()
    assert detail["resource_summary"]["resources"]["cpu"]["total"] == 0

    stored_first = pool.store.get_node(first.node_id)
    stale = datetime.now(timezone.utc) - timedelta(hours=1)
    pool.store.save_node(stored_first.model_copy(update={
        "runtime_status": RuntimeStatus.ONLINE, "operational_status": NodeOperationalStatus.ONLINE,
        "last_resource_at": stale,
    }))
    assert operations(client, "alpha")["load"]["status"] == "stale"
    assert operations(client, "alpha")["load"]["values"]["cpu"] is None

    gpu = stored_first.resources.gpu.model_copy(update={
        "memory_architecture": "unified", "utilization_percent": None,
    })
    fresh_resources = stored_first.resources.model_copy(update={"gpu": gpu})
    pool.store.save_node(stored_first.model_copy(update={
        "runtime_status": RuntimeStatus.ONLINE, "operational_status": NodeOperationalStatus.ONLINE,
        "resources": fresh_resources,
    }))
    load = operations(client, "alpha")["load"]
    assert load["unified_memory_gpu_nodes"] == 1
    assert load["values"]["gpu"] is None
    assert load["values"]["video_memory"] is None
    assert load["values"]["memory"] == 25


def test_tasks_deduplicate_attempts_and_follow_dispatch_cluster_after_member_move(platform, online_nodes):
    client, pool = platform
    first, second = online_nodes
    pool.store.save_node(first)
    pool.store.save_node(second)
    create_cluster(client, "alpha", (first,))
    create_cluster(client, "beta", (second,))
    now = datetime.now(timezone.utc).timestamp()

    task("running-a", "RUNNING")
    attempt("running-a", 1, "RUNNING", first, "alpha")
    attempt("running-a", 2, "RUNNING", first, "alpha")
    attempt("running-a", 3, "WAITING_SESSION", first, "alpha", actual=False)
    task("queued-a", "RUNNING")
    attempt("queued-a", 1, "FAILED", first, "alpha", active=False)
    attempt("queued-a", 1, "DISPATCHING", first, "alpha", actual=False, retry=1)
    task("done-a", "SUCCEEDED", now - 60)
    attempt("done-a", 1, "FAILED", first, "alpha", active=False)
    attempt("done-a", 1, "SUCCEEDED", first, "alpha", retry=1)
    attempt("done-a", 2, "SUCCEEDED", first, "alpha")
    task("timed-a", "TIMED_OUT", now - 60)
    attempt("timed-a", 1, "TIMED_OUT", first, "alpha", actual=False)
    task("old-a", "SUCCEEDED", now - 90_000)
    attempt("old-a", 1, "SUCCEEDED", first, "alpha")
    task("running-b", "RUNNING")
    attempt("running-b", 1, "RUNNING", second, "beta")
    task("unassigned", "QUEUED")

    assert client.post(f"/api/v1/clusters/beta/nodes/{first.node_id}/move").status_code == 200
    alpha = operations(client, "alpha")["tasks"]
    beta = operations(client, "beta")["tasks"]
    assert alpha["running_tasks"] == beta["running_tasks"] == 1
    assert alpha["executing_nodes"] == beta["executing_nodes"] == 1
    assert alpha["waiting_tasks"] == 1
    assert alpha["waiting_shards"] == 2
    assert alpha["succeeded_tasks"] == alpha["failed_tasks"] == 1
    assert beta["succeeded_tasks"] == beta["failed_tasks"] == 0
    assert {row["id"] for row in client.get("/api/tasks?cluster_id=alpha").json()} == {
        "running-a", "queued-a", "done-a", "timed-a", "old-a",
    }
    assert {row["id"] for row in client.get("/api/tasks?cluster_id=beta").json()} == {"running-b"}
    assert len(client.get("/api/tasks").json()) == 7


def test_missing_historical_cluster_attribution_returns_unknown(platform, online_nodes):
    client, _ = platform
    create_cluster(client, "alpha")
    task("legacy", "RUNNING")
    attempt("legacy", 1, "RUNNING", online_nodes[0], None)
    stats = operations(client, "alpha")["tasks"]
    assert stats["coverage"] == "unavailable"
    assert stats["running_tasks"] is None
    assert client.get("/api/tasks?cluster_id=alpha").json() == []


def test_failed_cross_cluster_task_is_only_counted_where_work_failed(platform, online_nodes):
    client, pool = platform
    first, second = online_nodes
    for cluster_id, node in zip(("alpha", "beta"), online_nodes):
        pool.store.save_node(node)
        create_cluster(client, cluster_id, (node,))
    task("mixed-result", "FAILED", datetime.now(timezone.utc).timestamp() - 1)
    attempt("mixed-result", 1, "SUCCEEDED", first, "alpha")
    attempt("mixed-result", 2, "FAILED", second, "beta")
    db.execute("UPDATE segments SET status='SUCCEEDED' WHERE id='mixed-result-1'")
    db.execute("UPDATE segments SET status='FAILED' WHERE id='mixed-result-2'")
    assert operations(client, "alpha")["tasks"]["failed_tasks"] == 0
    assert operations(client, "beta")["tasks"]["failed_tasks"] == 1
    assert [item["id"] for item in client.get("/api/tasks?cluster_id=alpha").json()] == ["mixed-result"]


def test_cluster_audit_tracks_registration_operations_and_member_recovery(platform, online_nodes):
    client, pool = platform
    node = online_nodes[0]
    pool.store.save_node(node)
    create_cluster(client, "alpha", (node,))
    create_cluster(client, "beta")
    assert client.patch("/api/v1/clusters/alpha", json={"version": "2.0.0"}).status_code == 200
    assert client.delete("/api/v1/clusters/alpha").status_code == 409

    stored = pool.store.get_node(node.node_id)
    past = datetime.now(timezone.utc) - timedelta(hours=1)
    pool.store.save_node(stored.model_copy(update={"last_heartbeat_at": past}))
    pool.node_service.evaluate_heartbeat_timeouts()
    heartbeat = HeartbeatRequest(timestamp=datetime.now(timezone.utc), agent_version="test")
    pool.node_service.heartbeat(node.node_id, heartbeat)
    pool.node_service.heartbeat(node.node_id, heartbeat)
    assert client.post(f"/api/v1/clusters/beta/nodes/{node.node_id}/move").status_code == 200
    assert client.delete("/api/v1/clusters/alpha").status_code == 204

    audit = client.get("/api/v1/clusters/alpha/events?category=lifecycle").json()
    assert {event["event_type"] for event in audit} == {
        "REGISTRY_CREATED", "REGISTRY_VERSION_CHANGED", "MEMBER_JOINED",
        "MEMBER_MOVED_OUT", "REGISTRY_DELETED",
    }
    version = next(event for event in audit if event["event_type"] == "REGISTRY_VERSION_CHANGED")
    assert version["before"] == {"version": "0.1.0"}
    assert version["after"] == {"version": "2.0.0"}
    assert {event["result"] for event in audit if event["event_type"] == "REGISTRY_DELETED"} == {"success", "rejected"}
    recovery = client.get("/api/v1/clusters/alpha/events?category=recovery").json()
    assert {event["event_type"] for event in recovery} == {"NODE_OFFLINE", "RECONNECT_STARTED", "NODE_RECOVERED"}
    assert client.get("/api/v1/clusters/beta/events?category=recovery").json() == []
    assert client.get("/api/v1/clusters/beta/events?category=sync").json() == []


def test_recovery_projection_pairs_node_episodes_and_preserves_cluster(platform, online_nodes, monkeypatch):
    from app.pool.models.cluster import ClusterEvent
    from app.pool.services import cluster_service

    client, pool = platform
    node = online_nodes[0]
    pool.store.save_node(node)
    now = datetime.now(timezone.utc)
    with monkeypatch.context() as clock:
        clock.setattr(cluster_service, "utc_now", lambda: now - timedelta(hours=2))
        create_cluster(client, "alpha", (node,))
        create_cluster(client, "beta")
    for seconds, kind in [(55, "NODE_OFFLINE"), (54, "NODE_OFFLINE"), (40, "RECONNECT_STARTED"),
                          (39, "RECONNECT_STARTED"), (25, "NODE_RECOVERED")]:
        pool.store.add_cluster_event(ClusterEvent(
            cluster_id="alpha", category="recovery", event_type=kind, result="observed",
            target_id=str(node.node_id), target_name=node.node_name, occurred_at=now - timedelta(seconds=seconds),
        ))
    before = operations(client, "alpha")["recovery"]
    assert before["nodes"]["retained_memberships"] == 1
    assert before["nodes"]["scheduling_restored"] is None
    assert before["nodes"]["losses"] == 1
    assert before["nodes"]["reconnections"] == 1
    assert before["nodes"]["mean_recovery_seconds"] == 30
    assert before["nodes"]["online_members"] == 1

    task("proof", "RUNNING")
    proof = attempt("proof", 0, "RUNNING", node, "alpha")
    original = db.query("SELECT decision FROM attempts WHERE id=?", (proof,), one=True)
    decision = db.loads(original["decision"])
    decision["candidates"][1]["pool"].update(enforced=True, reasons=[])
    db.execute("UPDATE attempts SET decision=? WHERE id=?", (db.dumps(decision), proof))
    assert operations(client, "alpha")["recovery"]["nodes"]["scheduling_restored"] == 1
    assert operations(client, "alpha")["tasks"]["executing_members"] == 1
    assert operations(client, "alpha")["recovery"]["members"][str(node.node_id)]["current_shards"] == 1
    assert client.post(f"/api/v1/clusters/beta/nodes/{node.node_id}/move").status_code == 200
    assert operations(client, "alpha")["recovery"]["nodes"]["retained_memberships"] == 1
    assert operations(client, "beta")["recovery"]["nodes"]["retained_memberships"] == 0
    rows = client.get("/api/v1/clusters/alpha/recovery-events?category=node_recovery").json()
    assert {r["event_type"] for r in rows} >= {"NODE_OFFLINE", "MEMBERSHIP_RETAINED", "SCHEDULING_RESTORED"}
    assert [r["event_type"] for r in rows].count("RECONNECT_STARTED") == 1
    assert next(r for r in rows if r["duration_seconds"] is not None)["duration_seconds"] == 30
    assert client.get("/api/v1/clusters/unknown/recovery-events").status_code == 404


def test_recovery_tracks_wait_reconcile_and_redispatch_without_counting_stale_result(platform, online_nodes):
    client, pool = platform
    for cluster, node in zip(("alpha", "beta"), online_nodes, strict=True):
        pool.store.save_node(node)
        create_cluster(client, cluster, (node,))
    node, other = online_nodes
    task("affected", "RUNNING")
    old = attempt("affected", 0, "UNKNOWN", node, "alpha")
    recovering = attempt("affected", 1, "UNKNOWN", node, "alpha")
    stamp = datetime.now(timezone.utc).timestamp()
    db.execute("UPDATE attempts SET created=? WHERE id IN (?,?)", (stamp - 10, old, recovering))
    affected = [{"attempt_id": old}, {"attempt_id": recovering}, {"attempt_id": old}]
    db.execute("INSERT INTO node_incidents(id,node_id,detected_at,reason,affected) VALUES(?,?,?,?,?)",
               ("loss", "executor-" + node.node_name, stamp - 5, "fixture", db.dumps(affected)))
    stats = operations(client, "alpha")["recovery"]["tasks"]
    assert (stats["affected_tasks"], stats["affected_shards"], stats["waiting_shards"]) == (1, 2, 2)
    assert operations(client, "beta")["recovery"]["tasks"]["affected_shards"] == 0
    db.execute("UPDATE node_incidents SET link_restored_at=? WHERE id='loss'", (stamp,))
    assert operations(client, "alpha")["recovery"]["tasks"]["pending_reconciliation_shards"] == 2

    db.execute("UPDATE attempts SET status='TIMED_OUT',finished=?,result='{}' WHERE id=?", (stamp, old))
    newer = attempt("affected", 0, "RUNNING", other, "beta", retry=1)
    stats = operations(client, "alpha")["recovery"]["tasks"]
    assert (stats["redispatched_shards"], stats["confirmed_results"]) == (1, 0)
    assert stats["last_redispatch"]["attempt_id"] == newer
    assert operations(client, "beta")["recovery"]["tasks"]["redispatched_shards"] == 0
    db.execute("UPDATE attempts SET status='SUCCEEDED',finished=?,result='{}' WHERE id IN (?,?)",
               (stamp + .001, newer, recovering))
    db.execute("UPDATE segments SET status='SUCCEEDED',result='{}' WHERE task_id='affected'")
    affected[1]["reconciled_at"] = stamp
    db.execute("UPDATE node_incidents SET affected=? WHERE id='loss'", (db.dumps(affected),))
    stats = operations(client, "alpha")["recovery"]["tasks"]
    assert (stats["confirmed_results"], stats["pending_reconciliation_shards"]) == (2, 0)
    rows = client.get("/api/v1/clusters/alpha/recovery-events?category=sync").json()
    assert {r["attempt_id"] for r in rows if r["event_type"] == "RECOVERY_RESULT_CONFIRMED"} == {newer, recovering}
    assert operations(client, "beta")["recovery"]["tasks"]["confirmed_results"] == 0
    task_rows = client.get("/api/v1/clusters/alpha/recovery-events?category=task_recovery").json()
    assert sum(r["event_type"] == "SHARD_REDISPATCHED" for r in task_rows) == 1
    assert sum(r["event_type"] == "SHARD_RETRY_COMPLETED" for r in task_rows) == 1
    assert all(r["category"] == "task_recovery" for r in task_rows)


def test_node_deletion_records_only_its_current_cluster_and_preserves_membership_audit(platform, online_nodes):
    client, pool = platform
    node = online_nodes[0]
    pool.store.save_node(node)
    create_cluster(client, "alpha", (node,))
    create_cluster(client, "beta")
    target = str(node.node_id)
    assert client.post(f"/api/v1/clusters/beta/nodes/{target}/move").status_code == 200
    assert client.delete(f"/api/v1/nodes/{target}").status_code == 204
    assert client.delete(f"/api/v1/nodes/{target}").status_code == 404
    assert pool.store.list_events(node.node_id) == []

    source = client.get("/api/v1/clusters/alpha/recovery-events?category=lifecycle").json()
    destination = client.get("/api/v1/clusters/beta/recovery-events?category=lifecycle").json()
    assert {e["event_type"] for e in source} >= {"MEMBER_JOINED", "MEMBER_MOVED_OUT"}
    assert "MEMBER_RECORD_DELETED" not in {e["event_type"] for e in source}
    assert {e["event_type"] for e in destination} >= {"MEMBER_MOVED_IN", "MEMBER_RECORD_DELETED"}
    deleted = [e for e in destination if e["event_type"] == "MEMBER_RECORD_DELETED"]
    assert len(deleted) == 1
    assert deleted[0]["target_id"] == target
    assert deleted[0]["target_name"] == node.node_name
    assert deleted[0]["before"] == {"cluster_id": "beta"}
    assert deleted[0]["after"] == {"cluster_id": None}
    assert deleted[0]["status"] == "操作成功"
    legacy = client.get("/api/v1/clusters/beta/events?category=lifecycle").json()
    assert any(e["id"] == deleted[0]["id"] for e in legacy)


def test_node_deletion_rolls_back_when_cluster_audit_fails(platform, online_nodes, monkeypatch):
    client, pool = platform
    node = online_nodes[0]
    pool.store.save_node(node)
    create_cluster(client, "alpha", (node,))

    def fail_audit(_):
        raise RuntimeError("test audit failure")

    monkeypatch.setattr(pool.store, "_insert_cluster_events", fail_audit)
    with pytest.raises(RuntimeError, match="test audit failure"):
        pool.node_service.delete(node.node_id)
    assert pool.store.get_node(node.node_id).cluster_id == "alpha"
    assert pool.store.list_cluster_events("alpha", None, 20)[0].event_type != "MEMBER_RECORD_DELETED"
