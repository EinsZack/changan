from datetime import datetime, timezone
from uuid import uuid4

from app import db
from test_cluster_operations import attempt, platform, task
from test_resource_aggregation import online_nodes


def test_task_summary_counts_current_work_by_physical_node(platform, online_nodes):
    client, pool = platform
    first, second = online_nodes
    for node in online_nodes:
        pool.store.save_node(node)

    task("active", "RUNNING")
    old = attempt("active", 0, "RUNNING", first, "old-cluster")
    attempt("active", 0, "UPLOADING", second, "new-cluster", retry=1)
    attempt("active", 1, "WAITING_SESSION", first, "old-cluster", actual=False)
    attempt("active", 2, "UNKNOWN", second, "new-cluster")
    # Repeated candidate metadata still describes just one execution attempt.
    decision = db.query("SELECT decision FROM attempts WHERE id=?", (old,), one=True)
    duplicate = db.loads(decision["decision"])
    attempt_id = "active-1-0"
    wait_decision = db.loads(db.query("SELECT decision FROM attempts WHERE id=?", (attempt_id,), one=True)["decision"])
    wait_decision["candidates"].append(duplicate["candidates"][-1])
    db.execute("UPDATE attempts SET decision=? WHERE id=?", (db.dumps(wait_decision), attempt_id))

    first_url = f"/api/v1/platform/nodes/{first.node_id}/task-summary"
    second_url = f"/api/v1/platform/nodes/{second.node_id}/task-summary"
    one = client.get(first_url).json()
    two = client.get(second_url).json()
    assert (one["active_task_count"], one["waiting_shards"], one["running_shards"], one["unverified_shards"]) == (1, 1, 0, 0)
    assert (two["active_task_count"], two["waiting_shards"], two["running_shards"], two["unverified_shards"]) == (1, 0, 1, 1)
    assert two["tasks"][0]["name"] == "active"
    assert client.get(f"/api/v1/platform/nodes/{uuid4()}/task-summary").status_code == 404


def test_task_summary_uses_live_binding_only_for_legacy_active_work(platform, online_nodes):
    client, pool = platform
    node, other = online_nodes
    machine = "a" * 64
    node.identity.machine_id_sha256 = machine
    pool.store.save_node(node)
    pool.store.save_node(other)
    task("active", "RUNNING")
    legacy = attempt("active", 0, "RUNNING", node, "old", actual=True)
    attempt("active", 1, "RUNNING", other, "old", actual=True)
    db.execute("UPDATE attempts SET decision='{}' WHERE id=?", (legacy,))
    db.execute("INSERT INTO nodes(id,endpoint,cluster_id,kind,snapshot) VALUES(?,?,?,?,?)",
               ("executor-" + node.node_name, "local", "old", "test", db.dumps({"machine_id_sha256": machine})))

    task("last", "SUCCEEDED")
    valid = attempt("last", 0, "SUCCEEDED", node, "old", actual=True)
    unbound = attempt("last", 1, "SUCCEEDED", node, "old", actual=True)
    expired = attempt("last", 2, "SUCCEEDED", node, "old", active=False)
    now = datetime.now(timezone.utc).timestamp()
    db.execute("UPDATE segments SET status='SUCCEEDED',result='{}' WHERE task_id='last'")
    for aid, finished in [(valid, now-20), (unbound, now-10), (expired, now)]:
        db.execute("UPDATE attempts SET finished=?,result='{}' WHERE id=?", (finished, aid))
    db.execute("UPDATE attempts SET decision='{}' WHERE id=?", (unbound,))

    response = client.get(f"/api/v1/platform/nodes/{node.node_id}/task-summary")
    assert response.status_code == 200
    summary = response.json()
    assert summary["execution_bound"] is True
    assert summary["running_shards"] == 1
    assert summary["recent_result"]["task_id"] == "last"
    assert datetime.fromisoformat(summary["recent_result"]["finished_at"]).timestamp() == now-20


def test_node_counts_deduplicate_shards_and_only_credit_the_finishing_node(platform, online_nodes):
    client, pool = platform
    first, second = online_nodes
    for node in online_nodes:
        pool.store.save_node(node)

    def counts(node):
        summary = client.get(f"/api/v1/platform/nodes/{node.node_id}/task-summary").json()
        return (summary["assigned_task_count"], summary["completed_task_count"], summary["active_task_count"])

    assert counts(first) == (0, 0, 0)
    task("shared", "RUNNING")
    task("retried", "RUNNING")
    attempt("shared", 0, "SUCCEEDED", first, "one")
    attempt("shared", 1, "SUCCEEDED", first, "one")
    attempt("shared", 2, "RUNNING", second, "two")
    attempt("retried", 0, "FAILED", first, "one")
    attempt("retried", 0, "SUCCEEDED", second, "two", retry=1)
    db.execute("UPDATE segments SET status='SUCCEEDED',result='{}' WHERE ordinal < 2")
    assert counts(first) == (2, 1, 0)
    assert counts(second) == (2, 1, 1)
