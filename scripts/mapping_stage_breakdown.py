"""Break down completed mapping tasks using a read-only control DB or its backup.

Run on Mac: python scripts/mapping_stage_breakdown.py control.sqlite task_... > stages.json
All times are seconds. The stage walls partition task wall time and must not be
added to per-Job spans or summed worker measures.
"""

import argparse
from collections import Counter
from contextlib import closing
import hashlib
import json
from pathlib import Path
import sqlite3
import sys

ODX_STAGES = (
    'dataset', 'split', 'merge', 'opensfm', 'openmvs', 'odm_filterpoints',
    'odm_meshing', 'mvs_texturing', 'odm_georeferencing', 'odm_dem',
    'odm_orthophoto', 'odm_report', 'odm_postprocess',
)
COLLAB_STAGES = ('prepare', 'reconstruct', 'align', 'toolchain', 'merge')


def elapsed(start, end):
    if start is None or end is None or end < start:
        return None
    return end - start


def get_marks(events, phase, has_local_alignment):
    marks = {}
    odx = {}
    for event in events:
        at = event['at']
        payload = json.loads(event['payload'])
        if event['type'] == 'dispatch_ack':
            marks.setdefault('ack', at)
        elif event['type'] == 'container_cleaned':
            marks['cleaned'] = at
        elif event['type'] == 'mapping_stage':
            stage, state = payload.get('stage'), payload.get('state')
            if stage == 'download' and state == 'RUNNING':
                marks.setdefault('download', at)
            elif stage == 'upload' and state == 'UPLOADING':
                marks.setdefault('upload', at)
            elif state == 'FINALIZING':
                marks.setdefault('final', at)
            elif has_local_alignment and stage == 'align' and state == 'RUNNING':
                marks.setdefault('local_align', at)
            if stage in ODX_STAGES:
                span = odx.setdefault(stage, {})
                if state == 'RUNNING':
                    span.setdefault('start', at)
                elif state == 'SUCCEEDED':
                    span['end'] = at
            if stage == phase:
                if state == 'RUNNING':
                    marks.setdefault('compute', at)
                elif state == 'SUCCEEDED':
                    marks['compute_end'] = at
    if phase == 'single':
        marks['compute'] = odx.get('dataset', {}).get('start')
        marks['compute_end'] = max((r['end'] for r in odx.values() if 'end' in r), default=None)
    # On the merge Job the worker's outer phase and ODX's inner step have the
    # same label. Both fall inside this single compute envelope.
    odx_durations = {name: elapsed(span.get('start'), span.get('end')) for name, span in odx.items()}
    return marks, {name: value for name, value in odx_durations.items() if value is not None}


def load_units(c, task):
    units = []
    for a in c.execute(
        "SELECT a.*,s.active_attempt,m.spec FROM attempts a "
        "JOIN segments s ON s.id=a.segment_id LEFT JOIN mapping_units m ON m.unit_id=s.id "
        "WHERE a.task_id=? ORDER BY a.created,a.id", (task['id'],),
    ):
        spec = json.loads(a['spec']) if a['spec'] else {'phase': 'single', 'submodel': None}
        result = json.loads(a['result']) if a['result'] else {}
        metrics = result.get('metrics', {})
        phase = spec['phase']
        events = c.execute("SELECT at,type,payload FROM events WHERE attempt_id=? ORDER BY id", (a['id'],))
        marks, odx = get_marks(events, phase, 'align_seconds' in metrics)
        marks['complete'] = a['finished']
        fields = {
            'submit_to_ack': elapsed(a['created'], marks.get('ack')),
            'download_extract': elapsed(marks.get('download'), marks.get('local_align', marks.get('compute'))),
            'local_align': elapsed(marks.get('local_align'), marks.get('compute')),
            'compute': elapsed(marks.get('compute'), marks.get('compute_end')),
            'pack': elapsed(marks.get('compute_end'), marks.get('upload')),
            'upload': elapsed(marks.get('upload'), marks.get('final')),
            'confirm': elapsed(marks.get('final'), marks.get('complete')),
            'cleanup': elapsed(marks.get('complete'), marks.get('cleaned')),
        }
        units.append({
            'id': a['segment_id'], 'attempt': a['id'], 'active_attempt': a['active_attempt'] == a['id'],
            'node': a['actual_node'], 'target_node': a['node_id'], 'phase': phase,
            'submodel': spec.get('submodel'), 'dependencies': spec.get('dependencies', []),
            'status': a['status'], 'error': a['error'], 'cleanup_state': a['cleanup_state'],
            'created': a['created'] - task['created'],
            'finished': a['finished'] - task['created'] if a['finished'] is not None else None,
            'metrics': metrics, 'odx': odx, 'seg': fields, 'alignment_sha256': result.get('alignment_sha256'),
            'marks': {k: v - task['created'] for k, v in marks.items() if v is not None},
        })
    return units


def compute_concurrency(units):
    windows = [(u['marks'].get('compute'), u['marks'].get('compute_end'), u['node']) for u in units]
    valid = [(a, b, n) for a, b, n in windows if n and a is not None and b is not None and b > a]
    boundaries = sorted({time for a, b, _ in valid for time in (a, b)})
    durations = Counter()
    for start, end in zip(boundaries, boundaries[1:]):
        active = {n for a, b, n in valid if a <= start < b}
        if active:
            durations[len(active)] += end - start
    return dict(sorted(durations.items()))


def stage_windows(wall, phases, units):
    ordered = []
    for phase in phases:
        attempts = [u for u in units if u['phase'] == phase]
        completed = [u for u in attempts if u['active_attempt'] and u['status'] == 'SUCCEEDED']
        if completed:
            ordered.append((phase, completed, min(u['created'] for u in attempts)))
    rows = []
    for idx, (phase, us, first) in enumerate(ordered):
        end = ordered[idx+1][2] if idx+1 < len(ordered) else wall
        start = first if idx else 0.0
        last = max(us, key=lambda u: u['finished'])
        part = {k: v for k, v in last['seg'].items() if k != 'cleanup' and v is not None}
        concurrency = compute_concurrency(us)
        rows.append({
            'phase': phase, 'start': start, 'first_submission': first, 'end': end, 'wall_seconds': end-start,
            'jobs': len(us), 'nodes': dict(Counter(u['node'] for u in us)),
            'peak_compute_nodes': max(concurrency, default=0), 'compute_seconds_by_node_count': concurrency,
            'last_finished_node': last['node'], 'last_finished_submodel': last['submodel'],
            'last_finished_job': part, 'other_wall_seconds': end-start-sum(part.values()),
            'after_last_job_seconds': elapsed(last['finished'], end),
            'download_bytes': sum(u['metrics'].get('download_bytes', 0) for u in us) if phase != 'single' else None,
            'upload_bytes': sum(u['metrics'].get('upload_bytes', 0) for u in us) if phase != 'single' else None,
            'cache_hit_bytes': sum(u['metrics'].get('cache_hit_bytes', 0) for u in us),
        })
    return rows


def analyze(c, tid):
    task = c.execute("SELECT * FROM tasks WHERE id=?", (tid,)).fetchone()
    if task is None:
        raise ValueError(f'任务不存在 {tid}')
    params, meta = json.loads(task['params']), json.loads(task['metadata'])
    mode = params.get('execution_mode', 'single')
    wall = (task['finished'] or task['updated']) - task['created']
    units = load_units(c, task)
    groups = meta.get('mapping_plan', {}).get('submodels', [])
    canonical_groups = sorted(sorted(group['images']) for group in groups)
    plan_sha = hashlib.sha256(json.dumps(canonical_groups).encode()).hexdigest() if groups else None
    return {
        'task': tid, 'name': task['name'], 'status': task['status'], 'mode': mode,
        'preset': params['preset'], 'source': params['source'], 'input_sha256': meta.get('input', {}).get('sha256'),
        'image': meta.get('application', {}).get('image'), 'version': meta.get('application', {}).get('pipeline_version'),
        'submodel_size': params.get('submodel_size'), 'overlap': params.get('split_overlap_m'),
        'max_nodes': params.get('max_nodes'), 'concurrency': params.get('concurrency'), 'elapsed': wall,
        'plan': [len(g['images']) for g in groups], 'group_plan_sha256': plan_sha,
        'stages': stage_windows(wall, COLLAB_STAGES if mode == 'collaborative' else ('single',), units)
        if task['status'] == 'SUCCEEDED' else [],
        'units': units,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('db', type=Path)
    parser.add_argument('tasks', nargs='+')
    args = parser.parse_args()
    with closing(sqlite3.connect(args.db.resolve().as_uri()+'?mode=ro', uri=True)) as conn:
        conn.row_factory = sqlite3.Row
        conn.execute('BEGIN')
        json.dump([analyze(conn, tid) for tid in args.tasks], sys.stdout, ensure_ascii=False, indent=2)
        print()


if __name__ == '__main__':
    main()
