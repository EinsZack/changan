import importlib.util, sqlite3
from pathlib import Path
import pytest

spec=importlib.util.spec_from_file_location('native_migration',Path(__file__).parents[1]/'scripts/migrate_platform_data.py')
module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)

def database(tmp_path,status='SUCCEEDED',present=True):
    data=tmp_path/'copied';data.mkdir()
    with sqlite3.connect(data/'control.sqlite') as c:
        c.executescript('CREATE TABLE tasks(id TEXT,status TEXT,params TEXT);CREATE TABLE segments(id TEXT,path TEXT);CREATE TABLE execution_sessions(cleanup_state TEXT);')
        c.execute('INSERT INTO tasks VALUES(?,?,?)',('historical',status,'{"frozen":"original"}'))
        c.execute('INSERT INTO segments VALUES(?,?)',('seg','/old/data/tasks/historical/segment.mp4'))
    if present:
        p=data/'tasks/historical/segment.mp4';p.parent.mkdir(parents=True);p.write_bytes(b'video')
    return data

def test_rebases_operational_paths_and_keeps_history(tmp_path):
    data=database(tmp_path)
    report=module.migrate(data,'/old/data')
    with sqlite3.connect(data/'control.sqlite') as c:
        assert c.execute('SELECT params FROM tasks').fetchone()[0]=='{"frozen":"original"}'
        assert c.execute('SELECT path FROM segments').fetchone()[0]==str(data/'tasks/historical/segment.mp4')
    with sqlite3.connect(report['backup']) as c:
        assert c.execute('SELECT path FROM segments').fetchone()[0]=='/old/data/tasks/historical/segment.mp4'
    assert module.migrate(data,'/old/data')['segment_paths_rebased']==0

def test_missing_artifact_rolls_back(tmp_path):
    data=database(tmp_path,present=False)
    with pytest.raises(ValueError,match='missing'):module.migrate(data,'/old/data')
    with sqlite3.connect(data/'control.sqlite') as c:
        assert c.execute('SELECT path FROM segments').fetchone()[0]=='/old/data/tasks/historical/segment.mp4'

def test_active_task_blocks_migration(tmp_path):
    data=database(tmp_path,status='RUNNING')
    with pytest.raises(ValueError,match='active tasks'):module.migrate(data,'/old/data')
