#!/usr/bin/env python
"""Rebase operational segment paths after copying a stopped platform's data directory."""
import argparse, json, shutil, sqlite3, time
from pathlib import Path

def migrate(data, old_data):
    data=Path(data).resolve();old_data=Path(old_data)
    database=data/'control.sqlite'
    if not database.is_file():raise ValueError('Copied control.sqlite is missing')
    connection=sqlite3.connect(database)
    try:
        connection.execute('BEGIN IMMEDIATE')
        terminal=('SUCCEEDED','FAILED','CANCELED','TIMED_OUT')
        if connection.execute('SELECT count(*) FROM tasks WHERE status NOT IN (?,?,?,?)',terminal).fetchone()[0]:
            raise ValueError('Stop active tasks before migration')
        if connection.execute("SELECT count(*) FROM execution_sessions WHERE cleanup_state!='DONE'").fetchone()[0]:
            raise ValueError('Execution sessions have not been cleaned')
        updates=[]
        for sid,path in connection.execute('SELECT id,path FROM segments'):
            original=Path(path)
            try:relative=original.relative_to(old_data)
            except ValueError:
                try:relative=original.relative_to(data)
                except ValueError:raise ValueError('Segment path is outside the known data roots: '+str(original))
            target=data/relative
            if not target.is_file():raise ValueError('Referenced segment is missing: '+str(target))
            if str(target)!=path:updates.append((str(target),sid))
        backup=data/'backups'/('before-mac-migration-'+str(time.time_ns())+'.sqlite')
        backup.parent.mkdir(exist_ok=True)
        # The source is stopped and has no active WAL writers during the deployment.
        snapshot=sqlite3.connect(backup)
        with sqlite3.connect(database) as reader:reader.backup(snapshot)
        snapshot.close()
        connection.executemany('UPDATE segments SET path=? WHERE id=?',updates)
        connection.commit()
        assert connection.execute('PRAGMA integrity_check').fetchone()[0]=='ok'
        report={'at':time.time(),'old_data':str(old_data),'new_data':str(data),'segment_paths_rebased':len(updates),
                'backup':str(backup),'tasks':connection.execute('SELECT count(*) FROM tasks').fetchone()[0],
                'integrity':'ok'}
        (data/'migration-report.json').write_text(json.dumps(report,ensure_ascii=False,indent=2)+'\n')
        return report
    finally:connection.close()

if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('data');parser.add_argument('old_data')
    args=parser.parse_args();print(json.dumps(migrate(args.data,args.old_data),ensure_ascii=False))
