#!/usr/bin/env python
"""One-time cgroup delegation for this privileged SSH development container only.
Run before starting Docker. Does not apply to THOR or ordinary systemd hosts.
"""
from pathlib import Path
p=Path('/sys/fs/cgroup')
if 'memory' in (p/'cgroup.subtree_control').read_text().split():
    print('Memory controller already delegated');raise SystemExit(0)
assert Path('/proc/1/comm').read_text().strip()=='sshd','Only this SSH development container is supported'
assert Path('/proc/self/cgroup').read_text().strip()=='0::/','Requires a delegated cgroup namespace'
assert not [x for x in p.iterdir() if x.is_dir()],'Stop project Docker first; inspect existing cgroups, never delete populated groups'
enabled=(p/'cgroup.subtree_control').read_text().split()
(p/'cgroup.subtree_control').write_text(' '.join('-'+x for x in enabled))
leaf=p/'changan-host-processes';leaf.mkdir(exist_ok=True)
for _ in range(5):
    pids=(p/'cgroup.procs').read_text().split()
    if not pids:break
    for pid in pids:
        try:(leaf/'cgroup.procs').write_text(pid)
        except ProcessLookupError:pass
assert not (p/'cgroup.procs').read_text().split()
(p/'cgroup.subtree_control').write_text('+cpu +cpuset +io +memory +pids')
print('Delegated this development container cgroup; no host limits changed')
