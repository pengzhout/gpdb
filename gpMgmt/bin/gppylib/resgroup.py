#!/usr/bin/env python
#
# Copyright (c) GPDB 2017. All Rights Reserved.

from gppylib.commands import base
from gppylib.commands.unix import *
from gppylib.commands.gp import *
from gppylib.gparray import GpArray
from gppylib.gplog import get_default_logger
from gppylib.gphostcache import *

class resgroup(object):

    def __init__(self):
        self.logger = get_default_logger()

    def validate(self):
        import platform
        system = platform.system()
        if system == 'Linux':
            return self.validate_cgroup()
        else:
            self.logger.warn('resgroup is enabled without cpu rate limitation function')
            return None

    def validate_cgroup(self):
        cgroup_check_commands = '''
        test -d /sys/fs/cgroup/cpu/gpdb &&
        test -r /sys/fs/cgroup/cpu/gpdb &&
        test -w /sys/fs/cgroup/cpu/gpdb &&
        test -x /sys/fs/cgroup/cpu/gpdb &&
        test -w /sys/fs/cgroup/cpu/gpdb/cgroup.procs &&
        test -r /sys/fs/cgroup/cpu/gpdb/cpu.cfs_period_us &&
        test -w /sys/fs/cgroup/cpu/gpdb/cpu.cfs_quota_us &&
        test -w /sys/fs/cgroup/cpu/gpdb/cpu.shares &&
        test -d /sys/fs/cgroup/cpuacct/gpdb &&
        test -r /sys/fs/cgroup/cpuacct/gpdb &&
        test -w /sys/fs/cgroup/cpuacct/gpdb &&
        test -x /sys/fs/cgroup/cpuacct/gpdb &&
        test -r /sys/fs/cgroup/cpuacct/gpdb/cpuacct.usage &&
        test -r /sys/fs/cgroup/cpuacct/gpdb/cpuacct.stat &&
        true
        '''
        pool = base.WorkerPool()
        gp_array = GpArray.initFromCatalog(dbconn.DbURL(), utility=True)
        host_cache = GpHostCache(gp_array, pool)
        msg = None

        for h in host_cache.get_hosts():
            cmd = Command(h.hostname, cgroup_check_commands, REMOTE, h.hostname)
            pool.addCommand(cmd)
        pool.join()

        items = pool.getCompletedItems()
        failed = []
        for i in items:
            if not i.was_successful():
                failed.append(i.remoteHost)
        pool.haltWork()
        pool.joinWorkers()
        if failed:
            msg = 'cgroup is not properly configured on segments: ' + ", ".join(failed)
        return msg

