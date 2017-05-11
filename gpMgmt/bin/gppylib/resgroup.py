#!/usr/bin/env python
#
# Copyright (c) 2017, Pivotal Software Inc.

import sys

from gppylib.commands import base
from gppylib.commands.unix import *
from gppylib.commands.gp import *
from gppylib.gparray import GpArray
from gppylib.gplog import get_default_logger
from gppylib.gphostcache import *

cgroup_check_commands = '''
import os
cgroup_mount_location = "/sys/fs/cgroup/"
def validiate_permission(path, mode):
    try:
        open(os.path.join(cgroup_mount_location, path), mode)
    except Exception, e:
        exit("cgroup is not properly configured: %s expect permission: %s"%(str(e), mode))

if (not os.access(os.path.join(cgroup_mount_location, "cpu/gpdb"), os.F_OK)):
    exit("cgroup is not properly configured: directory %s does not exist"%os.path.join(cgroup_mount_location, "cpu/gpdb"))

if (not os.access(os.path.join(cgroup_mount_location, "cpu/gpdb"), os.R_OK|os.W_OK|os.X_OK)):
    exit("cgroup is not properly configured: directory %s Permission denied, expect permission: rwx"%os.path.join(cgroup_mount_location, "cpu/gpdb"))

if (not os.access(os.path.join(cgroup_mount_location, "cpuacct/gpdb"), os.F_OK)):
    exit("cgroup is not properly configured: directory %s does not exist"%os.path.join(cgroup_mount_location, "cpuacct/gpdb"))

if (not os.access(os.path.join(cgroup_mount_location, "cpuacct/gpdb"), os.R_OK|os.W_OK|os.X_OK)):
    exit("cgroup is not properly configured: directory %s Permission denied, expect permission: rwx"%os.path.join(cgroup_mount_location, "cpuacct/gpdb"))

validiate_permission("cpu/gpdb/cgroup.procs","w");
validiate_permission("cpu/gpdb/cpu.cfs_period_us","r");
validiate_permission("cpu/gpdb/cpu.cfs_quota_us","w");
validiate_permission("cpu/gpdb/cpu.shares","w");
validiate_permission("cpuacct/gpdb/cpuacct.usage","r");
'''

class resgroup(object):

    def __init__(self):
        self.logger = get_default_logger()

    def validate(self):
        if sys.platform.startswith('linux'):
            return self.validate_cgroup()
        else:
            return "resource group is not supported on this platform"

    def validate_cgroup(self):
        pool = base.WorkerPool()
        gp_array = GpArray.initFromCatalog(dbconn.DbURL(), utility=True)
        host_cache = GpHostCache(gp_array, pool)
        msg = None

        for h in host_cache.get_hosts():
            cmd = Command(h.hostname, "python -c '%s'"%cgroup_check_commands, REMOTE, h.hostname)
            pool.addCommand(cmd)
        pool.join()

        items = pool.getCompletedItems()
        failed = []
        for i in items:
            if not i.was_successful():
                failed.append("[%s:%s]"%(i.remoteHost, i.get_stderr()))
        pool.haltWork()
        pool.joinWorkers()
        if failed:
            msg = ",".join(failed)
        return msg

