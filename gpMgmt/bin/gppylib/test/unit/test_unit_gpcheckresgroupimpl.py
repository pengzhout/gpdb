#!/usr/bin/env python
#
# Copyright (c) 2017, Pivotal Software Inc.
#

import unittest2 as unittest
import os
import shutil
import tempfile
import subprocess

class GpCheckResGroupImpl(unittest.TestCase):
    cgroup_mntpnt = None
    cgroup_default_mntpnt = "/sys/fs/cgroup"
    cmd = 'gpcheckresgroupimpl'
    error_prefix = 'cgroup is not properly configured: '

    def setUp(self):
        self.cgroup_mntpnt = tempfile.mkdtemp(prefix='fake-cgroup-mnt-')
        self.cmdline = [ self.cmd, self.cgroup_mntpnt ]

        os.mkdir(os.path.join(self.cgroup_mntpnt, "cpu"), 0755)
        os.mkdir(os.path.join(self.cgroup_mntpnt, "cpuacct"), 0755)

    def tearDown(self):
        shutil.rmtree(self.cgroup_mntpnt)

    def build_error(self, message):
        return self.error_prefix + message + "\n"

    def run_cmd(self):
        p = subprocess.Popen(self.cmdline, stderr=subprocess.PIPE)
        _, output = p.communicate()
        return output.replace(self.cgroup_mntpnt, self.cgroup_default_mntpnt)

    def touch(self, path, mode):
        with open(path, "w"):
            pass
        os.chmod(path, mode)

    def test_check_all_permission(self):
        self.assertEqual(self.run_cmd(), self.build_error("directory '/sys/fs/cgroup/cpu/gpdb/' does not exist"))
        os.mkdir(os.path.join(self.cgroup_mntpnt, "cpu", "gpdb"), 0500)

        self.assertEqual(self.run_cmd(), self.build_error("directory '/sys/fs/cgroup/cpu/gpdb/' permission denied: require permission 'rwx'"))
        os.chmod(os.path.join(self.cgroup_mntpnt, "cpu", "gpdb"), 0700)

        self.assertEqual(self.run_cmd(), self.build_error("file '/sys/fs/cgroup/cpu/gpdb/cgroup.procs' does not exist"))
        self.touch(os.path.join(self.cgroup_mntpnt, "cpu", "gpdb", "cgroup.procs"), 0100)

        self.assertEqual(self.run_cmd(), self.build_error("file '/sys/fs/cgroup/cpu/gpdb/cgroup.procs' permission denied: require permission 'rw'"))
        os.chmod(os.path.join(self.cgroup_mntpnt, "cpu", "gpdb", "cgroup.procs"), 0600)

        self.assertEqual(self.run_cmd(), self.build_error("file '/sys/fs/cgroup/cpu/gpdb/cpu.cfs_period_us' does not exist"))
        self.touch(os.path.join(self.cgroup_mntpnt, "cpu", "gpdb", "cpu.cfs_period_us"), 0100)

        self.assertEqual(self.run_cmd(), self.build_error("file '/sys/fs/cgroup/cpu/gpdb/cpu.cfs_period_us' permission denied: require permission 'rw'"))
        os.chmod(os.path.join(self.cgroup_mntpnt, "cpu", "gpdb", "cpu.cfs_period_us"), 0600)

        self.assertEqual(self.run_cmd(), self.build_error("file '/sys/fs/cgroup/cpu/gpdb/cpu.cfs_quota_us' does not exist"))
        self.touch(os.path.join(self.cgroup_mntpnt, "cpu", "gpdb", "cpu.cfs_quota_us"), 0100)

        self.assertEqual(self.run_cmd(), self.build_error("file '/sys/fs/cgroup/cpu/gpdb/cpu.cfs_quota_us' permission denied: require permission 'rw'"))
        os.chmod(os.path.join(self.cgroup_mntpnt, "cpu", "gpdb", "cpu.cfs_quota_us"), 0600)

        self.assertEqual(self.run_cmd(), self.build_error("file '/sys/fs/cgroup/cpu/gpdb/cpu.shares' does not exist"))
        self.touch(os.path.join(self.cgroup_mntpnt, "cpu", "gpdb", "cpu.shares"), 0100)

        self.assertEqual(self.run_cmd(), self.build_error("file '/sys/fs/cgroup/cpu/gpdb/cpu.shares' permission denied: require permission 'rw'"))
        os.chmod(os.path.join(self.cgroup_mntpnt, "cpu", "gpdb", "cpu.shares"), 0600)

        self.assertEqual(self.run_cmd(), self.build_error("directory '/sys/fs/cgroup/cpuacct/gpdb/' does not exist"))
        os.mkdir(os.path.join(self.cgroup_mntpnt, "cpuacct", "gpdb"), 0500)

        self.assertEqual(self.run_cmd(), self.build_error("directory '/sys/fs/cgroup/cpuacct/gpdb/' permission denied: require permission 'rwx'"))
        os.chmod(os.path.join(self.cgroup_mntpnt, "cpuacct", "gpdb"), 0700)

        self.assertEqual(self.run_cmd(), self.build_error("file '/sys/fs/cgroup/cpuacct/gpdb/cpuacct.usage' does not exist"))
        self.touch(os.path.join(self.cgroup_mntpnt, "cpuacct", "gpdb", "cpuacct.usage"), 0100)

        self.assertEqual(self.run_cmd(), self.build_error("file '/sys/fs/cgroup/cpuacct/gpdb/cpuacct.usage' permission denied: require permission 'r'"))
        os.chmod(os.path.join(self.cgroup_mntpnt, "cpuacct", "gpdb", "cpuacct.usage"), 0400)

        self.assertEqual(self.run_cmd(), self.build_error("file '/sys/fs/cgroup/cpuacct/gpdb/cpuacct.stat' does not exist"))
        self.touch(os.path.join(self.cgroup_mntpnt, "cpuacct", "gpdb", "cpuacct.stat"), 0100)

        self.assertEqual(self.run_cmd(), self.build_error("file '/sys/fs/cgroup/cpuacct/gpdb/cpuacct.stat' permission denied: require permission 'r'"))
        os.chmod(os.path.join(self.cgroup_mntpnt, "cpuacct", "gpdb", "cpuacct.stat"), 0400)

        self.assertEqual(self.run_cmd(), "")

if __name__ == '__main__':
    unittest.main()
