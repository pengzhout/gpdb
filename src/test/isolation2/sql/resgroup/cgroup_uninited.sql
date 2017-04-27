-- resgroup is enabled but cgroup is not properly setup yet

-- start_ignore
! gpconfig -r gp_resource_manager;
! gpstop -rai;
! rmdir /sys/fs/cgroup/cpu/gpdb/*/;
! rmdir /sys/fs/cgroup/cpuacct/gpdb/*/;
-- to make vim happy */
! rmdir /sys/fs/cgroup/cpu/gpdb;
! rmdir /sys/fs/cgroup/cpuacct/gpdb;
-- end_ignore

-- gpdb cgroup is missing
! gpconfig -c gp_resource_manager -v group;

-- start_ignore
! mkdir /sys/fs/cgroup/cpu/gpdb;
! mkdir /sys/fs/cgroup/cpuacct/gpdb;
! chmod 644 /sys/fs/cgroup/cpu/gpdb;
! chmod 644 /sys/fs/cgroup/cpuacct/gpdb;
-- end_ignore

-- no enough permission on gpdb cgroup
! gpconfig -c gp_resource_manager -v group;

-- start_ignore
! chmod 755 /sys/fs/cgroup/cpu/gpdb;
! chmod 755 /sys/fs/cgroup/cpuacct/gpdb;
! chmod 444 /sys/fs/cgroup/cpu/gpdb/cgroup.procs;
-- end_ignore

-- no enough permission on gpdb cgroup's config files
! gpconfig -c gp_resource_manager -v group;

-- start_ignore
! rmdir /sys/fs/cgroup/cpu/gpdb;
! rmdir /sys/fs/cgroup/cpuacct/gpdb;
! gpconfig -r gp_resource_manager;
-- end_ignore

