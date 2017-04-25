#!/bin/bash -l

set -eox pipefail

basedir=/sys/fs/cgroup
options=rw,nosuid,nodev,noexec,relatime
groups="hugetlb freezer pids devices cpuset blkio net_prio,net_cls cpuacct,cpu memory perf_event"

my_link()
{
	ln -nfs $1 $basedir/$2
}

cleanup()
{
	ret=$?

	for mnt in $mounts; do
		umount $mnt || :
	done

	exit $ret
}

# mount cgroups
mount -t tmpfs tmpfs $basedir || cleanup
mounts="$basedir"

for group in $groups; do
	mount -t cgroup -o $options,$group cgroup $basedir/$group || cleanup
	mounts="$mounts $basedir/$group"
done

my_link net_prio,net_cls net_prio || cleanup
my_link net_prio,net_cls net_cls || cleanup
my_link cpuacct,cpu cpuacct || cleanup
my_link cpuacct,cpu cpu || cleanup

${CWDIR}/ic_gpdb.bash || cleanup

cleanup
