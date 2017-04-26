/*-------------------------------------------------------------------------
 *
 * cgroup.c
 *	  CGroup based cpu resource group implementation.
 *
 *
 * Copyright (c) 2006-2017, Greenplum inc.
 *
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "postmaster/backoff.h"
#include "cgroup.h"

#ifdef __linux__

/* cgroup is only available on linux */

#include <fcntl.h>
#include <unistd.h>
#include <sched.h>
#include <sys/file.h>
#include <sys/stat.h>
#include <sys/types.h>

static char * buildPath(Oid group, const char *comp, const char *prop, char *path, size_t pathsize);
static int getCpuCores(void);
static size_t readData(Oid group, const char *comp, const char *prop, char *data, size_t datasize);
static void writeData(Oid group, const char *comp, const char *prop, char *data, size_t datasize);
static int64 readInt64(Oid group, const char *comp, const char *prop);
static void writeInt64(Oid group, const char *comp, const char *prop, int64 x);

static char *
buildPath(Oid group,
		  const char *comp,
		  const char *prop,
		  char *path,
		  size_t pathsize)
{
	if (group)
		snprintf(path, pathsize, "/sys/fs/cgroup/%s/gpdb/%d/%s", comp, group, prop);
	else
		snprintf(path, pathsize, "/sys/fs/cgroup/%s/gpdb/%s", comp, prop);

	return path;
}

static int
getCpuCores(void)
{
	cpu_set_t set;

	if (sched_getaffinity (0, sizeof (set), &set) == 0)
	{
		unsigned long count;

# ifdef CPU_COUNT
		/* glibc >= 2.6 has the CPU_COUNT macro.  */
		count = CPU_COUNT (&set);
# else
		size_t i;

		count = 0;
		for (i = 0; i < CPU_SETSIZE; i++)
			if (CPU_ISSET (i, &set))
				count++;
# endif
		if (count > 0)
			return count;
	}

	return -1;
}

static size_t
readData(Oid group, const char *comp, const char *prop, char *data, size_t datasize)
{
	char path[1024];
	size_t pathsize = sizeof(path);

	buildPath(group, comp, prop, path, pathsize);

	int fd = open(path, O_RDONLY);
	if (fd < 0)
		elog(ERROR, "cgroup: can't open file '%s': %s", path, strerror(errno));

	size_t ret = read(fd, data, datasize);
	close(fd);

	if (ret < 0)
		elog(ERROR, "cgroup: can't read data from file '%s': %s", path, strerror(errno));

	return ret;
}

static void
writeData(Oid group, const char *comp, const char *prop, char *data, size_t datasize)
{
	char path[1024];
	size_t pathsize = sizeof(path);

	buildPath(group, comp, prop, path, pathsize);

	int fd = open(path, O_WRONLY);
	if (fd < 0)
		elog(LOG, "cgroup: can't open file '%s': %s", path, strerror(errno));

	size_t ret = write(fd, data, datasize);
	close(fd);

	if (ret < 0)
		elog(LOG, "cgroup: can't write data to file '%s': %s", path, strerror(errno));
	if (ret != datasize)
		elog(LOG, "cgroup: can't write all data to file '%s'", path);
}

static int64
readInt64(Oid group, const char *comp, const char *prop)
{
	int64 x;
	char data[64];
	size_t datasize = sizeof(data);

	readData(group, comp, prop, data, datasize);

	if (sscanf(data, "%lld", (long long *) &x) != 1)
		elog(ERROR, "invalid number '%s'", data);

	return x;
}

static void
writeInt64(Oid group, const char *comp, const char *prop, int64 x)
{
	char data[64];
	size_t datasize = sizeof(data);

	snprintf(data, datasize, "%lld", (long long) x);

	writeData(group, comp, prop, data, strlen(data));
}

void
CGroupCheckPermission(Oid group)
{
	char path[1024];
	size_t pathsize = sizeof(path);
	const char *comp = "cpu";

	if (access(buildPath(group, comp, "", path, pathsize), R_OK | W_OK | X_OK))
		elog(ERROR, "can't access directory '%s': %s", path, strerror(errno));

	if (access(buildPath(group, comp, "cgroup.procs", path, pathsize), R_OK | W_OK))
		elog(ERROR, "can't access file '%s': %s", path, strerror(errno));
	if (access(buildPath(group, comp, "cpu.cfs_period_us", path, pathsize), R_OK | W_OK))
		elog(ERROR, "can't access file '%s': %s", path, strerror(errno));
	if (access(buildPath(group, comp, "cpu.cfs_quota_us", path, pathsize), R_OK | W_OK))
		elog(ERROR, "can't access file '%s': %s", path, strerror(errno));
	if (access(buildPath(group, comp, "cpu.shares", path, pathsize), R_OK | W_OK))
		elog(ERROR, "can't access file '%s': %s", path, strerror(errno));
}

void
CGroupInitTop(void)
{
	/* cfs_quota_us := cfs_period_us * ncores * gp_resource_group_cpu_limit */
	/* shares := 1024 * ncores */

	int64 cfs_period_us;
	int ncores = getCpuCores();
	const char *comp = "cpu";

	cfs_period_us = readInt64(0, comp, "cpu.cfs_period_us");
	writeInt64(0, comp, "cpu.cfs_quota_us",
			   cfs_period_us * ncores * gp_resource_group_cpu_limit);
	writeInt64(0, comp, "cpu.shares", 1024 * ncores);
}

void
CGroupCreateSub(Oid group)
{
	char path[1024];
	size_t pathsize = sizeof(path);
	const char *comp = "cpu";

	buildPath(group, comp, "", path, pathsize);

	if (access(path, F_OK))
	{
		/* the dir is not created yet, create it */
		if (mkdir(path, 0755) && errno != EEXIST)
			elog(ERROR, "cgroup: can't create cgroup for resgroup '%d': %s", group, strerror(errno));
	}

	/* check the permission */
	CGroupCheckPermission(group);
}

void
CGroupDestroySub(Oid group)
{
	char path[1024];
	size_t pathsize = sizeof(path);
	const char *comp = "cpu";

	buildPath(group, comp, "", path, pathsize);

	if (!access(path, F_OK))
	{
		/* the dir is already created, remove it */
		if (rmdir(path) && errno != ENOENT)
			elog(ERROR, "cgroup: can't remove cgroup for resgroup '%d': %s", group, strerror(errno));
	}
}

void
CGroupAssignGroup(Oid group, int pid)
{
	const char *comp = "cpu";

	writeInt64(group, comp, "cgroup.procs", pid);
}

void
CGroupSetCpuRateLimit(Oid group, float cpu_rate_limit)
{
	const char *comp = "cpu";

	/* SUB/shares := TOP/shares * cpu_rate_limit */

	int64 shares = readInt64(0, comp, "cpu.shares");
	writeInt64(group, comp, "cpu.shares", shares * cpu_rate_limit);
}

int64
CGroupGetCpuUsage(Oid group)
{
	const char *comp = "cpuacct";

	return readInt64(group, comp, "cpuacct.usage");
}

int
CGroupGetCpuCores(void)
{
	int cores = getCpuCores();

	if (cores < 0)
		elog(ERROR, "cgroup: can't get cpu cores");

	return cores;
}

#endif//__linux__
