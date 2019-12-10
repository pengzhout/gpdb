#include "pg_upgrade_greenplum.h"

typedef enum
{
	DISPATCHER = 0,
	SEGMENT
} segmentMode;

typedef struct {
	bool progress;
	segmentMode segment_mode;
	checksumMode checksum_mode;
	char *old_tablespace_file_path;
} GreenplumUserOpts;

static GreenplumUserOpts greenplum_user_opts;

void
initialize_greenplum_user_options(void)
{
	greenplum_user_opts.segment_mode = SEGMENT;
	greenplum_user_opts.old_tablespace_file_path = NULL;
}

bool
process_greenplum_option(greenplumOption option, char *option_value)
{
	switch (option)
	{
		case GREENPLUM_MODE_OPTION:        /* --mode={dispatcher|segment} */
			if (pg_strcasecmp("dispatcher", optarg) == 0)
				greenplum_user_opts.segment_mode = DISPATCHER;
			else if (pg_strcasecmp("segment", optarg) == 0)
				greenplum_user_opts.segment_mode = SEGMENT;
			else
			{
				pg_log(PG_FATAL, "invalid segment configuration\n");
				exit(1);
			}
			break;

		case GREENPLUM_PROGRESS_OPTION:        /* --progress */
			greenplum_user_opts.progress = true;
			break;

		case GREENPLUM_ADD_CHECKSUM_OPTION:        /* --add-checksum */
			greenplum_user_opts.checksum_mode = CHECKSUM_ADD;
			break;

		case GREENPLUM_REMOVE_CHECKSUM_OPTION:        /* --remove-checksum */
			greenplum_user_opts.checksum_mode = CHECKSUM_REMOVE;
			break;

		case GREENPLUM_OLD_GP_DBID: /* --old-gp-dbid */
			old_cluster.gp_dbid = atoi(optarg);
			break;

		case GREENPLUM_NEW_GP_DBID: /* --new-gp-dbid */
			new_cluster.gp_dbid = atoi(optarg);
			break;

		case GREENPLUM_OLD_TABLESPACES_FILE: /* --old-tablespaces-file */
			greenplum_user_opts.old_tablespace_file_path = pg_strdup(optarg);
			break;

		default:
			return false;
	}

	return true;
}

void
validate_greenplum_options(void)
{
	if (old_cluster.gp_dbid == GP_DBID_NOT_SET)
		pg_fatal("--old-gp-dbid must be set\n");

	if (new_cluster.gp_dbid == GP_DBID_NOT_SET)
		pg_fatal("--new-gp-dbid must be set\n");

	if (greenplum_user_opts.old_tablespace_file_path) {
		populate_old_cluster_with_old_tablespaces(
			&old_cluster,
			greenplum_user_opts.old_tablespace_file_path);
	}
}

bool
is_greenplum_dispatcher_mode()
{
	return greenplum_user_opts.segment_mode == DISPATCHER;
}

bool
is_checksum_mode(checksumMode mode)
{
	return mode == greenplum_user_opts.checksum_mode;
}

bool
is_show_progress_mode(void)
{
	return greenplum_user_opts.progress;
}
