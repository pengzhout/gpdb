#include "postgres.h"

#include <unistd.h>                             /* getpid() */
#include <pthread.h>
#include <limits.h>

#ifdef HAVE_POLL_H
#include <poll.h>
#endif
#ifdef HAVE_SYS_POLL_H
#include <sys/poll.h>
#endif

#include "libpq-fe.h"
#include "miscadmin.h"                  /* MyDatabaseId */
#include "storage/proc.h"               /* MyProc */
#include "storage/ipc.h"
#include "utils/memutils.h"

#include "access/xact.h"
#include "catalog/namespace.h"
#include "commands/variable.h"
#include "nodes/execnodes.h"    /* CdbProcess, Slice, SliceTable */
#include "postmaster/postmaster.h"
#include "tcop/tcopprot.h"
#include "utils/portal.h"
#include "utils/sharedsnapshot.h"
#include "tcop/pquery.h"

#include "libpq-fe.h"
#include "libpq-int.h"
#include "cdb/cdbconn.h"                /* SegmentDatabaseDescriptor */
#include "cdb/cdbfts.h"
#include "cdb/cdbvars.h"
#include "cdb/cdbdisp_query.h"
#include "cdb/cdbdisp_gang.h"

static SegdbDescsManager *SessionSegdbDescsManager = NULL;
static CdbComponentDatabases *cdb_component_dbs = NULL;

static Gang * buildGang(GangType type, List *segments, bool extendedQuery);
//static void cleanupSessionSegdbDescsManager(void);

static SegmentDatabaseDescriptor* allocateSegdbDesc(int segindex, bool extendedQuery);

static void recycleSegdbDesc(SegmentDatabaseDescriptor *segdbDesc);

static void InitSessionSegdbDescManager(void);

static SegmentDatabaseDescriptor ** createSegdbDescForGang(List *segments, bool extendedQuery);

static void destroySegdbDesc(SegmentDatabaseDescriptor *segdbDesc);

static Gang* createGangInternal(GangType type, List *segments, bool extendedQuery);

static CdbComponentDatabaseInfo *copyCdbComponentDatabaseInfo(CdbComponentDatabaseInfo *dbInfo);

static int
getPollTimeout(const struct timeval *startTS)
{
	struct timeval now;
	int			timeout = 0;
	int64		diff_us;

	gettimeofday(&now, NULL);

	if (gp_segment_connect_timeout > 0)
	{
		diff_us = (now.tv_sec - startTS->tv_sec) * 1000000;
		diff_us += (int) now.tv_usec - (int) startTS->tv_usec;
		if (diff_us >= (int64) gp_segment_connect_timeout * 1000000)
			timeout = 0;
		else
			timeout = gp_segment_connect_timeout * 1000 - diff_us / 1000;
	}
	else
		/* wait forever */
		timeout = -1;

	return timeout;
}
Gang *
GangState_AllocateGang(GangType type, List *segments, bool extendedQuery)
{
    MemoryContext oldContext = NULL;
    Gang       *newGang = NULL;

    if (Gp_role != GP_ROLE_DISPATCH)
    {
        elog(FATAL, "dispatch process called with role %d", Gp_role);
    }

    if (segments == NULL)
        return NULL;

    insist_log(IsTransactionOrTransactionBlock(),
               "cannot allocate segworker group outside of transaction");

    if (GangContext == NULL)
    {
        GangContext = AllocSetContextCreate(TopMemoryContext, "Gang Context", ALLOCSET_DEFAULT_MINSIZE,
                                            ALLOCSET_DEFAULT_INITSIZE,
                                            ALLOCSET_DEFAULT_MAXSIZE);
    }
    Assert(GangContext != NULL);
    oldContext = MemoryContextSwitchTo(GangContext);

    newGang = createGangInternal(type, segments, extendedQuery);
    newGang->allocated = true;

    MemoryContextSwitchTo(oldContext);
    return newGang;
}

static Gang*
createGangInternal(GangType type, List *segments, bool extendedQuery)
{
    Gang       *newGangDefinition;
    SegmentDatabaseDescriptor *segdbDesc = NULL;
    int         i = 0;
    int         create_gang_retry_counter = 0;
    int         in_recovery_mode_count = 0;
    int         successful_connections = 0;
    bool        retry = false;
    int         poll_timeout = 0;
    struct timeval startTS;
    PostgresPollingStatusType *pollingStatus = NULL;
    int size = 0;

    /*
     * true means connection status is confirmed, either established or in
     * recovery mode
     */
    bool       *connStatusDone = NULL;

    /* check arguments */
    Assert(CurrentResourceOwner != NULL);
    Assert(CurrentMemoryContext == GangContext);

    newGangDefinition = NULL;
    newGangDefinition = buildGang(type, segments, extendedQuery);		

create_gang_retry:
    /* If we're in a retry, we may need to reset our initial state, a bit */
    successful_connections = 0;
    in_recovery_mode_count = 0;
    retry = false;

    /* allocate and initialize a gang structure */

    Assert(newGangDefinition != NULL);

    size = newGangDefinition->size;

    /*
     * allocate memory within perGangContext and will be freed automatically
     * when gang is destroyed
     */
    pollingStatus = palloc(sizeof(PostgresPollingStatusType) * size);
    connStatusDone = palloc(sizeof(bool) * size);

    struct pollfd *fds;

    PG_TRY();
    {
        for (i = 0; i < size; i++)
        {
            char        gpqeid[100];
            char       *options;

            /*
             * Create the connection requests.  If we find a segment without a
             * valid segdb we error out.  Also, if this segdb is invalid, we
             * must fail the connection.
             */
            segdbDesc = newGangDefinition->db_descriptors[i];

            if (segdbDesc->conn)
            {
                successful_connections++;
                connStatusDone[i] = true;
                continue;
            }

            /*
             * Build the connection string.  Writer-ness needs to be processed
             * early enough now some locks are taken before command line
             * options are recognized.
             */
            build_gpqeid_param(gpqeid, sizeof(gpqeid),
                               segdbDesc->isWriter,
                               999,
                               segdbDesc->segment_database_info->hostSegs);
           options = makeOptions();

            /* start connection in asynchronous way */
            cdbconn_doConnectStart(segdbDesc, gpqeid, options);

            if (cdbconn_isBadConnection(segdbDesc))
                ereport(ERROR, (errcode(ERRCODE_GP_INTERCONNECTION_ERROR),
                                errmsg("failed to acquire resources on one or more segments"),
                                errdetail("%s (%s)", PQerrorMessage(segdbDesc->conn), segdbDesc->whoami)));

            connStatusDone[i] = false;

            /*
             * If connection status is not CONNECTION_BAD after
             * PQconnectStart(), we must act as if the PQconnectPoll() had
             * returned PGRES_POLLING_WRITING
             */
            pollingStatus[i] = PGRES_POLLING_WRITING;
        }

        /*
         * Ok, we've now launched all the connection attempts. Start the
         * timeout clock (= get the start timestamp), and poll until they're
         * all completed or we reach timeout.
         */
        gettimeofday(&startTS, NULL);
        fds = (struct pollfd *) palloc0(sizeof(struct pollfd) * size);

        for (;;)
        {
            int         nready;
            int         nfds = 0;

            poll_timeout = getPollTimeout(&startTS);

            for (i = 0; i < size; i++)
            {
                segdbDesc = newGangDefinition->db_descriptors[i];

                /*
                 * Skip established connections and in-recovery-mode
                 * connections
                 */
                if (connStatusDone[i])
                    continue;

                switch (pollingStatus[i])
                {
                    case PGRES_POLLING_OK:
                        cdbconn_doConnectComplete(segdbDesc);
                        if (segdbDesc->motionListener == 0)
                            ereport(ERROR, (errcode(ERRCODE_GP_INTERCONNECTION_ERROR),
                                            errmsg("failed to acquire resources on one or more segments"),
                                            errdetail("Internal error: No motion listener port (%s)", segdbDesc->whoami)));
                        successful_connections++;

                        if (segdbDesc->isWriter)
                            segdbDesc->cache->hasWriter = true;
                        connStatusDone[i] = true;
                        continue;

                    case PGRES_POLLING_READING:
                        fds[nfds].fd = PQsocket(segdbDesc->conn);
                        fds[nfds].events = POLLIN;
                        nfds++;
                        break;

                    case PGRES_POLLING_WRITING:
                        fds[nfds].fd = PQsocket(segdbDesc->conn);
                        fds[nfds].events = POLLOUT;
                        nfds++;
                        break;

                    case PGRES_POLLING_FAILED:
                        if (segment_failure_due_to_recovery(PQerrorMessage(segdbDesc->conn)))
                        {
                            in_recovery_mode_count++;
                            connStatusDone[i] = true;
                            elog(LOG, "segment is in recovery mode (%s)", segdbDesc->whoami);
                        }
                        else
                        {
                            ereport(ERROR, (errcode(ERRCODE_GP_INTERCONNECTION_ERROR),
                                            errmsg("failed to acquire resources on one or more segments"),
                                            errdetail("%s (%s)", PQerrorMessage(segdbDesc->conn), segdbDesc->whoami)));
                        }
                        break;
                    default:
                        ereport(ERROR, (errcode(ERRCODE_GP_INTERCONNECTION_ERROR),
                                        errmsg("failed to acquire resources on one or more segments"),
                                        errdetail("unknow pollstatus (%s)", segdbDesc->whoami)));
                        break;
                }

                if (poll_timeout == 0)
                    ereport(ERROR, (errcode(ERRCODE_GP_INTERCONNECTION_ERROR),
                                    errmsg("failed to acquire resources on one or more segments"),
                                    errdetail("timeout expired\n (%s)", segdbDesc->whoami)));
            }

            if (nfds == 0)
                break;

            CHECK_FOR_INTERRUPTS();

            /* Wait until something happens */
            nready = poll(fds, nfds, poll_timeout);

            if (nready < 0)
            {
                int         sock_errno = SOCK_ERRNO;

                if (sock_errno == EINTR)
                    continue;

                ereport(ERROR, (errcode(ERRCODE_GP_INTERCONNECTION_ERROR),
                                errmsg("failed to acquire resources on one or more segments"),
                                errdetail("poll() failed: errno = %d", sock_errno)));
            }
            else if (nready > 0)
            {
                int         currentFdNumber = 0;

                for (i = 0; i < size; i++)
                {
                    segdbDesc = newGangDefinition->db_descriptors[i];
                    if (connStatusDone[i])
                        continue;

                    Assert(PQsocket(segdbDesc->conn) > 0);
                    Assert(PQsocket(segdbDesc->conn) == fds[currentFdNumber].fd);

                    if (fds[currentFdNumber].revents & fds[currentFdNumber].events ||
                        fds[currentFdNumber].revents & (POLLERR | POLLHUP | POLLNVAL))
                        pollingStatus[i] = PQconnectPoll(segdbDesc->conn);

                    currentFdNumber++;

                }
            }
        }

        ELOG_DISPATCHER_DEBUG("createGang: %d processes requested; %d successful connections %d in recovery",
                              size, successful_connections, in_recovery_mode_count);

        /* some segments are in recovery mode */
        if (successful_connections != size)
        {
            Assert(successful_connections + in_recovery_mode_count == size);

            if (gp_gang_creation_retry_count <= 0 ||
                create_gang_retry_counter++ >= gp_gang_creation_retry_count ||
                type != GANGTYPE_PRIMARY_WRITER)
                ereport(ERROR, (errcode(ERRCODE_GP_INTERCONNECTION_ERROR),
                                errmsg("failed to acquire resources on one or more segments"),
                                errdetail("segments is in recovery mode")));

            ELOG_DISPATCHER_DEBUG("createGang: gang creation failed, but retryable.");

            releaseGang(newGangDefinition, true);
            newGangDefinition = NULL;
            retry = true;
        }
    }
    PG_CATCH();
    {
        //MemoryContextSwitchTo(GangContext);

        FtsNotifyProber();
        /* FTS shows some segment DBs are down */
        if (FtsTestSegmentDBIsDown(newGangDefinition->db_descriptors, size))
        {

            releaseGang(newGangDefinition, true);
            newGangDefinition = NULL;
            DisconnectAndDestroyAllGangs(true);
            CheckForResetSession();
            ereport(ERROR, (errcode(ERRCODE_GP_INTERCONNECTION_ERROR),
                            errmsg("failed to acquire resources on one or more segments"),
                            errdetail("FTS detected one or more segments are down")));

        }

        releaseGang(newGangDefinition, true);
        newGangDefinition = NULL;

        if (type == GANGTYPE_PRIMARY_WRITER)
        {
            DisconnectAndDestroyAllGangs(true);
            CheckForResetSession();
        }

        PG_RE_THROW();
    }
    PG_END_TRY();

    SIMPLE_FAULT_INJECTOR(GangCreated);

    if (retry)
    {
        CHECK_FOR_INTERRUPTS();
        pg_usleep(gp_gang_creation_retry_timer * 1000);
        CHECK_FOR_INTERRUPTS();

        goto create_gang_retry;
    }

    setLargestGangsize(size);

    return newGangDefinition;
}

static Gang *
buildGang(GangType type, List *segments, bool extendedQuery)
{
    Gang       *newGangDefinition = NULL;

    Assert(CurrentMemoryContext == GangContext);

    /* allocate a gang */
    newGangDefinition = (Gang *) palloc0(sizeof(Gang));
    newGangDefinition->type = type;
    newGangDefinition->size = list_length(segments);
    newGangDefinition->gang_id = -1;
    newGangDefinition->allocated = false;
    newGangDefinition->noReuse = false;
    newGangDefinition->dispatcherActive = false;
    newGangDefinition->portal_name = NULL;
    newGangDefinition->perGangContext = GangContext;
    newGangDefinition->db_descriptors = createSegdbDescForGang(segments, extendedQuery);

    ELOG_DISPATCHER_DEBUG("buildGangDefinition done");
    return newGangDefinition;
}

void
releaseGang(Gang *gp, bool noReuse)
{
    int         i = 0;

    if (!gp)
        return;

    ELOG_DISPATCHER_DEBUG("cleanupGang: cleaning gang id %d type %d size %d, "
                          "was used for portal: %s",
                          gp->gang_id, gp->type, gp->size,
                          (gp->portal_name ? gp->portal_name : "(unnamed)"));

    /*
     * if the process is in the middle of blowing up... then we don't do
     * anything here.  making libpq and other calls can definitely result in
     * things getting HUNG.
     */
    if (proc_exit_inprogress)
        return;

    for (i = 0; i < gp->size; i++)
    {
        SegmentDatabaseDescriptor *segdbDesc = gp->db_descriptors[i];

        Assert(segdbDesc != NULL);

        if (noReuse || gp->noReuse)
            goto destroy;

        if (cdbconn_isBadConnection(segdbDesc))
            goto destroy;

        /* if segment is down, the gang can not be reused */
        if (!FtsIsSegmentUp(segdbDesc->segment_database_info))
            goto destroy;

        /* Note, we cancel all "still running" queries */
        if (!cdbconn_discardResults(segdbDesc, 20))
            elog(FATAL, "cleanup called when a segworker is still busy");

        recycleSegdbDesc(segdbDesc);

        continue;

destroy:
        destroySegdbDesc(segdbDesc);
    }

    pfree(gp);
}

static void
destroySegdbDesc(SegmentDatabaseDescriptor *segdbDesc)
{
    SegdbDescCache *cache = segdbDesc->cache;
    cdbconn_disconnect(segdbDesc);
    cdbconn_termSegmentDescriptor(segdbDesc);

    if (segdbDesc->isWriter)
        cache->hasWriter = false;

    pfree(segdbDesc);
}

static void
recycleSegdbDesc(SegmentDatabaseDescriptor *segdbDesc)
{
    SegdbDescCache *cache = segdbDesc->cache;

    /* alway put writer to header */
    if (segdbDesc->isWriter)
    {
        DoublyLinkedHead_AddFirst(offsetof(SegmentDatabaseDescriptor, cachelist),
                                  &cache->idle_dbs,
                                  segdbDesc);
    }
    else
    {
        DoublyLinkedHead_AddLast(offsetof(SegmentDatabaseDescriptor, cachelist),
                                  &cache->idle_dbs,
                                  segdbDesc);
    }

    /* Add it to freelist either */
    DoublyLinkedHead_AddLast(offsetof(SegmentDatabaseDescriptor, freelist),
                                  &SessionSegdbDescsManager->idle_dbs,
                                  segdbDesc);

}

static void
InitSessionSegdbDescManager(void)
{
    HASHCTL     info;
    int         hash_flags;

    Assert(SessionSegdbDescsManager == NULL);

    SessionSegdbDescsManager = palloc(sizeof(SegdbDescsManager));

    /* Initialize HTAB */
    MemSet(&info, 0, sizeof(info));
    info.keysize = sizeof(int32);
    info.entrysize = sizeof(SegdbDescCache);
    info.hash = int32_hash;
    info.num_partitions = NUM_LOCK_PARTITIONS;
    hash_flags = (HASH_ELEM | HASH_FUNCTION);

    SessionSegdbDescsManager->cacheHash = hash_create("segdb description cache", 300, &info, hash_flags);

    /* initial quick access list */
    DoublyLinkedHead_Init(&SessionSegdbDescsManager->idle_dbs);
}

static SegmentDatabaseDescriptor **
createSegdbDescForGang(List *segments, bool extendedQuery)
{
    SegmentDatabaseDescriptor   **db_descriptors;
    SegmentDatabaseDescriptor   *segdbDesc;
    int                         count, size, segindex;
    ListCell                    *lc;

    if (SessionSegdbDescsManager == NULL)
        InitSessionSegdbDescManager();

    size = list_length(segments);
    db_descriptors = (SegmentDatabaseDescriptor **)
                            palloc0(size * sizeof(SegmentDatabaseDescriptor *));

    count = 0;
    foreach(lc, segments)
    {
        segindex = lfirst_int(lc);

        segdbDesc = allocateSegdbDesc(segindex, extendedQuery);

        db_descriptors[count++] = segdbDesc;
    }

    return db_descriptors;
}

static SegmentDatabaseDescriptor*
allocateSegdbDesc(int segindex, bool extendedQuery)
{
    SegmentDatabaseDescriptor   *segdbDesc;
    SegmentDatabaseDescriptor   *result = NULL;
    CdbComponentDatabaseInfo    *cdbInfoCopy;
    CdbComponentDatabaseInfo    *cdbinfo;
    SegdbDescCache              *cache = NULL;
    bool                        found = false;

    /* read gp_segment_configuration and build CdbComponentDatabases */
    cdb_component_dbs = getComponentDatabases();

    if (cdb_component_dbs == NULL ||
        cdb_component_dbs->total_segments <= 0 ||
        cdb_component_dbs->total_segment_dbs <= 0)
        insist_log(false, "schema not populated while building segworker group");

    if (segindex == -1)
        cdbinfo = &cdb_component_dbs->entry_db_info[0];
    else
    {
	if (cdb_component_dbs->total_segment_dbs == cdb_component_dbs->total_segments)
        	cdbinfo = &cdb_component_dbs->segment_db_info[segindex];
	else
        	cdbinfo = &cdb_component_dbs->segment_db_info[2*segindex];

        /* Q: if both primary and mirror are down, what is their role? */
        if (!SEGMENT_IS_ACTIVE_PRIMARY(cdbinfo))
        {
            /* FATAL level error, process will be cleanup */
            elog(FATAL, "segment %d is down", segindex);
        }
    }

    cache = hash_search(SessionSegdbDescsManager->cacheHash, (void *)&segindex, HASH_ENTER, &found);

    if (!found)
    {
        cache->segindex = segindex;
        cache->hasWriter = false;
        DoublyLinkedHead_Init(&cache->idle_dbs);
    }

    while (cache->idle_dbs.count != 0)
    {
        segdbDesc = (SegmentDatabaseDescriptor*)DoublyLinkedHead_Last(offsetof(SegmentDatabaseDescriptor, cachelist),
									&cache->idle_dbs);

	if (extendedQuery)
	{
		if (segdbDesc->isWriter)
		{
			break;	
		}
		else
		{
        		/* get a cached segdbDesc from cachelist */
        		segdbDesc = (SegmentDatabaseDescriptor*)DoublyLinkedHead_RemoveLast(offsetof(SegmentDatabaseDescriptor, cachelist),
                	                                                             &cache->idle_dbs);
		}
	}
	else
	{
        	/* get a cached segdbDesc from cachelist */
        	segdbDesc = (SegmentDatabaseDescriptor*)DoublyLinkedHead_RemoveFirst(offsetof(SegmentDatabaseDescriptor, cachelist),
                	                                                             &cache->idle_dbs);
	}

        /* remove from quick access list */
        DoubleLinks_Remove(offsetof(SegmentDatabaseDescriptor, freelist),
                                 &SessionSegdbDescsManager->idle_dbs,
                                 segdbDesc);

        /* check if cache match to latest cdbinfo */
        if (segdbDesc->segment_database_info->segindex != cdbinfo->segindex ||
            segdbDesc->segment_database_info->dbid != cdbinfo->dbid ||
            cdbconn_isBadConnection(segdbDesc) ||
            !FtsIsSegmentUp(segdbDesc->segment_database_info))
        {
            destroySegdbDesc(segdbDesc);
            continue;
        }
        else
        {
            result = segdbDesc;
            break;
        }
    }

    if (!result)
    {
        cdbInfoCopy = copyCdbComponentDatabaseInfo(cdbinfo);
        cdbconn_initSegmentDescriptor(&segdbDesc, cdbInfoCopy);
        setQEIdentifier(segdbDesc, -1, GangContext);
        DoubleLinks_Init(&segdbDesc->cachelist);
        DoubleLinks_Init(&segdbDesc->freelist);
        segdbDesc->isWriter = !cache->hasWriter;
        segdbDesc->cache = cache;
        result = segdbDesc;
    }

    return result;
}

static CdbComponentDatabaseInfo *
copyCdbComponentDatabaseInfo(
                             CdbComponentDatabaseInfo *dbInfo)
{
    int         i = 0;
    int         size = sizeof(CdbComponentDatabaseInfo);
    CdbComponentDatabaseInfo *newInfo = palloc0(size);

    memcpy(newInfo, dbInfo, size);

    if (dbInfo->hostip)
        newInfo->hostip = pstrdup(dbInfo->hostip);

    /* So far, we don't need them. */
    newInfo->address = NULL;
    newInfo->hostname = NULL;
    for (i = 0; i < COMPONENT_DBS_MAX_ADDRS; i++)
        newInfo->hostaddrs[i] = NULL;

    return newInfo;
}
