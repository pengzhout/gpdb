## Dispatcher API
This document illustrates the interfaces of a GPDB component called dispatcher, which is responsible for
1) building connections from master node to segments,
2) managing query/plan dispatching, and
3) collecting query execution results.

The implementation of dispatcher is mainly located under this directory.

### Terms Used:
* Gang: Gang refers to a group of processes on segments. There are 4 types of Gang:
	* `GANGTYPE_ENTRYDB_READER`: consist of one single process on master node
	* `GANGTYPE_SINGLETON_READER`: consist of one single process on a segment node
	* `GANGTYPE_PRIMARY_READER`: consist of N (number of segments) processes, each process is on a different segment
	* `GANGTYPE_PRIMARY_WRITER`: like `GANGTYPE_PRIMARY_READER`, while it can update segment databases, and is responsible for DTM (Distributed Transaction Management). A session can have at most one Gang of this type, and reader Gangs cannot exist without a writer Gang
<br><br>
For a query/plan, QD would build one `GANGTYPE_PRIMARY_WRITER` Gang, and several (0 included) reader Gangs based on the plan. A Gang could be reused across queries in a session. GPDB provides several GUCs to control Gang resuage, e.g, `gp_vmem_idle_resource_timeout`, `gp_cached_gang_threshold` and `gp_vmem_protect_gang_cache_limit`
<br><br>
* Dispatch: sending plan, utility statement, plain SQL text, and DTX command to Gangs, collecting results of execution and handling errors

### Interface Routines:
* Gang creation and tear down:
	* `AllocateGang`: allocate a gang with specified type on specified segments, the gang is made up of idle segment dbs got from CdbComponentDatabases (see cdbcomponent_allocateIdleSegdb).
	* `RecycleGang`: put each member of gang back to segment pool if gang can be cleanup correctly including discarding results, connection status check (see cdbcomponent_recycleIdleSegdb), otherwise, destroy it.
	* `DisconnectAndDestroyAllGangs`: tear down all existing Gangs of this session
* Gang status check:
	* `GangOK`: check if a created Gang is healthy
* Dispatch:
	* `CdbDispatchPlan`: send PlannedStmt to Gangs specified in `queryDesc` argument. Once finishes work on QD, call `CdbCheckDispatchResult` to wait results or `CdbDispatchHandleError` to cancel query on error
	* `CdbDispatchUtilityStatement`: send parsed utility statement to the writer Gang, and block to get results or error
	* `CdbDispatchCommand`: send plain SQL text to the writer Gang, and block to get results or error
	* `CdbDispatchSetCommand`: send SET commands to all existing Gangs except those allocated for extended queries, and block to get results or error
	* `CdbDispDtxProtocolCommand`: send DTX commands to the writer Gang, and block to get results or error
	
### Dispatcher Mode:
To improve parallelism, Dispatcher has two different implementations internally, one is using threads, the other leverages asynchronous network programming. When GUC `gp_connections_per_thread` is 0, async dispatcher is used, which is the default configuration

### Dispatcher routines:
All dispatcher routines contains few standard steps:
* CdbDispatchPlan/CdbDispatchUtilityStatement/CdbDispatchCommand/CdbDispatchSetCommand/CdbDispDtxProtocolCommand
	* `cdbdisp_makeDispatcherState`: create a dispatcher state and register it in the resource owner release callback.
	* `buildGpQueryString/buildGpDtxProtocolCommand`: serialize Plan/Utility/Command to raw text QEs can recognize, must allocate it within DispatcherContex.
	* `AllocateWriterGang/AssignGangs`: allocate a gang or a bunch of gangs (for Plan) and prepare for execution, gangs are tracked by dispatcher state
	* `cdbdisp_dispatchToGang`: send serialized raw query to QEs in unblocking mode which means the data in connection is not guaranteed being flushed, this is very usefull if a plan contains multiple slices, so dispatcher don't block when libpq connections is congested 
	* `cdbdisp_waitDispatchFinish`: as described above, this function will poll on libpq connections and flush the data in bunches 
	* `cdbdisp_checkDispatchResult`: block until QEs report a command OK response or an error etc
	* `cdbdisp_getDispatchResults`: fetch results from dispatcher state or error data if an error occurs
	* `cdbdisp_destroyDispatcherState`: destroy current dispatcher state and recycle gangs allocated by it.

### CdbComponentDatabases
CdbComponentDatabases is a snapshot of current cluster components based on catalog gp_segment_configuration.
It provides information about each segment component include dbid, contentid, hostname, ip address, current role etc.
It also maintains a pool of idle segment dbs (SegmentDatabaseDescriptor), dispatcher can reuse those segment dbs
between statements within a session.

CdbComponentDatabases has a memory context named CdbComponentsContext associated.

#### CdbComponentDatabases routines:
There are a few functions to manipulate CdbComponentDatabases, Dispatcher can use those functions to make up/clean up a gang.

* cdbcomponent_getCdbComponents(): get a snapshot of current cluster components from gp_segment_configuration. When FTS version changed since last time, destroy current components snapshot and get a new one if 1) session has no temp tables 2) current gxact need two-phase commit but gxid has not been dispatched to segments yet or current gxact don't need two phase commit.

* cdbcomponent_destroyCdbComponents(): destroy a snapshot of current cluster components include the MemoryContext and pool of idle segment dbs.

* cdbcomponent_allocateIdleSegdb(contentId, SegmentType): allocate a free segment db by 1) reuse a segment db from pool of idle segment dbs. 2) create a brand new segment db. For case2, the connection is not actually established inside this function, the caller should establish the connections in a batch to gain performance. This function guarantees each segment in a session have only one writer. SegmentType can be:
	* SEGMENTTYPE_EXPLICT_WRITER : must be writer, for DTX commands and DDL commands and DML commands need two-phase commit.
	* SEGMENTTYPE_EXPLICT_READER : must be reader, only be used by cursor.
	* SEGMENTTYPE_ANY: any type is ok, for most of queries which don't need two-phase commit.

* cdbcomponent_recycleIdleSegdb(SegmentDatabaseDescriptor): recycle/destroy a segment db. a segment db will be destroyed if 1) caller specify forceDestroy to true. 2)connection to segment db is already bad. 3) FTS detect the segment is already down. 4) exceeded the pool size of idle segment dbs. 5) cached memory exceeded the limitation. otherwise, the segment db will be put into a pool for reusing later.

* cdbcomponent_cleanupIdleSegdbs(includeWriter): disconnect and destroy idle segment dbs of all components. includeWriter tells cleanup idle writers or not.
