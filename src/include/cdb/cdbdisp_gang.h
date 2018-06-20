#ifndef CDBDISP_GANG_H
#define CDBDISP_GANG_H

#include "cdb/cdbutil.h"
#include "executor/execdesc.h"
#ifdef WIN32
#include "pthread-win32.h"
#else
#include <pthread.h>
#endif
#include "utils/faultinjector.h"
#include "utils/portal.h"
#include "cdb/cdbgang.h"

struct Port;
struct QueryDesc;
struct DirectDispatchInfo;
struct EState;
struct PQExpBufferData;

typedef struct SegdbDescCache
{
    int32                               segindex;
    bool                                hasWriter;
    DoublyLinkedHead                    idle_dbs;
}SegdbDescCache;

typedef struct SegdbDescsManager
{
    HTAB                    *cacheHash;
    /* qucik access to idle segment dbs*/
    DoublyLinkedHead        idle_dbs;
}
SegdbDescsManager;


Gang * GangState_AllocateGang(GangType type, List *segments, bool extendedQuery);

extern void releaseGang(Gang *gp, bool noReuse);

#endif
