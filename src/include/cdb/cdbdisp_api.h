#ifndef CDBDISP_API_H
#define CDBDISP_API_H

#include "cdb/cdbdisp_new.h"

struct QueryDesc;
struct pg_result;

void API_DispatchPlanStart(struct QueryDesc *queryDesc,
				bool planRequiresTxn,
				bool cancelOnError);

void API_DispatchPlanEnd(void);

void API_DispatchCommand(const char* queryText,
			char *serializedQuerytree,
			int serializedQuerytreelen,
			char *serializedQueryDispatchDesc,
			int serializedQueryDispatchDesclen, int flag, CdbPgResults *cdb_pgresults);

struct pg_result **
API_DispatchDTXCommand(DtxProtocolCommand dtxProtocolCommand,
			int flags,
			char *dtxProtocolCommandLoggingStr,
			char *gid,
			DistributedTransactionId gxid,
			ErrorData **qeError,
			int *numresults,
			bool *badGangs, CdbDispatchDirectDesc *direct,
			char *serializedDtxContextInfo,
			int serializedDtxContextInfoLen);

#endif
