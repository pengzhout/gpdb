+formatDispatchQueryText(const char *strCommand, int flag)
+{
+       DispatchCommandQueryParms       *pQueryParms;
+       char                            *queryText;
+       int                             queryTextLen;
+
+       pQueryParms = buildGpCommandQueryParms(strCommand, NULL, 0, NULL, 0, flag);
+
+       queryText = buildGpQueryString(pQueryParms, &queryTextLen);
+
+       queryTextLen = 0;
+
+       return queryText;
+
+}
+
+
+static DispatchCommandQueryParms *
+buildGpCommandQueryParms(const char *strCommand,
+               char *serializedQuerytree,
+               int serializedQuerytreelen,
+               char *serializedQueryDispatchDesc,
+               int serializedQueryDispatchDesclen,
+               int flags)
+
+{
+       DispatchCommandQueryParms *pQueryParms;
+       CdbComponentDatabaseInfo *qdinfo;
spatchCommandQueryParms *pQueryParms;
+       CdbComponentDatabaseInfo *qdinfo;
+
+       bool            needTwoPhase = flags & DF_NEED_TWO_PHASE;
+       bool            withSnapshot = flags & DF_WITH_SNAPSHOT;
+
+       elogif((Debug_print_full_dtm || log_min_messages <= DEBUG5), LOG,
+                  "cdbdisp_dispatchCommandInternal: %s (needTwoPhase = %s)",
+                  strCommand, (needTwoPhase ? "true" : "false"));
+
+       pQueryParms = palloc0(sizeof(*pQueryParms));
+       pQueryParms->strCommand = strCommand;
+       pQueryParms->serializedQuerytree = serializedQuerytree;
+       pQueryParms->serializedQuerytreelen = serializedQuerytreelen;
+       pQueryParms->serializedQueryDispatchDesc = serializedQueryDispatchDesc;
+       pQueryParms->serializedQueryDispatchDesclen = serializedQueryDispatchDesclen;
+       /*
+        * Serialize a version of our DTX Context Info
+        */
+       pQueryParms->serializedDtxContextInfo =
+               qdSerializeDtxContextInfo(&pQueryParms->serializedDtxContextInfolen,
+                                                                 withSnapshot, false,
+                                                                 mppTxnOptions(needTwoPhase),
+                                                                 "cdbdisp_dispatchCommandInternal");
+
+       /*
+        * sequence server info
+        */
+       qdinfo = &(getComponentDatabases()->entry_db_info[0]);
+       Assert(qdinfo != NULL && qdinfo->hostip != NULL);
+       pQueryParms->seqServerHost = pstrdup(qdinfo->hostip);

+       pQueryParms->seqServerHost = pstrdup(qdinfo->hostip);
+       pQueryParms->seqServerHostlen = strlen(qdinfo->hostip) + 1;
+       pQueryParms->seqServerPort = seqServerCtl->seqServerPort;
+
+       return pQueryParms;

#include "cdb/cdbpq.h"
#if 0
	int			i;
	CdbDispatchCmdAsync *pParms = (CdbDispatchCmdAsync *) ds->dispatchParams;

	{
		int query_text_len;
		int tmp_query_text_len;
		int origin_slice_id;
		memcpy(&tmp_query_text_len, pParms->query_text + 1, sizeof(int));

		query_text_len = ntohl(tmp_query_text_len);

//		elog(WARNING, "dispatch %d , %d size to gang", query_text_len, tmp_query_text_len);

		memcpy(&origin_slice_id, pParms->query_text + query_text_len  + 1 - sizeof(int), sizeof(int));

//		elog(WARNING, "sliceId address is %x", pParms->query_text + query_text_len);
//		elog(WARNING, "origin sliceId is %d", ntohl(origin_slice_id));

		int newSliceIndex = htonl(sliceIndex);
		memcpy(pParms->query_text + query_text_len + 1 - sizeof(int), &newSliceIndex, sizeof(int));

//		elog(WARNING, "changed sliceId to %d", sliceIndex);
	}
#endif


