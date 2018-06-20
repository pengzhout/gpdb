AtStart_Dispatcher(void);
AtAbort_Dispatcher(void);
AtCommit_Dispatcher(void);


cdbdisp_close(void);

void
cdbdisp_dispatchToGang(struct CdbDispatcherState *ds,
					                          struct Gang *gp,
											                         int sliceIndex,
																	                        CdbDispatchDirectDesc *direct);

/*dispatch a PLAN*/
PortalStart
	switch strategy:
	case one_select
		ExecutorStart:
			-- init_plan
				cdbdisp_connect();
				cdbdisp_dispatchCommand();
				cdbdisp_close();

			CdbDispatchPlan
				cdbdisp_connect()
				cdbdisp_dispatchCommand();
				cdbdisp_dispatchCommand();

	case multi_query


PortalRun
	case one_select
		ExecutorRun
			cdbdisp_processResult();

	case multi_query
		ExecutorStart
		ExecutorRun
			-- cdbdispatchCommand or cdbdispatchUtility
					cdbdisp_connect()
					cdbdisp_dispatchCommand()
					cdbdisp_close()
		ExecutorEnd

PortalEnd
	ExecutorEnd;
		cdbdisp_close()

CommitTransaction / AbortTransaction()
	cdbdisp_connect()
	cdbdisp_dispatchCommand()
	cdbdisp_close()
	

/*create a gang*/
result = cdbdisp_dispatchCommand(char *queryText_slice1, List *segments);
/*create another gang*/
result1 = cdbdisp_dispatchCommand(char *queryText_slice2, List *segments);

result2 = cdbdisp_dispatchCommand(char *queryText_slice3, List *segments);
cdbdisp_close(void);

/* dispatch a utility */
cdbdisp_connect(void);
result = cdbdisp_dispatchCommand(char *queryText_slice1, List *segments);
cdbdisp_close(void);




/*create a gang*/
cdbdisp_connect(void);
cdbdisp_connect(void); /* dispatch has been opened */
cdbdisp_close(void);

/* how to handle cursor? another dispatcher */
BEGIN; // don't create writer gang 
DECLARE c1 for select * from abc_region1; // new reader gang, resize the writer gang
DECLARE c2 for select * from abc_region2; // new reader gang, but also need to enlarge writer gang.
select * from abc_region3; // reuse writer gang, may need resize. 
END;

/* how to handle prepare & execute */
// scope of dispatcher : session.
// some kind of command need run on first gang, so you need to recycle the first gang after you dispatched a plan or utility or command.
//

Every time open a new dispatch


CurrentDispatchState          ---->    TopDispatchState
			      ---->    UnamedPortalDispatchState
                              ---->    NamedPortalDispatchState
