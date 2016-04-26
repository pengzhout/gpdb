/*
 * 1. AssignGangs
 *
 * --> dispatcher->getGangGroup()
 *
 * */

dispatcher::allocGangs(type, size)
{
	for i < size:
		PQconnect();

	return GangDefinition;
}

AssignGang --> gangGroupID = dispatcher->getGangGroup(sliceTable);

// To let dispatcher allocate num gangs as designed.
dispatcher::getGangGroup(sliceTable* sliceTable)
{
	slices = sliceTable->slices;
	GangGroup group[sizeof(sliceTable)];
	int index = 0;

	foreach(slice, slices)
		if isFirstNGang(slice):
			group[index++] = dispatch->getGangs(WRITER, n);
		else if is1Gang(slice):
			group[index++] = dispatch->getGangs(READER, 1);
		else:
			group[index++] = dispatch->getGangs(READER, n);
}

/*
 * 2. cdbdisp_dispatchPlan
 *
 *
 * --> dispatcher->sendPlan(plan, gangGroupID)
 *
 * */
cdbdisp_dispatchPlan -->  dispatcher->sendPlan(plan, gangGroupID);

dispatcher::sendPlan(plan, gangGroupID)
{
	queryParms.serializedPlanntree = serializeNode(plannedStmt);  
	queryParms.serializedsliceinfo = serializeNode(sliceTable);  
	queryParms.seqServerHost = stringxxx;
	queryParms.seqServerPort = stringyyy;
	queryParms.serializedGangGroup = serializeNode(gangGroup);

	foreach(gang, gangGroup)
	{
		gang->sendPlan(queryParms);
	}
}


/*
 * 3. CdbCheckDispatchResult()
 *
 * --> dispatch->close(gangGroupID)
 *
 * */

// QD have finished the work, need to release the gang
dispatch::Close(gangGroupID)
{
	foreach(gang, gangGroup)
	{
		gang->checkResult();
		gang->close();
		dispatch->addFreeGang(gang);
	}
}

Gang::CheckStatus();
Gang::CheckResult();
Gang::Close();
Gang::Cancel();
Gang::Terminate();
Gang::Open();
Gang::SendPlan(Plan);
Gang::SendQuery(Plan);
