-- enable resource group and restart cluster.
-- start_ignore
! gpconfig -c gp_resource_manager -v group;
! gpstop -rai;
-- end_ignore
