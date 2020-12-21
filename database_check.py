#!/usr/bin/env python

from ssh_input import ssh_input
from logging_method import log


# check the source database
@log 
def db_check(args,cmd_args,_REMOTE_COMMAND):
    schemas = "','".join(list(set([i.split('.')[0] for i in cmd_args[3].split(',')]))).upper()
    
    # the sqls of check
    check_supp_logging_sql = "select supplemental_log_data_min ,force_logging from v\$database;"
    check_compress_tab_sql = "select owner,table_name from dba_tables where owner in ('%s') and compression='ENABLED';"%schemas
    check_part_compress_tab_sql = "select distinct table_owner,TABLE_NAME from dba_tab_partitions where COMPRESSION='ENABLED' ;"
    check_unsup_col_tab_sql = "select owner,table_name,column_name,data_type \
            from dba_tab_columns where owner in ('%s') and data_type in \
                ('ANYDATA','ANYDATASET','ANYTYPE','BFILE','BINARY_INTEGER','MLSLABEL','PLS_INTEGER','TIMEZONE_ABBR','TIMEZONE_REGION','URITYPE,UROWID');"%schemas
    check_mtr_view_sql = " select object_name,owner from dba_objects where object_type='MATERIALIZED VIEW';"
    check_tri_ogg_sql = "select TRIGGER_NAME from dba_triggers where TRIGGER_NAME='GGS_DDL_TRIGGER_BEFORE';"
    check_mgr_user_sql = "select USERNAME from dba_users where username='OPS_ODC';"


   


    



    print("######1.The status from supplemental_log and force_logging:")
    res_supp_logging = ssh_input(args,_REMOTE_COMMAND % check_supp_logging_sql)[-1].split(' ')
    res_supp = res_supp_logging[0]
    res_logging = res_supp_logging[-1].replace('\n','')
    print("Supplemental_log: %s\nForce_logging: %s\n"%(res_supp,res_logging))
    if res_supp =='NO':
        print("Supplemental_log were Disabled.\nYou can run the following sql to Enable at the right time.")
        print("<alter database add supplemental log data;>\n")
    else:
        print("Supplemental_log were Enabled.")
    if res_logging =='NO':
        print("Force_logging were Disabled.\nYou can run the following sql to Enable at the right time.")
        print("<alter database force logging;>\n")
    else:
        print("Force_logging were Enabled.")

    print("######2.The compress table info :")
    res_commpress_tab = ssh_input(args,_REMOTE_COMMAND %check_compress_tab_sql)
    res_part_commpress_tab = ssh_input(args,_REMOTE_COMMAND %check_part_compress_tab_sql)
    if res_commpress_tab ==[]:
        print ("No compress table found.\n")
    else:
        print ("OWNER                          TABLE_NAME%s"%(''.join(res_commpress_tab)))
    if res_part_commpress_tab ==[]:
        print ("No  compress partition table found.\n")
    else:
        print ("OWNER                          TABLE_NAME%s"%(''.join(res_commpress_tab)))   

    print("######3.The unsupport column table info :")
    res_unsup_col_tab = ssh_input(args,_REMOTE_COMMAND %check_unsup_col_tab_sql)
    if res_unsup_col_tab ==[]:
        print ("No  unsupport column table found.\n")
    else:
        print ("OWNER                          TABLE_NAME                     COLUMN_NAME                    DATA_TYPE"%(''.join(res_unsup_col_tab)))


    print("######4.The materialized  view info :")
    res_mtr_view = ssh_input(args,_REMOTE_COMMAND %check_mtr_view_sql)
    if res_commpress_tab ==[]:
        print ("No materialized  view found.\n")
    else:
        print ("OWNER                            OBJECT_NAME%s"%(''.join(res_mtr_view)))


    print("######5.The check of pre-install for ogg:")
    print("5.1. Check the trigger of ogg:")
    res_tri_ogg = ssh_input(args,_REMOTE_COMMAND % check_tri_ogg_sql)
    if res_tri_ogg ==[]:
        print ("No trigger of  ogg found.\n")
    else:
        print("The trigger of ogg named 'GGS_DDL_TRIGGER_BEFORE' exsist.\n")
        drop_y_n = input("Do you want to drop the trigger now? Y/N")
        if drop_y_n.upper()=='Y':
             drop_tri_sql = _REMOTE_COMMAND % "drop trigger GGS_DDL_TRIGGER_BEFORE;"
             res_drop_tri = ssh_input(args,drop_tri_sql)
             print(''.join(res_drop_tri))
        else:
            print("If you want to drop the trigger by yourself,the sql is:\n<drop trigger GGS_DDL_TRIGGER_BEFORE;>")

    print("5.2. Check the user for ogg:")
    res_mgr_user = ssh_input(args,_REMOTE_COMMAND % check_mgr_user_sql)
    if res_mgr_user ==[]:
        print ("No manager user for  ogg found.\n")
    else:
        print("The manager user for  ogg named 'OPS_ODC' exsist.\n")
        drop_user_y_n = input("Do you want to drop the user now? Y/N")
        if drop_user_y_n.upper()=='Y':
             drop_user_sql = _REMOTE_COMMAND % "drop user ops_odc cascade;"
             res_drop_user = ssh_input(args,drop_user_sql)
             print(''.join(res_drop_user))
        else:
            print("If you want to drop the user by yourself,the sql is:\n<drop user ops_odc cascade;>")


    return 1
    

    



    

