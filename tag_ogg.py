#!/usr/bin/env python

from ssh_input import ssh_input
import time
from logging_method import log

@log
def tag_ogg(tag_args,src_args,tag_cmd_args,_REMOTE_COMMAND):
    
    _REMOTE_GGSCI_COMMAND = '''
su - %s <<EOF
export ORACLE_SID=%s
export LD_LIBRARY_PATH=%s:\$ORACLE_HOME/lib
export LIBPATH=%s:\$ORACLE_HOME/lib
%s/ggsci
%%s
exit
EOF
'''%(tag_cmd_args[0],tag_cmd_args[1],tag_cmd_args[2],tag_cmd_args[2],tag_cmd_args[2])

    # create ogg user and grant to user
    dir_sql = _REMOTE_COMMAND %"SELECT distinct(REVERSE(SUBSTR(REVERSE(d.file_name),INSTR(REVERSE(d.file_name),'/')+1))) \
                    from dba_data_files d,dual;"
    db_dir = ssh_input(tag_args,dir_sql)[0].replace('\n','')
    create_user_sql = '''create tablespace ops_odc_tps datafile '%s/ops_odc01.dbf' size 100M autoextend on;
create user ops_odc identified by ops_odc default tablespace ops_odc_tps;
GRANT CONNECT TO ops_odc;
GRANT ALTER ANY TABLE TO ops_odc;
GRANT ALTER SESSION TO ops_odc;
GRANT CREATE SESSION TO ops_odc;
GRANT FLASHBACK ANY TABLE TO ops_odc;
GRANT SELECT ANY DICTIONARY TO ops_odc;
GRANT SELECT ANY TABLE TO ops_odc;
GRANT RESOURCE TO ops_odc;
GRANT DBA TO ops_odc;'''%db_dir
    ssh_input(tag_args,_REMOTE_COMMAND % create_user_sql)

    # create subdirs
    create_subdirs_cmd = _REMOTE_GGSCI_COMMAND % "create subdirs"
    ssh_input(tag_args,create_subdirs_cmd)


    # create process mgr and start it.
    print("######Create and start the process MGR.")
    create_mgr_cmd = '''
su - %s <<EOF
export ORACLE_SID=%s
rm -f %s/dirprm/mgr.prm
rm -f %s/GLOBALS
echo 'port 7809
DYNAMICPORTLIST 7800-7810
PURGEOLDEXTRACTS ./dirdat/%s*, USECHECKPOINTS, MINKEEPHOURS 24
autorestart extract * retries 10 waitminutes 10'>>%s/dirprm/mgr.prm
echo 'GGSCHEMA ops_odc
CHECKPOINTTABLE ops_odc.ggs_checkpoint '>>%s/GLOBALS
EOF'''%(tag_cmd_args[0],tag_cmd_args[1],tag_cmd_args[2],tag_cmd_args[2],tag_cmd_args[6],tag_cmd_args[2],tag_cmd_args[2])
    ssh_input(tag_args,create_mgr_cmd)
    start_mgr_cmd = _REMOTE_GGSCI_COMMAND % "start mgr"
    start_mgr_res = ssh_input(tag_args,start_mgr_cmd)
    print(''.join(start_mgr_res))

    # create checkpoint table
    print("######Create checkpoint table.")
    create_checkpoint_tb_sql = '''
su - %s <<EOF
export ORACLE_SID=%s
cd %s
sqlplus -s ops_odc/ops_odc
@chkpt_ora_create.sql
exit
EOF
'''%(tag_cmd_args[0],tag_cmd_args[1],tag_cmd_args[2] )

    create_checkpoint_tb_res = ssh_input(tag_args,create_checkpoint_tb_sql)
    print(''.join(create_checkpoint_tb_res))


    # create the process replicate.
    print("######Create the process replicate.")
add_rep_cmd = _REMOTE_GGSCI_COMMAND % f"dblogin userid ops_odc password ops_odc\nadd replicat {tag_cmd_args[6]}_rep exttrail ./dirdat/{tag_cmd_args[6]}"
    add_rep_res = ssh_input(tag_args,add_rep_cmd)
    print(''.join(add_rep_res))
    
    map_obj_cmd = '\n'.join(['map %s target %s;'%(x,x) for x in tag_cmd_args[3].split(',')])

    edit_rep_cmd = '''
su - %s <<EOF
export ORACLE_SID=%s
rm -f %s/dirprm/%s_rep.prm
echo 'replicat %s_rep
setenv (NLS_LANG="SIMPLIFIED CHINESE_CHINA.ZHS16GBK")
userid ops_odc, password ops_odc
ASSUMETARGETDEFS
ALLOWNOOPUPDATES
DBOPTIONS DEFERREFCONST
HANDLETPKUPDATE
--batchsql
ddlerror 955 ignore
ddlerror 1917 ignore
ddlerror 24344 ignore
ddlerror 1031 ignore
ddl include mapped
DISCARDFILE %s/dirrpt/%s.dsc, APPEND megabytes 20
DISCARDROLLOVER on sunday
%s'>>%s/dirprm/%s_rep.prm
EOF'''%(tag_cmd_args[0],tag_cmd_args[1],tag_cmd_args[2],tag_cmd_args[6],tag_cmd_args[6],tag_cmd_args[2],tag_cmd_args[6],map_obj_cmd,tag_cmd_args[2],tag_cmd_args[6])

    edit_rep_res = ssh_input(tag_args,edit_rep_cmd)


    # if version is 11g 
    check_version_rep_sql = _REMOTE_COMMAND % "select distinct(VERSION) from product_component_version;\nshow parameter ENABLE_GOLDENGATE_REPLICATION"
    check_res = ''.join(ssh_input(tag_args,check_version_rep_sql))
    if '11' in check_res and 'FALSE' in check_res:
        rep_true_sql = _REMOTE_COMMAND % "ALTER SYSTEM SET ENABLE_GOLDENGATE_REPLICATION = TRUE SCOPE=BOTH;"
        ssh_input(tag_args,rep_true_sql)


    print(''.join(edit_rep_res))
    
