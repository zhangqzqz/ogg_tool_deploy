#!/usr/bin/env python
import time
import sys
import os
from multiprocessing import Process
from ssh_input import ssh_input,ssh_scp
from check_file import get_file_sha256
from logging_method import log



# start process 
@log
def start_prc(src_args,tag_args,src_cmd_args,_REMOTE_GGSCI_COMMAND):
    _REMOTE_GGSCI_COMMAND = _REMOTE_GGSCI_COMMAND % (src_cmd_args[0],src_cmd_args[1],src_cmd_args[2],src_cmd_args[2],src_cmd_args[2])
    # start ext and dmp
    start_ext_cmd = _REMOTE_GGSCI_COMMAND % "start mc_ext"
    start_ext_res = ssh_input(src_args,start_ext_cmd)
    print('\n'.join(start_ext_res))
    
    time.sleep(10)
    select_trailfile_cmd = "ls %s/dirdat/mc*"%src_cmd_args[2]
    trailfile_res = ssh_input(src_args,select_trailfile_cmd)[0]
    if 'No such file or directory' in trailfile_res:
        print("The trail file have not generate.")
        return "trail file generate error."
        os._exit(0)
    else:
        start_dmp_cmd = _REMOTE_GGSCI_COMMAND % "start mc_dmp"
        start_dmp_res = ssh_input(src_args,start_dmp_cmd)
        print('\n'.join(start_dmp_res))
        return "process start succees"

# target database create tbs
@log
def create_tbs(src_args,tag_args,src_cmd_args,_REMOTE_COMMAND,_REMOTE_TAG_COMMAND):
    dir_sql_tmp = '''
                SELECT distinct(REVERSE(SUBSTR(REVERSE(d.file_name),INSTR(REVERSE(d.file_name),'/')+1))) 
                    from dba_data_files d,dual;
        '''

    dir_sql = _REMOTE_TAG_COMMAND % dir_sql_tmp
    res = ssh_input(tag_args,dir_sql)[0].replace('\n','')
    schemas = "','".join(list(set([i.split('.')[0] for i in src_cmd_args[3].split(',')]))).upper()

    select_tbs_sql = "select distinct DEFAULT_TABLESPACE from dba_users where username in ('%s'); "%(schemas)
    tbs_res = ssh_input(src_args,_REMOTE_COMMAND % select_tbs_sql)
    tbs_res = [i for i in tbs_res if i!='\n']
    init_tbs_sql_dir=[]
    for tbs_name in tbs_res:
        tbs_name = tbs_name.replace('\n','')
        select_datafile_sql = "SELECT round(bytes/1024/1024/1024,0)  \
            from dba_data_files where  tablespace_name='%s';"%tbs_name
        datafile_res = ssh_input(src_args,_REMOTE_COMMAND % select_datafile_sql)
        datafile_res = [i for i in datafile_res if i!='\n']
        create_tbs_sql = "create tablespace %s datafile '%s/%s01.dbf' size 1g autoextend on;"%(tbs_name,res,tbs_name)
        init_tbs_sql_dir.append(create_tbs_sql)
        for index,dbfile_size in enumerate(datafile_res[1:]):
            index = index+2
            add_db_file_sql = "alter tablespace %s add datafile '%s/%s%s.dbf' size 1g autoextend on;"%(tbs_name,res,tbs_name,index)
            init_tbs_sql_dir.append(add_db_file_sql)
    init_tbs_sql = _REMOTE_TAG_COMMAND % '\n'.join(init_tbs_sql_dir)
    init_tbs_res = ssh_input(tag_args,init_tbs_sql)
    print("init tbs success")
    


    return "create tbs success"


# get a scn and run the cmd of expdp
@log
def full_trans(src_args,src_cmd_args,_REMOTE_COMMAND,tag_args,tag_cmd_args,_REMOTE_TAG_COMMAND):

    # get scn and schemas
    select_scn_sql = _REMOTE_COMMAND %'''select to_char(current_scn) from v\$database;'''
    scn = ssh_input(src_args,select_scn_sql)[0].replace('\n','')

    exp_time = time.strftime("%Y%m%d%H%M", time.localtime())

    schemas = ",".join(list(set([i.split('.')[0] for i in src_cmd_args[3].split(',')]))).upper()

    # create directory of expdp
    exp_dir = src_cmd_args[5]
    create_dir_sql = '''create or replace directory expdp_ogg as '%s';
 grant read,write on directory expdp_ogg to public;
'''%exp_dir

    create_dir_res = ssh_input(src_args,_REMOTE_COMMAND %create_dir_sql)

    # add a new undo undofile on source database
    dir_sql_tmp = '''
                SELECT distinct(REVERSE(SUBSTR(REVERSE(d.file_name),INSTR(REVERSE(d.file_name),'/')+1))) 
                    from dba_data_files d,dual;
    '''

    dir_sql = _REMOTE_COMMAND % dir_sql_tmp
    dir_res = ssh_input(src_args,dir_sql)[0].replace('\n','')
    # select_undo_sql = "select count(*),tablespace_name from dba_data_files \
    #     where tablespace_name =(\
    #         select VALUE from v\$system_parameter where name ='undo_tablespace') group by tablespace_name;"
    # undo_res = ssh_input(src_args,_REMOTE_COMMAND % select_undo_sql)[0].split(' ')
    # undo_list = [i for i in undo_res if i != '']
    # undo_name = undo_list[1].replace('\n','')
    # undo_num = int(undo_list[0])+1
    # add_undo_sql = "alter tablespace %s add datafile '%s/%s%s.dbf' size 10m autoextend on ; "\
    #     %(undo_name,dir_res,undo_name,undo_num)

    # ssh_input(src_args,_REMOTE_COMMAND % add_undo_sql)

    # get cmd of expdp
    dmp_file_dir = 'full_ogg_%s.dmp' % (exp_time)
    expdp_cmd = '''su - %s <<EOF
export ORACLE_SID=%s
expdp "'"/ as sysdba"'" schemas=%s directory=expdp_ogg flashback_scn=%s dumpfile=%s logfile=full_ogg_%s.log
EOF'''%(src_cmd_args[0],src_cmd_args[1],schemas,scn,dmp_file_dir,exp_time)
    


    schemas = ",".join(list(set([i.split('.')[0] for i in tag_cmd_args[3].split(',')]))).upper()
    schemas_tmp = "','".join(list(set([i.split('.')[0] for i in tag_cmd_args[3].split(',')]))).upper()
    # create directory of impdp
    imp_dir = tag_cmd_args[5]
    create_dir_sql = '''create or replace directory impdp_ogg as '%s';
 grant read,write on directory impdp_ogg to public;
'''%imp_dir

    create_dir_res = ssh_input(tag_args,_REMOTE_TAG_COMMAND %create_dir_sql)

   

    # get cmd of impdp
    remote_dmp_file = imp_dir + '/' + dmp_file_dir.split('/')[-1]
    imp_time = time.strftime("%Y%m%d%H%M", time.localtime())
    dmp_file = remote_dmp_file.split('/')[-1]

    impdp_cmd = '''su - %s <<EOF
export ORACLE_SID=%s
impdp "'"/ as sysdba"'" schemas=%s directory=impdp_ogg dumpfile=%s logfile=full_ogg_%s.log table_exists_action=replace
EOF'''%(tag_cmd_args[0],tag_cmd_args[1],schemas,dmp_file,imp_time)
    
    


    # disable the trigger and job on target.
    select_disable_sql = _REMOTE_TAG_COMMAND % '''select 'alter trigger '||owner||'.'||trigger_name||' disable;'  from dba_triggers where owner in ('%s') and status='ENABLED';'''%schemas_tmp
    select_remove_job_sql = _REMOTE_TAG_COMMAND % '''select 'exec dbms_job.remove('||JOB||');' from dba_jobs where SCHEMA_USER in ('%s') and BROKEN='N';'''%schemas_tmp

    # start process of replicate

    start_rep_cmd = 'start mc_rep atcsn %s' %scn
   

    # # get os system 
    # os_name = ssh_input(tag_args,"uname")[0].replace('\n','')
    print("\nINFO:源库开始导出:")
    print(expdp_cmd)
    expdp_res = ssh_input(src_args,expdp_cmd)

    # #get sha256sum of dmpfile
    dmp_file_dir = '%s/%s' % (exp_dir,dmp_file_dir)

    # sha256sum = get_file_sha256(dmp_file_dir)

    #get dmpfile from srouce
    print("\nINFO:开始传输导出文件到目标端")
    
    ssh_scp(src_args,tag_args,dmp_file_dir,remote_dmp_file)
    ssh_input(tag_args,f"chown {tag_cmd_args[0]} {remote_dmp_file}")

    #sha_local = get_file_sha256(local_dmp_file)
    print("\nINFO:目标库开始导入：")
    print(impdp_cmd)
    impdp_res = ssh_input(tag_args,impdp_cmd)

    # disable the trigger and job on target.
    select_tri_res = ''.join(ssh_input(tag_args,select_disable_sql))
    disable_tri_res = ssh_input(tag_args,_REMOTE_TAG_COMMAND % select_tri_res)

    select_job_res = ''.join(ssh_input(tag_args,select_remove_job_sql))
    remove_job_res = ssh_input(tag_args,_REMOTE_TAG_COMMAND % select_job_res)

    # start process of replicate
    print (f"You could run this cmd in ggsci if you want to start replicate.\n{start_rep_cmd}")
    return 'impdp complete.'
    # else:

    #     print("The OS system is %s.\nYou should run the commands by yourself:\n"%os_name)
    #     print("The expdp command on source database:%s\n"%expdp_cmd)
    #     print("The impdp command on target database:%s\n"%impdp_cmd)
    #     print("When impdp complete, you should run the command to get the sql of disable triggers and remove jobs:\n%s\n%s "%(select_disable_sql,select_remove_job_sql))
    #     print("Finally, you can start the replicate use the command :<%s>\n"%start_rep_cmd)
    #     return "AIX full trans"










# the main function of full_ogg
@log
def full_ogg(src_args,tag_args,src_cmd_args,tag_cmd_args,_REMOTE_COMMAND,_REMOTE_TAG_COMMAND,_REMOTE_GGSCI_COMMAND):
    start_prc(src_args,tag_args,src_cmd_args,_REMOTE_GGSCI_COMMAND)
    #create_tbs(src_args,tag_args,src_cmd_args,_REMOTE_COMMAND,_REMOTE_TAG_COMMAND)
    p = Process(target=create_tbs,args=(src_args,tag_args,src_cmd_args,_REMOTE_COMMAND,_REMOTE_TAG_COMMAND,))
    p.start()
    full_trans(src_args,src_cmd_args,_REMOTE_COMMAND,tag_args,tag_cmd_args,_REMOTE_TAG_COMMAND)


