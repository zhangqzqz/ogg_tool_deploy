#!/usr/bin/env python

from ssh_input import ssh_input
import time
import os
import re
from logging_method import log

# init the mgr and user for ogg
@log
def src_ogg_mgr(src_args,tag_args,src_cmd_args,_REMOTE_COMMAND,_REMOTE_GGSCI_COMMAND):
    print("\nINFO:配置源端ogg服务")
    # create ogg user and grant to user
    dir_sql = _REMOTE_COMMAND %"SELECT distinct(REVERSE(SUBSTR(REVERSE(d.file_name),INSTR(REVERSE(d.file_name),'/')+1))) \
                    from dba_data_files d,dual;"
    db_dir = ssh_input(src_args,dir_sql)[0].replace('\n','')
    create_user_sql = '''create tablespace mc_odc_tps datafile '%s/mc_odc01.dbf' size 100M autoextend on;
create user mc_odc identified by mc_odc default tablespace mc_odc_tps;
GRANT CONNECT TO mc_odc;
GRANT ALTER ANY TABLE TO mc_odc;
GRANT ALTER SESSION TO mc_odc;
GRANT CREATE SESSION TO mc_odc;
GRANT FLASHBACK ANY TABLE TO mc_odc;
GRANT SELECT ANY DICTIONARY TO mc_odc;
GRANT SELECT ANY TABLE TO mc_odc;
GRANT RESOURCE TO mc_odc;
GRANT DBA TO mc_odc;
alter user mc_odc quota unlimited on users;
'''%db_dir
    ssh_input(src_args,_REMOTE_COMMAND % create_user_sql)

    # create subdirs
    create_subdirs_cmd = _REMOTE_GGSCI_COMMAND % "create subdirs"
    ssh_input(src_args,create_subdirs_cmd)


    # create process mgr and start it.
    print("######Create and start the process MGR.")
    create_mgr_cmd = '''
su - %s <<EOF
export ORACLE_SID=%s
rm -f %s/dirprm/mgr.prm
rm -f %s/GLOBALS
echo 'port 7809
DYNAMICPORTLIST 7800-7810
PURGEOLDEXTRACTS ./dirdat/mc*, USECHECKPOINTS, MINKEEPHOURS 24
autorestart extract * retries 10 waitminutes 10'>>%s/dirprm/mgr.prm
echo 'GGSCHEMA mc_odc
CHECKPOINTTABLE mc_odc.ggs_checkpoint '>>%s/GLOBALS
EOF'''%(src_cmd_args[0],src_cmd_args[1],src_cmd_args[2],src_cmd_args[2],src_cmd_args[2],src_cmd_args[2])
    ssh_input(src_args,create_mgr_cmd)
    start_mgr_cmd = _REMOTE_GGSCI_COMMAND % "start mgr"
    start_mgr_res = ssh_input(src_args,start_mgr_cmd)
    print(''.join(start_mgr_res))
    return "init mgr complete"
    
# init ddl script for ogg
@log
def src_ogg_ddl(src_args,tag_args,src_cmd_args,_REMOTE_COMMAND,_REMOTE_GGSCI_COMMAND):
    ddl_y_n = input("You should turn off all seesions of oracle when you run the sql of ddl .\nDo you want to continue? Y/N")
    if ddl_y_n.upper()=='Y':
        # grant to mc_odc for ddl setup
        grant_ddl_sql = _REMOTE_COMMAND % '''grant create any table to mc_odc;
grant create any view to mc_odc;
grant create any procedure to mc_odc;
grant create any sequence to mc_odc;
grant create any index to mc_odc;
grant create any trigger to mc_odc;
grant create any view to mc_odc;
GRANT EXECUTE ON UTL_FILE TO mc_odc;'''
        ssh_input(src_args,grant_ddl_sql)

        #run the sql of marker_setup
        print("######Run the script of marker_setup.")
        run_marker_setup = _REMOTE_COMMAND % "@%s/marker_setup\nmc_odc\n" % src_cmd_args[2]
        run_marker_res = ssh_input(src_args,run_marker_setup)
        print(''.join(run_marker_res))

        # run the sql of ddl_setup

        # if version is 10g,turn off the recyclebin
        check_version_bin_sql = _REMOTE_COMMAND % "select distinct(VERSION) from product_component_version;\nshow parameter recyclebin"
        check_res = ''.join(ssh_input(src_args,check_version_bin_sql))
        do_off_bin = ''
        if '10' in check_res and 'ON' in check_res:
            recylebin_yn = input("The version of oracle is 10g,and you should turn off the recyclebin.Do you want continue?Y/N")
            if recylebin_yn.upper()=='Y':
                recylebin_off_sql = _REMOTE_COMMAND % "alter system set recyclebin=off;"
                ssh_input(src_args,recylebin_off_sql)
                do_off_bin = 'do'
            else:
                print("User choose to turn off recyclebin by themself,the sql is:\n<alter system set recyclebin=off;>")
                do_off_bin = ''
                return "recyclebin"
        print("######Run the script of ddl_setup.")
        run_ddl_setup = _REMOTE_COMMAND % "@%s/ddl_setup\nmc_odc\n" % src_cmd_args[2]
        run_ddl_res = ssh_input(src_args,run_ddl_setup)
        print(''.join(run_ddl_res))

        if 'ORA-06512:' in ''.join(run_ddl_res):
            exists_user = re.findall(r'\((\D+)\)',''.join(run_ddl_res))[0]
            run_remove_ddl = _REMOTE_COMMAND % "@%s/ddl_remove\n%s\n" % (src_cmd_args[2],exists_user)
            run_remove_res = ssh_input(src_args,run_remove_ddl)
            print(''.join(run_remove_res))
            run_ddl_setup = _REMOTE_COMMAND % "@%s/ddl_setup\nmc_odc\n" % src_cmd_args[2]
            run_ddl_res = ssh_input(src_args,run_ddl_setup)
            print(''.join(run_ddl_res))
        
        #run the sql of role_setup
        print("######Run the script of role_setup.")
        run_role_setup = _REMOTE_COMMAND % "@%s/role_setup\nmc_odc\n" % src_cmd_args[2]
        run_role_res = ssh_input(src_args,run_role_setup)
        print(''.join(run_role_res))
        grant_mc_odc_sql = _REMOTE_COMMAND % "GRANT GGS_GGSUSER_ROLE TO mc_odc;" 
        ssh_input(src_args,grant_mc_odc_sql)
        print("GRANT GGS_GGSUSER_ROLE TO mc_odc\nGrant role to mc_odc complete.")

        #run the sql of ddl_enable
        print("######Run the script of ddl_enable.")
        run_ddl_enable = _REMOTE_COMMAND % "@%s/ddl_enable" % src_cmd_args[2]
        run_ddl_enable = ssh_input(src_args,run_ddl_enable)
        print(''.join(run_ddl_enable))
        print('DDL enable script complete')

        #run the sql of ddl_enable
        print("######Run the script of ddl_enable.")
        run_ddl_enable = _REMOTE_COMMAND % "@%s/ddl_enable" % src_cmd_args[2]
        run_ddl_enable = ssh_input(src_args,run_ddl_enable)
        print(''.join(run_ddl_enable))
        print('DDL enable script complete')

        #run the sql of ddl_pin
        print("######Run the script of ddl_pin.")
        run_ddl_pin = _REMOTE_COMMAND % "@%s/ddl_pin\nmc_odc\nmc_odc" % src_cmd_args[2]
        run_ddl_pin = ssh_input(src_args,run_ddl_pin)
        print(''.join(run_ddl_pin))
        print('DDL pin script complete')

    else:
        print("User choose to skip the oparation of run ddl script.")

    # add supplement log for table
    print("######Add supplement log for table.")
    add_sup_y_n = input("Do you want to add supplement log now? y/n ")
    add_sup_cmd = 'dblogin userid mc_odc password mc_odc\nadd trandata '+ src_cmd_args[3].replace(',',';\nadd trandata ')

    if add_sup_y_n.upper()=='Y': 
        add_count = 0
        while add_count<3:
            ssh_input(src_args,_REMOTE_GGSCI_COMMAND %  add_sup_cmd)
            add_count +=1
            time.sleep(10)
        info_sup_cmd = 'dblogin userid mc_odc password mc_odc\ninfo trandata '+ src_cmd_args[3].replace(',',';\ninfo trandata ')
        info_sup_res = ssh_input(src_args,_REMOTE_GGSCI_COMMAND % info_sup_cmd)

        disable_sup_res = [res for res in info_sup_res if 'disable' in res]
        if disable_sup_res!=[]:
            print(''.join(disable_sup_res))
        else:
            print("Logging of supplemental redo log data is enabled for table all.")
    else:
        print("User choose add supplement log another time.")
        print("The cmd are:\n<%s>"%add_sup_cmd)
    
    return 'init ddl/sup complete',do_off_bin,check_res

#init process of ogg for  single instance
@log
def src_ogg_prc_single(src_args,tag_args,src_cmd_args,_REMOTE_COMMAND,_REMOTE_GGSCI_COMMAND,do_off_bin):
    # create the process extract and start it.
    print("######Create process extract.")
    add_ext_cmd = _REMOTE_GGSCI_COMMAND % "add extract mc_ext tranlog begin now\nadd exttrail ./dirdat/mc extract mc_ext"
    add_ext_res = ssh_input(src_args,add_ext_cmd)
    print(''.join(add_ext_res))

    # get archivelog dest
    select_arch_dest_cmd = _REMOTE_COMMAND % "archive log list"
    arch_dest_tmp = [arch for arch in ssh_input(src_args,select_arch_dest_cmd) if 'Archive destination' in arch]
    arch_dest = arch_dest_tmp[0].split(' ')[-1].replace('\n','')
    

    ddl_obj_cmd_tmp =  ".* exclude objtype 'TRIGGER' &\ninclude objname ".join(list(set([i.split('.')[0] for i in src_cmd_args[3].split(',')])))
    ddl_obj_cmd = "ddl include objname %s.* exclude objtype 'TRIGGER'  "%ddl_obj_cmd_tmp

    table_obj_tmp = src_cmd_args[3].replace(',',';\ntable ')
    table_obj_cmd = "table %s ;\n"%table_obj_tmp
    
    edit_ext_cmd = '''
su - %s <<EOF
export ORACLE_SID=%s
rm -f %s/dirprm/mc_ext.prm
echo 'extract mc_ext
setenv (NLS_LANG="SIMPLIFIED CHINESE_CHINA.ZHS16GBK")
userid mc_odc,password mc_odc
exttrail ./dirdat/mc
tranlogoptions altarchivelogdest %s
FETCHOPTIONS FETCHPKUPDATECOLS
%s
%s
'>>%s/dirprm/mc_ext.prm
EOF'''%(src_cmd_args[0],src_cmd_args[1],src_cmd_args[2],arch_dest,ddl_obj_cmd,table_obj_cmd,src_cmd_args[2])

    ssh_input(src_args,edit_ext_cmd)


    # create the process dmp 

    print("######Create process dmp.")
    add_dmp_cmd = _REMOTE_GGSCI_COMMAND % "add extract mc_dmp EXTTRAILSOURCE ./dirdat/mc\nADD RMTTRAIL ./dirdat/mc, EXTRACT mc_dmp"
    add_dmp_res = ssh_input(src_args,add_dmp_cmd)
    print(''.join(add_dmp_res))

    edit_dmp_cmd = '''
su - %s <<EOF
export ORACLE_SID=%s
rm -f %s/dirprm/mc_dmp.prm
echo 'extract mc_dmp
userid mc_odc,password mc_odc
rmthost %s, mgrport 7809
rmttrail ./dirdat/mc
passthru
%s
'>>%s/dirprm/mc_dmp.prm
EOF'''%(src_cmd_args[0],src_cmd_args[1],src_cmd_args[2],tag_args[0],table_obj_cmd,src_cmd_args[2])

    ssh_input(src_args,edit_dmp_cmd)



    # check recycle bin

    if do_off_bin == 'do':
        recylebin_on_sql = _REMOTE_COMMAND % "alter system set recyclebin=on;"
        ssh_input(src_args,recylebin_on_sql)
    return "init process complete."


#init process of ogg for  rac
@log
def src_ogg_prc_rac(src_args,tag_args,src_cmd_args,_REMOTE_COMMAND,_REMOTE_GGSCI_COMMAND,do_off_bin,inst_num,check_res):

#     # create the process extract and start it.
    print("######Create process extract.")
    add_ext_cmd = _REMOTE_GGSCI_COMMAND % f"add extract mc_ext tranlog  threads {inst_num} begin now\nadd exttrail ./dirdat/mc extract mc_ext"
    add_ext_res = ssh_input(src_args,add_ext_cmd)
    print(''.join(add_ext_res))

    # get archivelog dest
    select_arch_dest_cmd = _REMOTE_COMMAND % "archive log list"
    arch_dest_tmp = [arch for arch in ssh_input(src_args,select_arch_dest_cmd) if 'Archive destination' in arch]
    arch_dest = arch_dest_tmp[0].split(' ')[-1].replace('\n','')
    

    ddl_obj_cmd_tmp =  ".* exclude objtype 'TRIGGER' &\ninclude objname ".join(list(set([i.split('.')[0] for i in src_cmd_args[3].split(',')])))
    ddl_obj_cmd = "ddl include objname %s.* exclude objtype 'TRIGGER'  "%ddl_obj_cmd_tmp

    table_obj_tmp = src_cmd_args[3].replace(',',';\ntable ')
    table_obj_cmd = "table %s ;\n"%table_obj_tmp

    # if version is 11g rac:
    if '11' in check_res:
        edit_ext_cmd = '''
su - %s <<EOF
export ORACLE_SID=%s
rm -f %s/dirprm/mc_ext.prm
echo 'extract mc_ext
setenv (NLS_LANG="SIMPLIFIED CHINESE_CHINA.ZHS16GBK")
userid mc_odc,password mc_odc
exttrail ./dirdat/mc
tranlogoptions dblogreader
FETCHOPTIONS FETCHPKUPDATECOLS
%s
%s
'>>%s/dirprm/mc_ext.prm
EOF'''%(src_cmd_args[0],src_cmd_args[1],src_cmd_args[2],ddl_obj_cmd,table_obj_cmd,src_cmd_args[2])
        ssh_input(src_args,edit_ext_cmd)

    # if version is 10g rac    
    elif '10' in check_res:
        # write tns to tnsnames.ora and backup the old file.
        get_tns_txt_cmd = '''
    su - %s<<EOF
    cat \$ORACLE_HOME/network/admin/tnsnames.ora
    EOF
        '''%(src_cmd_args[0])
        get_tns_txt = ''.join(ssh_input(src_args,get_tns_txt_cmd))
        asm_sid_num = ssh_input(src_args,'ps -ef|grep pmon|grep +ASM')[0].replace('\n','')[-1]
        modify_time = time.strftime("%Y-%m-%d-%H-%M-%S", time.localtime()) 
        modify_tns_cmd = f'''su - {src_cmd_args[0]}<<EOF
    mv \$ORACLE_HOME/network/admin/tnsnames.ora \$ORACLE_HOME/network/admin/tnsnames.ora{modify_time}
    echo '{get_tns_txt}
    MC_ASM =
    (DESCRIPTION =
        (ADDRESS = (PROTOCOL = TCP)(HOST = {src_args[0]})(PORT = 1521))
        (CONNECT_DATA =
        (SERVER = DEDICATED)
            (SERVICE_NAME = +ASM)
            (INSTANCE_NAME = +ASM{asm_sid_num})
    )
    )'>>\$ORACLE_HOME/network/admin/tnsnames.ora
    EOF'''
        ssh_input(src_args,modify_tns_cmd)
        check_tns = ''.join(ssh_input(src_args,f'su - {src_cmd_args[0]}<<EOF\ntnsping mc_asm\nEOF'))
        print(check_tns)
        if 'OK' not in check_tns:
            print(check_tns)
            os._exit(0)
        else:
            pass
        edit_ext_cmd = '''
        su - %s <<EOF
        export ORACLE_SID=%s
        rm -f %s/dirprm/mc_ext.prm
        echo 'extract mc_ext
        setenv (NLS_LANG="SIMPLIFIED CHINESE_CHINA.ZHS16GBK")
        userid mc_odc,password mc_odc
        exttrail ./dirdat/mc
        tranlogoptions altarchivelogdest %s
        tranlogoptions asmuser sys@mc_asm, asmpassword %s
        FETCHOPTIONS FETCHPKUPDATECOLS
        %s
        %s
        '>>%s/dirprm/mc_ext.prm
        EOF'''%(src_cmd_args[0],src_cmd_args[1],src_cmd_args[2],arch_dest,src_cmd_args[4],ddl_obj_cmd,table_obj_cmd,src_cmd_args[2])
    else:
        print("Please check the database version.")
        os._exit(0)

        # create the process dmp 
        
    print("######Create process dmp.")
    add_dmp_cmd = _REMOTE_GGSCI_COMMAND % "add extract mc_dmp EXTTRAILSOURCE ./dirdat/mc\nADD RMTTRAIL ./dirdat/mc, EXTRACT mc_dmp"
    add_dmp_res = ssh_input(src_args,add_dmp_cmd)
    print(''.join(add_dmp_res))

    edit_dmp_cmd = '''
su - %s <<EOF
export ORACLE_SID=%s
rm -f %s/dirprm/mc_dmp.prm
echo 'extract mc_dmp
userid mc_odc,password mc_odc
rmthost %s, mgrport 7809
rmttrail ./dirdat/mc
passthru
%s
'>>%s/dirprm/mc_dmp.prm
EOF'''%(src_cmd_args[0],src_cmd_args[1],src_cmd_args[2],tag_args[0],table_obj_cmd,src_cmd_args[2])

    ssh_input(src_args,edit_dmp_cmd)


    # check recycle bin

    if do_off_bin == 'do':
        recylebin_on_sql = _REMOTE_COMMAND % "alter system set recyclebin=on;"
        ssh_input(src_args,recylebin_on_sql)
    return "init process complete."


#init ogg
@log
def src_ogg(src_args,tag_args,src_cmd_args,_REMOTE_COMMAND):
    _REMOTE_GGSCI_COMMAND = '''
su - %s <<EOF
export ORACLE_SID=%s
export LD_LIBRARY_PATH=%s:\$ORACLE_HOME/lib
export LIBPATH=%s:\$ORACLE_HOME/lib
%s/ggsci
%%s
exit
EOF
'''%(src_cmd_args[0],src_cmd_args[1],src_cmd_args[2],src_cmd_args[2],src_cmd_args[2])
    select_instnum_sql = "show parameter cluster_database_instances"
    print("\nINFO:获取源端实例信息")
    inst_num = int(re.findall(r"\d+\.?\d*",''.join(ssh_input(src_args,_REMOTE_COMMAND % select_instnum_sql)))[0])
    int_mgr_res = src_ogg_mgr(src_args,tag_args,src_cmd_args,_REMOTE_COMMAND,_REMOTE_GGSCI_COMMAND)
    if int_mgr_res =='init mgr complete':
        int_ddl_res,do_off_bin,check_res = src_ogg_ddl(src_args,tag_args,src_cmd_args,_REMOTE_COMMAND,_REMOTE_GGSCI_COMMAND)
        if int_ddl_res =='init ddl/sup complete':
            if inst_num==1:
                init_prc_res = src_ogg_prc_single(src_args,tag_args,src_cmd_args,_REMOTE_COMMAND,_REMOTE_GGSCI_COMMAND,do_off_bin)
            else:
                
                init_prc_res = src_ogg_prc_rac(src_args,tag_args,src_cmd_args,_REMOTE_COMMAND,_REMOTE_GGSCI_COMMAND,do_off_bin,inst_num,check_res)
        else:
            return "init ddl/sup uncomplete"
    else:
            return "init mgr uncomplete"




