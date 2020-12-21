 
 #!/usr/bin/env python
import os
import time
import re
from logging_method import log
from ssh_input import ssh_input
from method import res_table,get_ssh_list,create_dir,parse_prm_rep,del_null_list
from validate_ogg import create_dblink

# 获取scn
@log
def get_scn(src_args,tag_args,_REMOTE_COMMAND,_REMOTE_TAG_COMMAND):
    get_scn_sql = "select to_char(current_scn) from v\$database;"
    src_scn = ssh_input(src_args,_REMOTE_COMMAND%get_scn_sql)
    tag_scn = ssh_input(tag_args,_REMOTE_TAG_COMMAND%get_scn_sql)
    if src_scn == [] or tag_scn ==[]:
        print("\nINFO:数据库获取scn失败，请查看日志:ogg.log")
        return "scn erro"
    else:
        print("\nINFO:获取数据库scn号成功")
        return min(src_scn[0],tag_scn[0])



# 获取表大小等信息
@log
def get_tb_info(src_args,_REMOTE_COMMAND,reinital_tables):
    get_info_sql_init = '''select owner, segment_name, sum(bytes) / 1024 / 1024 / 1024 as BYTS_GB
  from dba_segments
 where segment_name ='%s'
   and owner = '%s'
   AND segment_type = 'TABLE'
group by owner,segment_name;'''

    

    re_tbs = [tb.split('.') for tb in reinital_tables.upper().split(',')]
    re_tbs_list = []
    for re_tb_info in re_tbs:
        get_info_sql = get_info_sql_init % (re_tb_info[1],re_tb_info[0])
        res = ssh_input(src_args,_REMOTE_COMMAND % get_info_sql)[0].replace('\n','')
        res_list = del_null_list(res.split(' '))
        re_tbs_list.append(res_list)
    tbs_table = res_table(re_tbs_list,['用户名','表名','表大小(G)'])
    print(f"\nINFO:重新初始化表信息如下:\n{tbs_table}")
    owners = str(tuple(set([b[0] for b in re_tbs ]))).replace(',)',')')
    tbs = str(tuple(set([b[1] for b in re_tbs ]))).replace(',)',')')
    get_unsupport_tbs_sql = '''
    select owner, table_name, column_name, data_type
from dba_tab_columns
where table_name in %s
and owner in %s
and data_type  in ('BLOB', 'CLOB', 'LONG', 'LONG RAW');'''%(tbs,owners)

    unsup_tbs = ssh_input(src_args,_REMOTE_COMMAND % get_unsupport_tbs_sql)
    if unsup_tbs != []:

        unsup_tbs_tmp = [c.replace('\n','').split(' ') for c in unsup_tbs]
        unsup_tbs_list = [del_null_list(d) for d in unsup_tbs_tmp]
        uns_table = res_table(unsup_tbs_list,['用户名', '表名' ,'字段名', '字段类型'])
        print(f"\nINFO:以下为包含不支持类型字段的表信息{uns_table}")
        return unsup_tbs_list
    else:
        return "none support tb"

    
# 目标端关闭应用进程并获取scn
@log
def stop_rep_scn(src_args,tag_args,tag_other_args,ogg_rep,_REMOTE_COMMAND, _REMOTE_TAG_COMMAND):
    
    _REMOTE_GGSCI_COMMAND = '''
su - %s <<EOF
export ORACLE_SID=%s
export LD_LIBRARY_PATH=%s:\$ORACLE_HOME/lib
export LIBPATH=%s:\$ORACLE_HOME/lib
%s/ggsci
%%s
exit
EOF
'''%(tag_other_args[0],tag_other_args[1],tag_other_args[2],tag_other_args[2],tag_other_args[2])

    print(f"\nINFO:停止目标端应用进程 {ogg_rep}")
    stop_res = ssh_input(tag_args,_REMOTE_GGSCI_COMMAND % f"stop {ogg_rep}")
    print(''.join(stop_res))
    time.sleep(5)
    info_res = ssh_input(tag_args,_REMOTE_GGSCI_COMMAND % f"info {ogg_rep}")
    print('\n'*3)
    print(''.join(info_res))
    rep_status = [str for str in info_res if f"{ogg_rep.upper()}" in str.upper()]
    if "STOPPED" in rep_status[0]:
        print(f"\nINFO:进程{ogg_rep}正常停止，开始获取进程当前scn号")
        info_rep_res = ssh_input(tag_args,_REMOTE_GGSCI_COMMAND % f"info {ogg_rep} showch")
        print('\n'*3)
        print(''.join(info_rep_res))
        csn_str = [str1 for str1 in info_rep_res if f"Latest CSN of finished TXNs:" in str1]
        if csn_str == []:
            print("\nWARRING:应用进程scn获取失败,开始从数据库获取scn")

            scn = get_scn(src_args, tag_args, _REMOTE_COMMAND, _REMOTE_TAG_COMMAND)
        else:
            scn_tmp = re.findall(r'\d+', csn_str[0])
 
            if scn_tmp == []:
                print("\nWARRING:应用进程scn获取失败,开始从数据库获取scn")
                scn = get_scn(src_args, tag_args, _REMOTE_COMMAND, _REMOTE_TAG_COMMAND)
            else:
                scn = scn_tmp[0]

        return scn
    else:
        print(f"\nINFO:进程{ogg_rep}未正常停止，请检查进程")
        return "stop error"

# 创建dblink，directory,从源端导入数据到目标端
@log
def impdp_tag(src_args,tag_args,tag_other_args,reinit_args,scn,_REMOTE_COMMAND,_REMOTE_TAG_COMMAND):
    src_ogg_user,src_ogg_passwd,src_db_port,tag_dmp_dir,reinitial_tables,ogg_rep = reinit_args

    create_dbl_res = create_dblink(src_args,tag_args,reinit_args[0:4],_REMOTE_COMMAND,_REMOTE_TAG_COMMAND)
    if create_dbl_res is not False:
        create_dir_res = create_dir(tag_args,reinit_args[0:4],_REMOTE_TAG_COMMAND)
        if create_dir_res == 'create dir s':
            imp_time = time.strftime("%Y%m%d%H%M", time.localtime())
            
            impdp_cmd = '''su - %s <<EOF
export ORACLE_SID=%s
impdp "'"/ as sysdba"'"   tables=%s directory=ops_expdp logfile=imptabs_ogg_%s.log network_link= ops_dblink table_exists_action=replace exclude=jobs,trigger	 flashback_scn=%s
EOF'''%(tag_other_args[0],tag_other_args[1],reinitial_tables,imp_time,scn)
            print("\nINFO:目标库开始导入：")
            print(impdp_cmd)
            impdp_res = ssh_input(tag_args,impdp_cmd)
            print(''.join(impdp_res))
            if "successfully " in ''.join(impdp_res):
                print("\nINFO:数据重新初始化成功")
                return "impdp s"
            else:
                print("\nINFO:数据重新初始化失败，请检查日志")
                return "impdp f"
        else:
            return create_dir_res
    else:
        return create_dir_res


# 启动应用进程
@log
def start_rep_prc(tag_args,tag_other_args,reinit_args,_REMOTE_TAG_COMMAND):
    
    _REMOTE_GGSCI_COMMAND = '''
su - %s <<EOF
export ORACLE_SID=%s
export LD_LIBRARY_PATH=%s:\$ORACLE_HOME/lib
export LIBPATH=%s:\$ORACLE_HOME/lib
%s/ggsci
%%s
exit
EOF
'''%(tag_other_args[0],tag_other_args[1],tag_other_args[2],tag_other_args[2],tag_other_args[2])

    ogg_dir = tag_other_args[2]
    src_ogg_user,src_ogg_passwd,src_db_port,tag_dmp_dir,reinital_tables,ogg_rep = reinit_args
    print("\nINFO:应用进程参数修改")
    ori_text = ssh_input(tag_args,f"cat {ogg_dir}/dirprm/{ogg_rep}.prm")
    modify_text = [txt for txt in ori_text if 'mapexclude' not in txt]
    bak_time = time.strftime("%Y%m%d%H%M", time.localtime())
    ssh_input(tag_args,f"cp {ogg_dir}/dirprm/{ogg_rep}.prm {ogg_dir}/dirprm/{ogg_rep}.prm_bak_{bak_time}\necho '''{''.join(modify_text)}'''>{ogg_dir}/dirprm/{ogg_rep}.prm")

    
    print(f"\nINFO:启动目标端应用进程 {ogg_rep}")
    start_res = ssh_input(tag_args,_REMOTE_GGSCI_COMMAND % f"start {ogg_rep}")
    print(''.join(start_res))
    time.sleep(3)
    info_res = ssh_input(tag_args,_REMOTE_GGSCI_COMMAND % f"info {ogg_rep}")
    print('\n'*2)
    print(''.join(info_res))
    rep_status = [str for str in info_res if f"{ogg_rep.upper()}" in str.upper()]
    if "RUNNING" in rep_status[0]:
        print(f"\nINFO:进程{ogg_rep}启动正常")
    else:
        print(f"\nINFO:进程{ogg_rep}启动失败")



@log
def reinitial_tb(src_args,tag_args,tag_other_args,reinit_args,_REMOTE_COMMAND, _REMOTE_TAG_COMMAND):
    _,_,_,_,reinital_tables,ogg_rep = reinit_args
    unsup_tb = get_tb_info(src_args,_REMOTE_COMMAND,reinital_tables)
    if unsup_tb == 'none support tb':
        scn = stop_rep_scn(src_args,tag_args,tag_other_args,ogg_rep,_REMOTE_COMMAND, _REMOTE_TAG_COMMAND)
        if 'error' not in scn :
            impdp_res = impdp_tag(src_args,tag_args,tag_other_args,reinit_args,scn,_REMOTE_COMMAND,_REMOTE_TAG_COMMAND)
            if impdp_res == 'impdp s':
                start_rep_prc(tag_args,tag_other_args,reinit_args,_REMOTE_TAG_COMMAND)
            else:
                return impdp_res
        else:
            return scn
    else:
        return unsup_tb

    