 
 #!/usr/bin/env python
import os
import time
import re
from logging_method import log
from ssh_input import ssh_input
from method import res_table,get_ssh_list,create_dir,parse_prm_rep,del_null_list


# 获取挖掘进程文件的userid及passwd
@log
def get_userid_passwd(src_args,src_other_args,ext_name):
    print("\nINFO:获取源库userid及password")
    ext_prm_str = ssh_input(src_args,f"cat {src_other_args[2]}/dirprm/{ext_name}.prm")
    userid_str = [i for i in ext_prm_str if 'USERID' in i.upper() and 'PASSWORD' in i.upper() and '#' not in i and '-' not in i][0].lower()
    userid = userid_str.split('password')[0].split(' ')[-1].replace(',','')
    ogg_passwd = userid_str.split('password')[-1].replace(' ','')
    return userid,ogg_passwd

# 获取挖掘进程文件同步对象
@log
def get_ext_obj(src_args,src_other_args,ext_name):
    print("\nINFO:获取源库挖掘对象")
    ext_prm_str = ssh_input(src_args,f"cat {src_other_args[2]}/dirprm/{ext_name}.prm")
    obj_list = [i.lower().replace('table','').replace(';','') for i in ext_prm_str if i.upper()[0:6] == 'TABLE ']
    return ','.join(obj_list)

# 类unix系统，oracle 添加附加日志
@log
def unix_ora_add_trandata(src_args,trandata_args,src_other_args,_REMOTE_GGSCI_COMMAND):
    _REMOTE_GGSCI_COMMAND = _REMOTE_GGSCI_COMMAND % (src_other_args[0],src_other_args[1],src_other_args[2],src_other_args[2],src_other_args[2])
    os_type,db_type,ogg_ext,ext_object = trandata_args
    user_id,ogg_passwd = get_userid_passwd(src_args,src_other_args,ogg_ext)
    
    
    db_login_str = f'dblogin userid {user_id} password {ogg_passwd}\n'
    if ext_object != '':
        ext_object = ext_object
    else:
        ext_object = get_ext_obj(src_args,src_other_args,ogg_ext)
    

    add_count = 1
    while add_count<4:
        print(f"\nINFO:第{add_count}次添加附加日志")
        add_sup_cmd = db_login_str + 'add trandata ' + ext_object.replace(',','\nadd trandata ')

        ssh_input(src_args,_REMOTE_GGSCI_COMMAND %  add_sup_cmd)
        info_sup_cmd = db_login_str + 'info trandata '+ext_object.replace(',','\ninfo trandata ')
        info_sup_res = ssh_input(src_args,_REMOTE_GGSCI_COMMAND % info_sup_cmd)
        if 'ORA-' in ''.join(info_sup_res) or 'ERROR' in ''.join(info_sup_res):
            print("\nERROR:数据库或数据库连接可能存在问题，详情请查看ogg.log")
            return 'oracle f'
        disable_sup_res = [res for res in info_sup_res if 'disable' in res]
        if disable_sup_res!=[]:
            print('\nINFO:以下对象附加日志添加未成功,10s后重新执行添加')
            print(''.join(disable_sup_res))
            add_count +=1
            time.sleep(10)
        else:
            break
    if disable_sup_res ==[]:
        print("\nINFO:所有指定对象的附加日志已添加成功")
    else:
        print('\nINFO:以下对象附加日志添加未成功,请在空闲时间重新添加以下表的附加日志')
        print(''.join(disable_sup_res))

    return 'add trandata s'


# 类unix系统，oracle 删除附加日志
@log
def unix_ora_del_trandata(src_args,trandata_args,src_other_args,_REMOTE_GGSCI_COMMAND):
    _REMOTE_GGSCI_COMMAND = _REMOTE_GGSCI_COMMAND % (src_other_args[0],src_other_args[1],src_other_args[2],src_other_args[2],src_other_args[2])
    os_type,db_type,ogg_ext,ext_object = trandata_args
    user_id,ogg_passwd = get_userid_passwd(src_args,src_other_args,ogg_ext)

    db_login_str = f'dblogin userid {user_id} password {ogg_passwd}\n'

    if ext_object != '':
        ext_object = ext_object
    else:
        ext_object = get_ext_obj(src_args,src_other_args,ogg_ext)
    

    del_count = 1
    while del_count<4:
        print(f"\nINFO:第{del_count}次删除附加日志")
        add_sup_cmd = db_login_str +'delete trandata '+ ext_object.replace(',','\ndelete  trandata ')
        ssh_input(src_args,_REMOTE_GGSCI_COMMAND %  add_sup_cmd)
        info_sup_cmd = db_login_str + 'info trandata '+ext_object.replace(',','\ninfo trandata ')
        info_sup_res = ssh_input(src_args,_REMOTE_GGSCI_COMMAND % info_sup_cmd)
        if 'ORA-' in ''.join(info_sup_res) or 'ERROR' in ''.join(info_sup_res):
            print("\nERROR:数据库或数据库连接可能存在问题，详情请查看ogg.log")
            return 'oracle f'
        disable_sup_res = [res for res in info_sup_res if 'enable' in res]
        if disable_sup_res!=[]:
            print('\nINFO:以下对象附加日志添加未成功,10s后重新执行删除')
            print(''.join(disable_sup_res))
            del_count +=1
            time.sleep(10)
        else:
            break
    if disable_sup_res ==[]:
        print("\nINFO:所有指定对象的附加日志已删除成功")
    else:
        print('\nINFO:以下对象附加日志删除未成功,请在空闲时间重新删除以下表的附加日志')
        print(''.join(disable_sup_res))

    return 'del trandata s'

# 类unix系统，oracle 查看附加日志
@log
def unix_ora_info_trandata(src_args,trandata_args,src_other_args,_REMOTE_GGSCI_COMMAND):
    _REMOTE_GGSCI_COMMAND = _REMOTE_GGSCI_COMMAND % (src_other_args[0],src_other_args[1],src_other_args[2],src_other_args[2],src_other_args[2])
    os_type,db_type,ogg_ext,ext_object = trandata_args
    user_id,ogg_passwd = get_userid_passwd(src_args,src_other_args,ogg_ext)

    db_login_str = f'dblogin userid {user_id} password {ogg_passwd}\n'
    if ext_object != '':
        ext_object = ext_object
    else:
        ext_object = get_ext_obj(src_args,src_other_args,ogg_ext)
    

    info_sup_cmd = db_login_str + 'info trandata '+ext_object.replace(',','\ninfo trandata ')
    info_sup_res = ssh_input(src_args,_REMOTE_GGSCI_COMMAND % info_sup_cmd)
    if 'ORA-' in ''.join(info_sup_res) or 'ERROR' in ''.join(info_sup_res):
            print("\nERROR:数据库或数据库连接可能存在问题，详情请查看ogg.log")
            return 'oracle f'
    else:
        print('\nINFO:附加日志情况请查看ogg.log文件')


        return 'info trandata s'