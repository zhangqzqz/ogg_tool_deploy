 
 #!/usr/bin/env python

from logging_method import log
from ssh_input import ssh_input
from prettytable import PrettyTable,ALL
import re


# put res to a prettytable
@log
def res_table(res,title):
    '''
    put res to a prettytable
    '''
    t= PrettyTable(title)
    t.hrules = ALL
    for i in res:
        i = list(i)
        for index,m in enumerate(i):
            if isinstance(m,str):
                if len(m) > 100:
                    n = re.findall(r'.{50}',m)
                    m = '\n'.join(n)
                    i[index] = m
        t.add_row(i)

    return t


# 去除ssh执行返回值中的空格，返回列表
@log
def get_ssh_list(ssh_res):
    select_list_tmp = [obj.replace('\n','').split(' ') for obj in ssh_res ]
    select_list = []
    for tmp_l in select_list_tmp:
        tmp_l = [obj_t for obj_t in tmp_l if obj_t!='']
       
        # if len(tmp_l) == 4:
        #     tmp_l[1] =' '.join(tmp_l[1:3])
        
        #     tmp_l.pop(2)
        if 'MATERIALIZED' in tmp_l:
            ind = tmp_l.index('MATERIALIZED')
            tmp_l[ind] = ' '.join(tmp_l[ind:(ind+2)])
            tmp_l.pop(ind+1)
        if 'BODY' in tmp_l:
            ind = tmp_l.index('BODY')
            tmp_l[ind-1] = ' '.join(tmp_l[(ind-1):(ind+1)])
            tmp_l.pop(ind)


        select_list.append(tmp_l)

    return select_list

# parse the parameter file of ogg replidate
@log
def parse_prm_rep(tag_args,tag_other_args):
    rep_prm_str = ssh_input(tag_args,f"cat {tag_other_args[2]}/dirprm/*_rep.prm")
    sync_users_str = [i for i in rep_prm_str if 'MAP' in i.upper() and 'TARGET' in i.upper()]
    sync_users = list(set([k.split('.')[0].split(' ')[-1] for k in sync_users_str ]))
    if sync_users != []:
        print(f"\nINFO: OGG同步用户为：{','.join(sync_users)}")
        return sync_users
    else:
        print("\nWARRING:获取同步用户列表失败！")
        return "get users failed"
    

# create the directory for expdp or impdp
@log
def create_dir(os_args,vali_args,_REMOTE_COMMAND):
    db_user,db_passwd,db_port,dmp_dir = vali_args
    print("\nINFO:创建数据库目录OPS_EXPDP")
    
    check_dir_res = ''.join(ssh_input(os_args,f"ls {dmp_dir}"))
    if 'No such file or directory' in check_dir_res:
        print("\nWARRING:该路径不存在，请检查系统环境！")
        return 0
    create_dir_sql = '''create or replace directory ops_expdp as '%s';
 grant read,write on directory ops_expdp to %s;
'''%(dmp_dir,db_user)

    create_dir_res = ssh_input(os_args,_REMOTE_COMMAND %create_dir_sql)
    check_sql = "select * from dba_directories where DIRECTORY_NAME = 'OPS_EXPDP';"
    if 'fail' not in create_dir_res:
        check_res = ssh_input(os_args,_REMOTE_COMMAND %check_sql)

        print(f"\nINFO:数据库目录OPS_EXPDP：{dmp_dir} 创建成功")
        
        return 'create dir s'
    else:
        print(f"\nWARRING:数据库目录OPS_EXPDP：{dmp_dir} 创建失败")
        return 'create dir f'


# 去除列表中的空值

def del_null_list(list_input):
    list_res = [v for v in list_input if v != '' ]
    return list_res


