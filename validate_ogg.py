 
 
 #!/usr/bin/env python
import os
from logging_method import log
from ssh_input import ssh_input
from method import res_table,get_ssh_list,create_dir,parse_prm_rep


 # create dblink 
@log
def create_dblink(src_os_args,tag_os_args,vali_args,_REMOTE_COMMAND,_REMOTE_TAG_COMMAND):
    src_ogg_user,src_ogg_passwd,src_db_port,src_dmp_dir = vali_args
    srv_name_tmp = ssh_input(src_os_args,_REMOTE_COMMAND % "show parameter service_name")[-1].split(' ')

    srv_name = [i for i in srv_name_tmp if i!=''][-1].strip('\n')
    print("\nINFO:源端数据库服务名为: %s\n"%srv_name)

    create_dblink_sql = '''
    create public  database link ops_dblink
connect to %s identified by %s
using '(DESCRIPTION =(ADDRESS_LIST =(ADDRESS =(PROTOCOL = TCP)(HOST = %s)(PORT = %s)))(CONNECT_DATA =(SERVICE_NAME = %s)))';
'''%(src_ogg_user,src_ogg_passwd,src_os_args[0],src_db_port,srv_name)


    check_dblink_sql = "select open_mode from v\$database@ops_dblink;"
 
    ssh_input(tag_os_args,_REMOTE_TAG_COMMAND % "drop public  database link ops_dblink;")
    ssh_input(tag_os_args,_REMOTE_TAG_COMMAND % create_dblink_sql)
    check_dblink = ssh_input(tag_os_args,_REMOTE_TAG_COMMAND % check_dblink_sql)

    
    if "READ WRITE\n" in check_dblink:
        print("\nINFO:DBLINK创建成功\n")
        return True
    else:
        print("\nWARRING:DBLINK创建失败，详情请查看日志：ogg.log\n")
        return False


# 获取两端数量不一致的对象，并处理
# 此步骤获取两端除了trigger、job、view、lob、table以外其他对象的不一致情况。
# 输出不一致信息到 ops_check_detail.txt  //辅助判断
# 输出不一致表的修复语句到 ops_check.sql  //人工检查之后执行修复
@log
def get_inconsist_obj(os_args,sync_users,_REMOTE_TAG_COMMAND):
    sync_users_str = str(tuple(sync_users)).upper().replace(',)',')')
    create_count_tb_sql = '''drop table ops_count;
create table ops_count(OWNER,OBJECT_TYPE,total) as
SELECT D.OWNER, D.OBJECT_TYPE, COUNT(*)
FROM dba_objects@ops_dblink d
WHERE d.OWNER in %s
and d.object_type not in ('TRIGGER', 'JOB', 'VIEW', 'LOB','TABLE')
and object_name not like 'MLOG%%'
AND NOT EXISTS (SELECT *
    FROM DBA_RECYCLEBIN@ops_dblink B
    WHERE B.object_name = D.OBJECT_NAME
      AND D.OWNER = B.owner)
GROUP BY D.OWNER, D.OBJECT_TYPE
minus
SELECT D.OWNER, D.OBJECT_TYPE, COUNT(*)
FROM dba_objects d
WHERE d.OWNER in %s
and d.object_type not in ('TRIGGER', 'JOB', 'VIEW', 'LOB','TABLE')
and object_name not like 'MLOG%%'
AND NOT EXISTS (SELECT *
    FROM DBA_RECYCLEBIN B
    WHERE B.object_name = D.OBJECT_NAME
      AND D.OWNER = B.owner)
GROUP BY D.OWNER, D.OBJECT_TYPE;'''%(sync_users_str,sync_users_str)

    select_count_sql='select * from ops_count;'
    ssh_input(os_args,_REMOTE_TAG_COMMAND % create_count_tb_sql)
    select_count_res = ssh_input(os_args,_REMOTE_TAG_COMMAND % select_count_sql)
    if select_count_res !=[] :
        print("\nINFO:两端除了trigger、job、view、lob、table以外其他对象数量不一致的信息如下:\n")
      
        select_count_list = get_ssh_list(select_count_res)

      
        title =['用户名','对象类型','不一致数目']
        table_res = res_table(select_count_list,title)
        print(table_res)
    else:
        print("\nINFO:两端除了trigger、job、view、lob、table以外其他对象信息数量一致")
    return select_count_res


# 复用的获取ddl语句的方法
@log
def get_ddl_info(os_args,obj_name,obj_owner,obj_type,_REMOTE_COMMAND):
    if obj_type == 'PACKAGE BODY':
        obj_type = 'PACKAGE_BODY'
    get_ddl_sql = '''set long 9999
    select sys.dbms_metadata.get_ddl('%s','%s','%s') From DUAL;'''%(obj_type,obj_name,obj_owner)
    get_ddl_res = ssh_input(os_args,_REMOTE_COMMAND % get_ddl_sql)
    ddl_res = (f"#### {obj_type}:\n\n{''.join(get_ddl_res)}")

    return ddl_res



# 复用的获取对象信息的方法
@log
def get_diff_obj_info(src_args,tag_args,get_obj_info_sql,obj_type,_REMOTE_COMMAND,_REMOTE_TAG_COMMAND):
    if obj_type == 'LOB':
        title =[f'{obj_type} 名',f'{obj_type} 用户名']
    elif  obj_type == 'INVAILD OBJECT':
        title =[f'{obj_type} 名',f'{obj_type} 用户名','对象类型','状态']
    else:
        title =[f'{obj_type} 名',f'{obj_type} 用户名','状态']
    if get_obj_info_sql =='':
        get_obj_info_sql = '''select distinct s.name, s.owner, b.status
    from dba_source@ops_dblink s, dba_objects@ops_dblink b
    where s.name = b.object_name
    and b.object_type = '%s'
    and s.owner in (select owner from ops_count where OBJECT_TYPE='%s')
    minus
    select distinct s.name, s.owner, b.status
    from dba_source s, dba_objects b
    where s.name = b.object_name
    and b.object_type = '%s'
    and s.owner in (select owner from ops_count where OBJECT_TYPE='%s');
        '''%(obj_type,obj_type,obj_type,obj_type)
    else:
        pass
    get_obj_res = ssh_input(tag_args,_REMOTE_TAG_COMMAND % get_obj_info_sql)
    obj_list = get_ssh_list(get_obj_res)

    if obj_list != []:
        print(f"\nINFO: 目标库缺失的{obj_type} 具体信息请查看本地 ops_check_detail.txt文件：")
        table_res = res_table(obj_list,title)
        with open('ops_check_detail.txt','a+',encoding='utf-8') as file:
            file.write(f"\nINFO: 目标库缺失的{obj_type} 具体信息:\n{table_res}\n")

        ## 获取缺失对象的ddl语句
        if obj_type not in ['TABLE','LOB','DATABASE LINK','INVAILD OBJECT']:
            for obj in obj_list:
                obj_name,obj_owner,_ = obj
                ddl_res = get_ddl_info(src_args,obj_name,obj_owner,obj_type,_REMOTE_COMMAND)
                ssh_input(tag_args,f'''echo "{ddl_res}" >> /tmp/ops_check.sql''')
            print(f"\nINFO: 目标库缺失{obj_type}的ddl语句请在目标端查看 /tmp/ops_check.sql 文件 ")

            
    else:
        print(f"\nINFO: 源与目标端{obj_type}类型对象信息一致")
    return obj_list


# 分支一 objects_count包含index
@log
def index_vali(src_args,tag_args,_REMOTE_COMMAND,_REMOTE_TAG_COMMAND):
    get_diff_index_count_sql = '''
select TABLE_OWNER, TABLE_NAME, COUNT(*)
  from DBA_INDEXES@ops_dblink
 where owner in (select owner from ops_count where OBJECT_TYPE='INDEX')
 group by table_owner, table_name
minus
select TABLE_OWNER, TABLE_NAME, COUNT(*)
  from DBA_INDEXES
 where owner in (select owner from ops_count where OBJECT_TYPE='INDEX')
 group by table_owner, table_name;'''
 
    get_diff_index_info_sql = '''
set lin 10000
select index_owner,
       index_name,
       table_owner,
       table_name,
       listagg(to_char(column_name), ',') within group(order by index_name) as full_column
  from DBA_IND_COLUMNS@ops_dblink
 where table_owner = '%s'
   and table_name = '%s'
 group by index_owner, index_name, table_owner, table_name
minus
select index_owner,
       index_name,
       table_owner,
       table_name,
       listagg(to_char(column_name), ',') within group(order by index_name) as full_column
  from DBA_IND_COLUMNS
 where table_owner = '%s'
   and table_name = '%s'
 group by index_owner, index_name, table_owner, table_name;'''

    get_ind_count_res = ssh_input(tag_args,_REMOTE_TAG_COMMAND % get_diff_index_count_sql)
    ind_list = get_ssh_list(get_ind_count_res)

    ind_res_list =[]
    ind_ddl_list = []
    for obj in ind_list:
        get_diff_index_info_sql_tmp = get_diff_index_info_sql % (obj[0],obj[1],obj[0],obj[1])
        get_ind_res = ssh_input(tag_args,_REMOTE_TAG_COMMAND % get_diff_index_info_sql_tmp)

        ind_info_list = get_ssh_list(get_ind_res)[0]
 
        ind_res_list.append(ind_info_list)

        ## 获取缺失对象的ddl语句
        ind_owner,ind_name,_,_,_ = ind_info_list
        ddl_res = get_ddl_info(src_args,ind_name,ind_owner,'INDEX',_REMOTE_COMMAND)

        ind_ddl_list.append(ddl_res)
    print(f"\nINFO: 目标库缺失的索引具体信息请查看本地 ops_check_detail.txt文件：")
    title = ['索引用户名','索引名','表用户名','表名','列名']

    table_res = res_table(ind_res_list,title)
    with open('ops_check_detail.txt','a+',encoding='utf-8') as file:
        file.write(f"\nINFO: 目标库缺失的INDEX 具体信息:\n{table_res}\n")
    ssh_input(tag_args,f'''echo "{''.join(ind_ddl_list)}" >> /tmp/ops_check.sql''')
    print(f"\nINFO: 目标库缺失 INDEX 的ddl语句请在目标端查看 /tmp/ops_check.sql 文件 ")

    return ind_res_list

        



 # 分支2 objects_count包含function
@log
def function_vali(src_args,tag_args,_REMOTE_COMMAND,_REMOTE_TAG_COMMAND):
    func_list = get_diff_obj_info(src_args,tag_args,'','FUNCTION',_REMOTE_COMMAND,_REMOTE_TAG_COMMAND)
    return func_list

 # 分支3 objects_count包含PROCEDURE
@log
def proc_vali(src_args,tag_args,_REMOTE_COMMAND,_REMOTE_TAG_COMMAND):
    proc_list = get_diff_obj_info(src_args,tag_args,'','PROCEDURE',_REMOTE_COMMAND,_REMOTE_TAG_COMMAND)
    return proc_list

 # 分支4 objects_count包含PACKAGE
@log
def pkg_vali(src_args,tag_args,_REMOTE_COMMAND,_REMOTE_TAG_COMMAND):
    pkg_list = get_diff_obj_info(src_args,tag_args,'','PACKAGE',_REMOTE_COMMAND,_REMOTE_TAG_COMMAND)
    return pkg_list


 # 分支5 objects_count包含PACKAGE BODY
@log
def pkg_body_vali(src_args,tag_args,_REMOTE_COMMAND,_REMOTE_TAG_COMMAND):
    pkg_body_list = get_diff_obj_info(src_args,tag_args,'','PACKAGE BODY',_REMOTE_COMMAND,_REMOTE_TAG_COMMAND)
    return pkg_body_list

 # 分支6 objects_count包含SYNONYM
@log
def synonym_vali(src_args,tag_args,_REMOTE_COMMAND,_REMOTE_TAG_COMMAND):
    get_obj_sql = '''
    select  s.synonym_name,s.owner,b.status
  from dba_synonyms@ops_dblink s, dba_objects@ops_dblink b
 where s.synonym_name = b.object_name
   and b.object_type = 'SYNONYM'
   and s.owner in (select owner from ops_count where OBJECT_TYPE='SYNONYM')
minus
select  s.synonym_name,s.owner,  b.status
  from dba_synonyms s, dba_objects b
 where s.synonym_name = b.object_name
   and b.object_type = 'SYNONYM'
   and s.owner in (select owner from ops_count where OBJECT_TYPE='SYNONYM');'''
    synonym_list = get_diff_obj_info(src_args,tag_args,get_obj_sql,'SYNONYM',_REMOTE_COMMAND,_REMOTE_TAG_COMMAND)
    return synonym_list


 # 同步用户以外的table检查
@log
def table_vali(src_args,tag_args,sync_users,_REMOTE_COMMAND,_REMOTE_TAG_COMMAND):
    sync_users_str = str(tuple(sync_users)).upper().replace(',)',')')
    get_table_sql = '''
    select  table_name,owner,STATUS
    from dba_tables@ops_dblink
    where owner not in %s
    minus
    select table_name,owner,STATUS
    from dba_tables where owner not in %s;'''%(sync_users_str,sync_users_str)
    tab_list = get_diff_obj_info(src_args,tag_args,get_table_sql,'TABLE',_REMOTE_COMMAND,_REMOTE_TAG_COMMAND)
    return tab_list

 # lob检查
@log
def lob_vali(src_args,tag_args,sync_users,_REMOTE_COMMAND,_REMOTE_TAG_COMMAND):
    sync_users_str = str(tuple(sync_users)).upper().replace(',)',')')
    get_lob_sql = '''
    select  table_name,owner
    from dba_lobs@ops_dblink
    where owner in %s
    minus
    select table_name,owner
    from dba_lobs
    where owner in %s;'''%(sync_users_str,sync_users_str)
    lob_list = get_diff_obj_info(src_args,tag_args,get_lob_sql,'LOB',_REMOTE_COMMAND,_REMOTE_TAG_COMMAND)
    return lob_list


# VIEW检查
@log
def view_vali(src_args,tag_args,sync_users,_REMOTE_COMMAND,_REMOTE_TAG_COMMAND):
    sync_users_str = str(tuple(sync_users)).upper().replace(',)',')')
    get_view_sql = '''
    select  v.view_name, v.owner,b.status
    from dba_views@ops_dblink v, dba_objects@ops_dblink b
    where v.owner in %s
    and v.view_name = b.object_name
    minus
    select v.view_name,v.owner, b.status
    from dba_views v, dba_objects b
    where v.owner in %s
    and v.view_name = b.object_name;'''%(sync_users_str,sync_users_str)
    view_list = get_diff_obj_info(src_args,tag_args,get_view_sql,'VIEW',_REMOTE_COMMAND,_REMOTE_TAG_COMMAND)
    return view_list

# dblink检查
@log
def dblink_vali(src_args,tag_args,_REMOTE_COMMAND,_REMOTE_TAG_COMMAND):
    get_dblink_sql = '''
    select object_name, owner,status
  from dba_objects@ops_dblink
 where object_type = 'DATABASE LINK'
minus
select  object_name, owner,status
  from dba_objects
 where object_type = 'DATABASE LINK';'''
    dblink_list = get_diff_obj_info(src_args,tag_args,get_dblink_sql,'DATABASE LINK',_REMOTE_COMMAND,_REMOTE_TAG_COMMAND)
    if dblink_list != []:
        with open('ops_check_detail.txt','a+',encoding='utf-8') as file:
            file.write('''\nINFO: DBLINK逻辑泵导出导入语句为:\nexpdp "'"/ as sysdba"'" include=db_link directory=ops_EXPDP full=y network_link=ops_dblink dumpfile=ops_dblink.dmp logfile=ops_dblink_expdp.log
impdp "'"/ as sysdba"'" directory=ops_EXPDP dumpfile=ops_dblink.dmp logfile=ops_dblink_impdp.log\n''')

    return dblink_list


# 权限对比
@log
def priv_vali(src_args,tag_args,sync_users,_REMOTE_COMMAND,_REMOTE_TAG_COMMAND):
    sync_users_str = str(tuple(sync_users)).upper().replace(',)',')')
    create_src_sql = '''
drop table t_tmp_user_lhr;
create table t_tmp_user_lhr( id number, username varchar2(50), exec_sql varchar2(4000),create_type varchar2(20));
DROP sequence s_t_tmp_user_lhr;
create sequence s_t_tmp_user_lhr;

begin for cur IN 
    (SELECT d.username,
         d.default_tablespace,
         d.account_status,
         'create user ' || d.username || ' identified by ' || d.username || ' default tablespace ' || d.default_tablespace || ' TEMPORARY TABLESPACE ' || D.temporary_tablespace || ';' CREATE_USER, replace(to_char(DBMS_METADATA.GET_DDL('USER', D.username)), chr(10), '') create_USER1
    FROM dba_users@ops_dblink d
    WHERE d.username IN %s) loop 
    INSERT INTO t_tmp_user_lhr (id, username, exec_sql, create_type) values (s_t_tmp_user_lhr.nextval, cur.username, cur.CREATE_USER, 'USER');
    
    INSERT INTO t_tmp_user_lhr (id, username, exec_sql, create_type) SELECT s_t_tmp_user_lhr.nextval,cur.username,CASE WHEN D.ADMIN_OPTION = 'YES' THEN 
    'GRANT ' || d.privilege || ' TO ' || d.GRANTEE || ' WITH GRANT OPTION ;'
    ELSE 'GRANT ' || d.privilege || ' TO ' || d.GRANTEE || ';'
    END priv, 'DBA_SYS_PRIVS'
    FROM dba_sys_privs@ops_dblink d
    WHERE D.GRANTEE = CUR.USERNAME;

    INSERT INTO t_tmp_user_lhr (id, username, exec_sql, create_type) SELECT s_t_tmp_user_lhr.nextval,cur.username,CASE WHEN D.ADMIN_OPTION = 'YES' THEN
    'GRANT ' || d.GRANTED_ROLE || ' TO ' || d.GRANTEE || ' WITH GRANT OPTION;'
    ELSE 'GRANT ' || d.GRANTED_ROLE || ' TO ' || d.GRANTEE || ';'
    END priv, 'DBA_ROLE_PRIVS'
    FROM DBA_ROLE_PRIVS@ops_dblink d
    WHERE D.GRANTEE = CUR.USERNAME;
    
    INSERT INTO t_tmp_user_lhr (id, username, exec_sql, create_type) SELECT s_t_tmp_user_lhr.nextval,cur.username,CASE WHEN d.grantable = 'YES' THEN
    'GRANT ' || d.privilege || ' ON ' || d.owner || '.' || d.table_name || ' TO ' || d.GRANTEE || ' WITH GRANT OPTION ;'
    ELSE 'GRANT ' || d.privilege || ' ON ' || d.owner || '.' || d.table_name || ' TO ' || d.GRANTEE || ';'
    END priv, 'DBA_TAB_PRIVS'
    FROM DBA_TAB_PRIVS@ops_dblink d
    WHERE D.GRANTEE = CUR.USERNAME;
    
END loop;
COMMIT;
end;
/   
    '''%sync_users_str
    create_tag_sql = '''
    drop table t_tmp_user_lhr_new;
create table t_tmp_user_lhr_new( id number, username varchar2(50), exec_sql varchar2(4000),create_type varchar2(20));
DROP sequence s_t_tmp_user_lhr_new;
create sequence s_t_tmp_user_lhr_new; 

begin for cur IN 
    (SELECT d.username,
         d.default_tablespace,
         d.account_status,
         'create user ' || d.username || ' identified by ' || d.username || ' default tablespace ' || d.default_tablespace || ' TEMPORARY TABLESPACE ' || D.temporary_tablespace || ';' CREATE_USER, replace(to_char(DBMS_METADATA.GET_DDL('USER', D.username)), chr(10), '') create_USER1
    FROM dba_users d
    WHERE d.username IN %s) loop 
    INSERT INTO t_tmp_user_lhr_new (id, username, exec_sql, create_type) values (s_t_tmp_user_lhr_new.nextval, cur.username, cur.CREATE_USER, 'USER');
    
    INSERT INTO t_tmp_user_lhr_new (id, username, exec_sql, create_type) SELECT s_t_tmp_user_lhr_new.nextval,cur.username,CASE WHEN D.ADMIN_OPTION = 'YES' THEN 
    'GRANT ' || d.privilege || ' TO ' || d.GRANTEE || ' WITH GRANT OPTION ;'
    ELSE 'GRANT ' || d.privilege || ' TO ' || d.GRANTEE || ';'
    END priv, 'DBA_SYS_PRIVS'
    FROM dba_sys_privs d
    WHERE D.GRANTEE = CUR.USERNAME;
    
    INSERT INTO t_tmp_user_lhr_new (id, username, exec_sql, create_type) SELECT s_t_tmp_user_lhr_new.nextval,cur.username,CASE WHEN D.ADMIN_OPTION = 'YES' THEN 
    'GRANT ' || d.GRANTED_ROLE || ' TO ' || d.GRANTEE || ' WITH GRANT OPTION;'
    ELSE 'GRANT ' || d.GRANTED_ROLE || ' TO ' || d.GRANTEE || ';'
    END priv, 'DBA_ROLE_PRIVS'
    FROM DBA_ROLE_PRIVS d
    WHERE D.GRANTEE = CUR.USERNAME;
    
    INSERT INTO t_tmp_user_lhr_new (id, username, exec_sql, create_type) SELECT s_t_tmp_user_lhr_new.nextval,cur.username, CASE WHEN d.grantable = 'YES' THEN
    'GRANT ' || d.privilege || ' ON ' || d.owner || '.' || d.table_name || ' TO ' || d.GRANTEE || ' WITH GRANT OPTION ;'
    ELSE 'GRANT ' || d.privilege || ' ON ' || d.owner || '.' || d.table_name || ' TO ' || d.GRANTEE || ';'
    END priv, 'DBA_TAB_PRIVS'
    FROM DBA_TAB_PRIVS d
    WHERE D.GRANTEE = CUR.USERNAME;
END loop;
COMMIT;
end;
/
'''%sync_users_str
    get_role_sql = '''
    SELECT EXEC_SQL FROM t_tmp_user_lhr where CREATE_TYPE not in %s
    minus 
    SELECT EXEC_SQL FROM t_tmp_user_lhr_new where CREATE_TYPE not in %s;'''%(sync_users_str,sync_users_str)
    drop_src_sql = '''
    drop table t_tmp_user_lhr;
DROP sequence s_t_tmp_user_lhr;
    '''
    drop_tag_sql = '''
    drop table t_tmp_user_lhr_new;
DROP sequence s_t_tmp_user_lhr_new;
    '''

    ssh_input(src_args,_REMOTE_COMMAND % create_src_sql)
    ssh_input(tag_args,_REMOTE_TAG_COMMAND % create_tag_sql)
    role_res = ssh_input(tag_args,_REMOTE_TAG_COMMAND % get_role_sql)
    if role_res == []:
        print("\nINFO: 两端role权限一致")
    else:
        print(f"\nINFO: 目标库缺失ROLE的dcl语句请在目标端查看 /tmp/ops_check.sql 文件 ")
        ssh_input(tag_args,f'''echo "{''.join(role_res)}" >> /tmp/ops_check.sql''')

    ssh_input(src_args,_REMOTE_COMMAND % drop_src_sql)
    ssh_input(tag_args,_REMOTE_TAG_COMMAND % drop_tag_sql)
    return role_res
    
# 无效对象比对
@log
def invaild_obj_vali(src_args,tag_args,_REMOTE_COMMAND,_REMOTE_TAG_COMMAND):
    create_inv_obj_sql = "drop table t_tmp_invalid_object;\ncreate table t_tmp_invalid_object as select * from dba_objects@ops_dblink where status<>'VALID';"
    utlrp_sql = "@?/rdbms/admin/utlrp.sql"
    get_inv_obj_sql = '''
    select  object_name, owner,object_type, status
  from dba_objects
 where status <> 'VALID'
   and owner not in ('HZopsASSET')
   and object_name not in (select object_name from t_tmp_invalid_object);'''
    ssh_input(tag_args,_REMOTE_TAG_COMMAND % create_inv_obj_sql)
    print("\nINFO: 开始编译无效对象\n")
    ssh_input(tag_args,_REMOTE_TAG_COMMAND % utlrp_sql )
    inv_obj_list = get_diff_obj_info(src_args,tag_args,get_inv_obj_sql,'INVAILD OBJECT',_REMOTE_COMMAND,_REMOTE_TAG_COMMAND)

    return inv_obj_list


# 物化视图比对
@log
def mview_vali(src_args,tag_args,_REMOTE_COMMAND,_REMOTE_TAG_COMMAND):
    get_src_mview_sql = "select MVIEW_NAME,owner from DBA_MVIEWS@ops_dblink;"
    get_tag_mview_sql = "select MVIEW_NAME,owner from DBA_MVIEWS;"

    src_mview_res = ssh_input(tag_args,_REMOTE_TAG_COMMAND % get_src_mview_sql)
    tag_mview_res = ssh_input(tag_args,_REMOTE_TAG_COMMAND % get_tag_mview_sql)

    src_obj_list = get_ssh_list(src_mview_res)
    tag_obj_list = get_ssh_list(tag_mview_res)
    title =['物化视图名','物化视图用户名']
    if src_obj_list != [] :
        print(f"\nINFO: 源端物化视图具体信息请查看本地 ops_check_detail.txt文件：")
        src_table_res = res_table(src_obj_list,title)
        with open('ops_check_detail.txt','a+',encoding='utf-8') as file:
            file.write(f"\nINFO: 源端物化视图具体信息:\n{src_table_res}\n")

        ## 获取缺失对象的ddl语句
        for obj in src_obj_list:
            obj_name,obj_owner= obj
            ddl_res = get_ddl_info(src_args,obj_name,obj_owner,'MATERIALIZED_VIEW',_REMOTE_COMMAND)
            ssh_input(tag_args,f'''echo '{ddl_res}' >> /tmp/ops_check.sql''')
            print(f"\nINFO: 源端物化视图的ddl语句请在目标端查看 /tmp/ops_check.sql 文件 ")
    else:
        print(f"\nINFO: 源端不存在物化视图")
    if tag_obj_list != [] :
        print(f"\nINFO: 目标端物化视图具体信息请查看本地 ops_check_detail.txt文件：")
        tag_table_res = res_table(tag_obj_list,title)
        with open('ops_check_detail.txt','a+',encoding='utf-8') as file:
            file.write(f"\nINFO: 目标端物化视图具体信息:\n{src_table_res}\n")
    else:
        print(f"\nINFO: 源端不存在物化视图")
    return src_obj_list
    
# veridata比对慢的表
@log
def slow_vali(src_args,tag_args,_REMOTE_COMMAND,_REMOTE_TAG_COMMAND):

    owner = input("请输入veridata比对慢的表的用户名: ").upper()
    slow_tables_tmp = input("请输入veridata比对慢的表表名,多表请用逗号隔开: ").upper()
    slow_tables = str(tuple(slow_tables_tmp.split(','))).replace(',)',')')
    get_slow_sql = '''
set serveroutput on;
declare
  v_tablename varchar2(60);
  v_count     int;
  v_sql       varchar2(2000);
  cursor cur_tablename is
    select table_name
      from dba_tables
     where table_name in
           %s
       and owner = '%s';
begin
  open cur_tablename;
  loop
    fetch cur_tablename
      into v_tablename;
    exit when cur_tablename%%notfound;
    v_sql := 'select count(*) from %s.' || v_tablename || '';
    execute immediate v_sql
      into v_count;
    dbms_output.put_line('%s.'||v_tablename || '.' || v_count);
  end loop;
  close cur_tablename;
end;
/
   '''%(slow_tables,owner,owner,owner)
    
    src_slow_res = ssh_input(src_args,_REMOTE_COMMAND % get_slow_sql) 
    tag_slow_res = ssh_input(tag_args,_REMOTE_TAG_COMMAND % get_slow_sql) 
    diff_list = []

    for i,v in enumerate(src_slow_res):
        src_cnt = v.split('.')[-1]
        tag_cnt = tag_slow_res[i].split('.')[-1]
        if src_cnt != tag_cnt:
            diff_tmp = v.split('.').append(tag_cnt)
            diff_list.append()
        else:
            pass
    print(diff_list)
    if diff_list == []:
        print(f"\nINFO: 两端{owner}的表 {slow_tables_tmp} 表行数一致.\n")
    else:
        
        title = ['用户名','表名','源端行数','目标端行数']
        slow_table_res = res_table(diff_list,title)
        print_res = f"\nINFO: 两端{owner}的表 {slow_tables_tmp} 表行数不一致:\n{slow_table_res}"
        print(print_res)
        print(f"\nINFO: 相关信息已保存到本地 ops_check_detail_slow.txt文件：")
        with open('ops_check_detail_slow.txt','a+',encoding='utf-8') as file:
            file.write(print_res)
    return diff_list


# 切换检查主程序
@log
def vali_check(src_args,tag_args,tag_other_args,vali_args,_REMOTE_COMMAND,_REMOTE_TAG_COMMAND):
        sync_users = parse_prm_rep(tag_args,tag_other_args)
        create_dir(tag_args,vali_args,_REMOTE_TAG_COMMAND)
        create_dbl_res = create_dblink(src_args,tag_args,vali_args,_REMOTE_COMMAND,_REMOTE_TAG_COMMAND)
        if create_dbl_res is not False:
            get_inconsist_obj(tag_args,sync_users,_REMOTE_TAG_COMMAND)
            ssh_input(tag_args,'rm -rf /tmp/ops_check.sql')
            os.system("echo '' >ops_check_detail.txt")
            index_vali(src_args,tag_args,_REMOTE_COMMAND,_REMOTE_TAG_COMMAND)
            function_vali(src_args,tag_args,_REMOTE_COMMAND,_REMOTE_TAG_COMMAND)
            proc_vali(src_args,tag_args,_REMOTE_COMMAND,_REMOTE_TAG_COMMAND)
            pkg_vali(src_args,tag_args,_REMOTE_COMMAND,_REMOTE_TAG_COMMAND)
            pkg_body_vali(src_args,tag_args,_REMOTE_COMMAND,_REMOTE_TAG_COMMAND)
            synonym_vali(src_args,tag_args,_REMOTE_COMMAND,_REMOTE_TAG_COMMAND)
            table_vali(src_args,tag_args,sync_users,_REMOTE_COMMAND,_REMOTE_TAG_COMMAND)
            lob_vali(src_args,tag_args,sync_users,_REMOTE_COMMAND,_REMOTE_TAG_COMMAND)
            view_vali(src_args,tag_args,sync_users,_REMOTE_COMMAND,_REMOTE_TAG_COMMAND)
            dblink_vali(src_args,tag_args,_REMOTE_COMMAND,_REMOTE_TAG_COMMAND)
            priv_vali(src_args,tag_args,sync_users,_REMOTE_COMMAND,_REMOTE_TAG_COMMAND)
            invaild_obj_vali(src_args,tag_args,_REMOTE_COMMAND,_REMOTE_TAG_COMMAND)
            mview_vali(src_args,tag_args,_REMOTE_COMMAND,_REMOTE_TAG_COMMAND)

        return "check complete"

    