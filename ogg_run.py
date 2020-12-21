#!/usr/bin/env python
import argparse
import functools
import json
import logging
import sys
import paramiko
from config_parse import get_config,get_other_args,get_init_args,get_vali_args,get_reinit_args,get_trandata_args
from database_check import db_check
from ssh_input import ssh_input,ssh_scp
from src_ogg import src_ogg
from tag_ogg import tag_ogg
from full_ogg import full_ogg
from ogg_service_config import check_ogg,start_prc_srp
from validate_ogg import slow_vali,vali_check
from reinitial import reinitial_tb
from trandata import unix_ora_add_trandata,unix_ora_del_trandata,unix_ora_info_trandata




# global

__version__ = "1.1.0"


# logging

logging.basicConfig(format="%(levelname)s\t%(asctime)s\t%(message)s",filename="ogg.log")
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)




src_other_args= get_other_args('source_config')
tag_other_args = get_other_args('target_config')

_REMOTE_COMMAND = """
su - %s <<EOF
export ORACLE_SID=%s
cd %s
sqlplus -s / as sysdba
set linesize 800;
set heading off
set feedback off
set tab off
%%s
exit
EOF
"""%(src_other_args[0],src_other_args[1],src_other_args[2] )


_REMOTE_TAG_COMMAND = """
su - %s <<EOF
export ORACLE_SID=%s
cd %s
sqlplus -s / as sysdba
set linesize 800;
set heading off
set feedback off
set tab off
%%s
exit
EOF
"""%(tag_other_args[0],tag_other_args[1],tag_other_args[2] )

_REMOTE_GGSCI_COMMAND = '''
su - %s <<EOF
export ORACLE_SID=%s
export LD_LIBRARY_PATH=%s:\$ORACLE_HOME/lib
export LIBPATH=%s:\$ORACLE_HOME/lib
%s/ggsci
%%s
exit
EOF
'''





# main

def main():
    
    parser = argparse.ArgumentParser(description="ogg")

    parser.add_argument("item", help="dbcheck:ogg搭建前检查\
                || src:ogg源端配置\
                ||tag:ogg目标端配置 \
                ||full:ogg逻辑泵全量传输\
                ||check_ogg:ogg进程状态查看\
                ||config_srv:ogg开机自启动设置\
                ||slow_table:veridata慢表信息查询\
                ||vali_check:切换前基础检查\
                ||reinitial_table:ogg表重新初始化\
                ||add_trandata:源库添加附加日志\
                ||del_trandata:源库删除附加日志\
                ||info_trandata:源库查看附加日志",

            choices=['dbcheck','src','tag','full','check_ogg','config_srv','slow_table','vali_check','reinitial_table','add_trandata','del_trandata','info_trandata'])
   
    args = parser.parse_args()

    print("INFO:读取解析脚本参数文件")
    src_args = get_config("source_config")
    tag_args = get_config("target_config")
    


    if args.item=='dbcheck':
        src_init_args = get_init_args('source_config')
        tag_init_args = get_init_args('target_config')
        src_cmd_args = src_other_args+src_init_args
        tag_cmd_args = tag_other_args+tag_init_args
        db_check(src_args,src_cmd_args,_REMOTE_COMMAND)
    elif args.item=='src':
        src_init_args = get_init_args('source_config')
        src_cmd_args = src_other_args+src_init_args

        src_ogg(src_args,tag_args,src_cmd_args,_REMOTE_COMMAND)
    elif args.item=='tag':

        tag_init_args = get_init_args('target_config')

        tag_cmd_args = tag_other_args+tag_init_args
        tag_ogg(tag_args,src_args,tag_cmd_args,_REMOTE_TAG_COMMAND)
    elif args.item=='full':
        src_init_args = get_init_args('source_config')
        tag_init_args = get_init_args('target_config')
        src_cmd_args = src_other_args+src_init_args
        tag_cmd_args = tag_other_args+tag_init_args
        full_ogg(src_args,tag_args,src_cmd_args,tag_cmd_args,_REMOTE_COMMAND,_REMOTE_TAG_COMMAND,_REMOTE_GGSCI_COMMAND)
    elif args.item =='check_ogg':
        src_res = check_ogg(src_args,src_other_args,_REMOTE_GGSCI_COMMAND)
        tag_res = check_ogg(tag_args,tag_other_args,_REMOTE_GGSCI_COMMAND)
    elif args.item == 'config_srv':
        src_res = check_ogg(src_args,src_other_args,_REMOTE_GGSCI_COMMAND)
        start_prc_srp(src_args,src_other_args,src_res)
        tag_res = check_ogg(tag_args,tag_other_args,_REMOTE_GGSCI_COMMAND)
        start_prc_srp(tag_args,tag_other_args,tag_res)
    elif args.item == 'vali_check':
        vali_args = get_vali_args()
        vali_check(src_args,tag_args,tag_other_args,vali_args,_REMOTE_COMMAND,_REMOTE_TAG_COMMAND)
    elif args.item == 'slow_table':
        slow_vali(src_args,tag_args,_REMOTE_COMMAND,_REMOTE_TAG_COMMAND)
    elif args.item == 'reinitial_table':
        reinit_args = get_reinit_args()
        reinitial_tb(src_args,tag_args,tag_other_args,reinit_args,_REMOTE_COMMAND, _REMOTE_TAG_COMMAND)
    elif args.item == 'add_trandata':
        trandata_args = get_trandata_args()
        unix_ora_add_trandata(src_args,trandata_args,src_other_args,_REMOTE_GGSCI_COMMAND)
    elif args.item == 'del_trandata':
        trandata_args = get_trandata_args()
        unix_ora_del_trandata(src_args,trandata_args,src_other_args,_REMOTE_GGSCI_COMMAND)
    elif args.item == 'info_trandata':
        trandata_args = get_trandata_args()
        unix_ora_info_trandata(src_args,trandata_args,src_other_args,_REMOTE_GGSCI_COMMAND)



if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger.error(e)
        sys.exit(1)
