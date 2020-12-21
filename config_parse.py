# -*- coding:utf-8 -*-
import os
import configparser




def get_config(item):
    config = configparser.ConfigParser()
    config.read("config.cfg")

    host = config.get(item,'host')
    username = config.get(item,'user')
    port = config.getint(item,'port')
    password = config.get(item,'password')

    args = [host, port, username, password]

    return args

def get_init_args(item):
    config = configparser.ConfigParser()
    config.read("config.cfg")
    
    sync_segments = config.get('ogg_init_config','sync_segments')
    if item == 'source_config':
        asm_passwd = config.get('ogg_init_config','src_sysasm_passwd')
        dmp_dir = config.get('ogg_init_config','src_dmp_dir')
    elif item == 'target_config':
        asm_passwd = config.get('ogg_init_config','tag_sysasm_passwd')
        dmp_dir = config.get('ogg_init_config','tag_dmp_dir')


    init_args = [sync_segments,asm_passwd,dmp_dir]

    return init_args


def get_other_args(item):
    config = configparser.ConfigParser()
    config.read("config.cfg")
    ora_user = config.get(item,'oracle_user')
    sid = config.get(item,'sid')
    ogg_dir = config.get(item,'ogg_dir')

    os_args = [ora_user,sid,ogg_dir]

    return os_args


def get_vali_args():
    item = 'ogg_validate_config'
    config = configparser.ConfigParser()
    config.read("config.cfg")
    src_ogg_user = config.get(item,'src_ogg_user')
    src_ogg_passwd = config.get(item,'src_ogg_passwd')
    src_db_port = config.get(item,'src_db_port')
    tag_dmp_dir = config.get(item,'tag_dmp_dir')

    vali_args = [src_ogg_user,src_ogg_passwd,src_db_port,tag_dmp_dir]

    return vali_args



def get_reinit_args():
    item = 'ogg_reinitial_config'
    config = configparser.ConfigParser()
    config.read("config.cfg")
    reinital_tables = config.get(item,'reinital_tables')
    src_ogg_user = config.get(item,'src_ogg_user')
    src_ogg_passwd = config.get(item,'src_ogg_passwd')
    src_db_port = config.get(item,'src_db_port')
    tag_dmp_dir = config.get(item,'tag_dmp_dir')
    ogg_rep = config.get(item,'ogg_rep')

    reinit_args = [src_ogg_user,src_ogg_passwd,src_db_port,tag_dmp_dir,reinital_tables,ogg_rep]

    return reinit_args



def get_trandata_args():
    item = 'ogg_trandata_config'
    config = configparser.ConfigParser()
    config.read("config.cfg")
    os_type = config.get(item,'os_type')
    db_type = config.get(item,'db_type')
    ogg_ext = config.get(item,'ogg_ext_name')
    ext_object = config.get(item,'ext_object')


    trandata_args = [os_type,db_type,ogg_ext,ext_object]

    return trandata_args
