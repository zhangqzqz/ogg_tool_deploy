#!/usr/bin/env python

from logging_method import log
from ssh_input import ssh_input



# check the process of ogg
@log
def check_ogg(os_args,cmd_args,_REMOTE_GGSCI_COMMAND):
    
    _REMOTE_GGSCI_COMMAND = _REMOTE_GGSCI_COMMAND % (cmd_args[0],cmd_args[1],cmd_args[2],cmd_args[2],cmd_args[2])
    check_prc_cmd  = _REMOTE_GGSCI_COMMAND % "info all"
    check_prc_res = ssh_input(os_args,check_prc_cmd)
    check_prc_list = [list(filter(None,info.split(' '))) for info in check_prc_res if 'EXTRACT' in info or 'REPLICAT' in info]
    print(f"\nINFO: {os_args[0]}\n")
    print(''.join(check_prc_res[4:]))
    return check_prc_list

# parse the parameter of proceses
@log
def parse_prc():
    pass

# generate the script of start process
@log
def start_prc_srp(os_args,cmd_args,check_prc_list):
    ora_user,sid,ogg_dir = cmd_args
    start_prc = [f'start {prc[2]}' for prc in check_prc_list if 'RUNNING' in prc]
    start_prc_txt = '\n'.join(start_prc)
    start_ogg_txt = f'''sh date
start mgr
{start_prc_txt}'''
    ssh_input(os_args,f"echo '{start_ogg_txt}' >{ogg_dir}/start_ogg_mc.txt")
    ssh_input(os_args,f'''chown {ora_user} {ogg_dir}/start_ogg_mc.txt
chmod +x {ogg_dir}/start_ogg_mc.txt
''')
    print("\nINFO:OGG进程状态获取完成\n")


    start_ogg_sh_txt = '''echo 'su - %s <<EOF
export ORACLE_SID=%s
export LD_LIBRARY_PATH=%s:\$ORACLE_HOME/lib
export LIBPATH=%s:\$ORACLE_HOME/lib 
%s/ggsci paramfile %s/start_ogg_mc.txt>>%s/start_ogg_out.log
EOF'>%s/start_ogg_mc.sh
'''%(ora_user,sid,ogg_dir,ogg_dir,ogg_dir,ogg_dir,ogg_dir,ogg_dir)

    
    ssh_input(os_args,start_ogg_sh_txt)
    ssh_input(os_args,f'''chown {ora_user} {ogg_dir}/start_ogg_mc.sh
chmod +x {ogg_dir}/start_ogg_mc.sh
''')
    print("\nINFO:OGG启动脚本生成完成\n")

    service_txt='''#!/bin/bash
#chkconfig: 2345 80 90
#description:OggService
StartOggService(){
    sleep 2
    echo "start OggService..."
    %s/start_ogg_mc.sh
    echo "OggService start completed."
}
 
case $1 in
start)
      StartOggService
      ;;
esac
'''%ogg_dir
    ssh_input(os_args,f"echo '{service_txt}'>{ogg_dir}/service_ogg")
    ssh_input(os_args,f'''cp {ogg_dir}/service_ogg /etc/rc.d/init.d/OggService
chmod 755 /etc/rc.d/init.d/OggService 
chkconfig --add OggService
chkconfig OggService on''' )
    print("\nINFO:OGG进程开机自启动配置完成")