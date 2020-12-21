#!/usr/bin/env python
import paramiko
import logging
import time,re

import warnings
warnings.filterwarnings('ignore')


# logging
logging.basicConfig(format="%(levelname)s\t%(asctime)s\t%(message)s",filename="ogg.log")
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)



def ssh_input(args, cmd):
    
    host, port, username, password = args

    client = paramiko.SSHClient()
    client.load_system_host_keys()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect(host, port, username, password, look_for_keys=False)
    logger.debug("[%s:%s] connect: ok", host, port)

    try:
        remote_command = cmd
        stdin, stdout, stderr = client.exec_command(remote_command)
        logger.debug("[%s:%s] cmd: %s \nexecute: ok", host, port, remote_command)

        result = stdout.readlines()
        err = stderr.readlines()
        
        logger.debug("[%s:%s] result: %s", host, port, "".join(result))
        if err!=[] and "stty: standard " not in err[0] :
            logger.debug("[%s:%s] error: %s", host, port, "".join(err))
            print("".join(err))
            return err
        if '[YOU HAVE NEW MAIL]\n' in result:
            result.remove('[YOU HAVE NEW MAIL]\n')

        result = [ i for i in result if 'Last login ' not in i and i != '\n' and 'Oracle Corporation' not in i]
        return result
    finally:
        client.close()

        logger.debug("[%s:%s] close: ok", host, port)

def ssh_ftp(args,remotepath,localpath):
    host, port, username, password = args

    tran = paramiko.Transport((host, port))
    tran.connect(username=username, password=password)
    sftp = paramiko.SFTPClient.from_transport(tran)
    logger.debug("[%s:%s] connect: ok", host, port)
    try:
        sftp.get(remotepath, localpath)
        logger.debug("[%s:%s] get '%s' to '%s'",host, port, remotepath, localpath)
       # sftp.get(remotepath, localpath)
    finally:
        tran.close()
        logger.debug("[%s:%s] close: ok", host, port)


# scp传输文件不配置互信的方法
def ssh_scp(src_args, tag_args, localpath,remotepath):

    host, port, username, password = src_args
    r_host, r_port, r_username, r_password = tag_args

    trans = paramiko.Transport((host, port))    
    trans.start_client()
    trans.auth_password(username=username, password=password)
    channel = trans.open_session()
    channel.settimeout(7200)
    channel.get_pty()
    channel.invoke_shell()
    logger.debug("[%s:%s] connect: ok", host, port)
    try:
        remote_command = f'scp -o StrictHostKeyChecking=no -P {r_port} {localpath} {r_username}@{r_host}:{remotepath}\r'
        channel.send(remote_command)
        logger.debug("[%s:%s] cmd: %s \nexecute: ok", host, port, remote_command)

        while True:
            time.sleep(0.2)
            res1 = channel.recv(65535)
            res1 = res1.decode('utf-8')
            if 'password' in res1 :
                break

        if 'password' in res1:
            channel.send(f'{r_password}\r')

            while True:
                time.sleep(0.2)
                res3 = channel.recv(65535).decode('utf-8')
                print(res3)
                if '100%' in res3:
                    break

        # elif 'Are you sure you' in res1:
        #     channel.send('yes\r')
        #     while True:
        #         time.sleep(0.2)
        #         res2 = channel.recv(65535).decode('utf-8')
        #         print(res2)
        #         if 'password' in res2 :
        #             break
        #     channel.send(f'{r_password}\r')
        #     while True:
        #         time.sleep(0.2)
        #         res4 = channel.recv(65535).decode('utf-8')
        #         print(res4)
        #         if '100%' in res4:
        #             break
    finally:
        channel.close()
        trans.close()

        logger.debug("[%s:%s] close: ok", host, port)



    

