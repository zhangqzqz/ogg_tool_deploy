
oracle 自动化运维工具

修订记录

| 作者 | 版本 | 时间       | 备注       |
| ---- | ---- | ---------- | ---------- |
| 张全针 | v1.1 | 2020/01/19 | ogg自动部署          |
| 张全针 | v1.2 | 2020/04/19 | ogg自启动部署          |
| 张全针 | v2.0 | 2020/05/15 | ogg切换前检查功能        |
| 张全针 | v2.1 | 2020/05/18 | 添加全量传输scp功能及其他优化       |
| 张全针 | v3.0 | 2020/05/25 | 添加表重新初始化功能|
| 张全针 | v4.0 | 2020/06/22 | 添加附加日志管理功能|


## 定位
        给MC公司内部人员使用，提供自动化运维功能，提高工作效率


## 先决条件

        1.执行环境与目标环境打通ssh连接
        2.两端数据库已经搭建完成，源端归档已经打开。

### 使用表初始化功能的先决条件

        1.执行该脚本的前提条件是人为排除掉需要初始化的表(使用mapexclude排除)，并且应用进程已经追平数据。
	2.只支持单个应用进程下的表重新初始化，用户无此限制，多应用进程条件下，可多次执行脚本实现。
        3.建议初始化表的大小小于10g。
	4.不支持含有DBLINK不支持字段的表。


## 注意

        1.本脚本在导入时对于目标端已存在表执行的是replace操作
        2.请确保同步表或用户在源端是存在可用状态
        3.两端ogg进程将使用7800-7810端口段,请确保该端口未被占用

## 支持的操作系统和数据库版本配对
          支持的oracle版本：10g单机及rac,11g单机及rac
          支持的ogg版本：11g,12c
          支持的系统：Linux,AIX

 




## 使用说明

### 两端环境配置

        1.配置library环境变量
        AIX : LIBPATH
        LINUX :LD_LIBRARY_PATH
        例如：
        export LIBRARY_PATH=$ORACLE_HOME/lib:$LIBRARY_PATH

        2.两端建好ogg目录，并将ogg介质上传到该目录并使用oracle软件用户解压



### 本地 python运行环境

python 3 64位

        1.在本地解压ogg工具包

        2. 进入脚本目录
        pip install -r requirements.txt    #安装项目python依赖包prettytable与paramiko。  #第一次安装时用

        如果网速慢，可以用以下命令安装，使用国内的PIP源

           ```
           pip install -i http://mirrors.aliyun.com/pypi/simple/ --trusted-host mirrors.aliyun.com -r requirements.txt
           ```




## 代码说明

| 文件或目录       | 功能                 | 备注                                                         |
| --------------- | -------------------- | ------------------------------------------------------------ |
| ogg_run.py      | 命令行主入口         |                                                             |
| ogg.log   | 安装脚本输出结果日志  |                                                              |


### 参数文件介绍
     [source_config]                                                  --源端远程登录参数
     host=192.168.238.36
     user=root
     port=22
     password=hzmcdba
     oracle_user=oracle
     sid=ora11g2
     ogg_dir=/oradata/ogg


     [target_config]                                                  --目标端远程登录参数
     host=192.168.238.56
     user=root
     port=22
     password=hzmcdba
     oracle_user=oracle
     sid=orcl
     ogg_dir=/ogg


     [ogg_init_config]                                                --ogg自动部署功能参数
     sync_segments=zqz.zqz1,zmy.*
     src_sysasm_passwd=oracle
     tag_sysasm_passwd=oracle
     src_dmp_dir=/oradata/ogg
     tag_dmp_dir=/ogg

     [ogg_validate_config]                                            --ogg切换前检查参数
     src_ogg_user=mc_odc
     src_ogg_passwd=mc_odc
     src_db_port=1521
     tag_dmp_dir=/ogg


     [ogg_reinitial_config]                                           --ogg表初始化功能参数
     reinital_tables=zqz.zqz1,zmy.zqz1
     src_db_port=1521
     src_ogg_user=mc_odc
     src_ogg_passwd=mc_odc
     ogg_rep=mc_rep
     tag_dmp_dir=/ogg


     [ogg_trandata_config]                                              -ogg附加日志管理参数，
                                                                        ext_object：需要添加附加日志的用户或者用户名和表名，用逗号分开,如果有pdb需要添加pdb（如：用户级别：pdb1.owner1.*，或者owner1.*，owner2.*....
                                                                        表级别： pdb1.owner1.table1，或者owner1.table1，owner.table2....)
                                                                        该参数可选，如果没有提供，则默认添加抽取进程中配置的所有用户或表的附加日志。
     os_type=linux
     db_type=oracle
     ogg_ext_name=mc_ext
     ext_object=zqz.t1,zmy.zqz1


     远程登录参数是必须填写的参数项，其余的在需要调用功能时添加即可

## 主程序的使用
python ogg_run.py

python ogg_run.py  -h    ------(查看帮助）
python ogg_run.py   dbcheck        ------(ogg搭建前检查）
python ogg_run.py   src        ------(配置源端ogg）
python ogg_run.py   tag        ------(配置目标端ogg，不启动应用）
python ogg_run.py  full        -----（全量传输)
python ogg_run.py  check_ogg        -----（检查ogg进程状态)
python ogg_run.py  config_srv        -----（配置ogg进程自启动，尚未适配oracle数据库状态启动检查）
python ogg_run.py  vali_check        -----（ogg切换前基础检查)
python ogg_run.py  slow_table        -----（veridata慢表信息查询）
python ogg_run.py  reinitial_table        -----（ogg表重新初始化）
python ogg_run.py  add_trandata        -----（源库添加附加日志）
python ogg_run.py  del_trandata        -----（源库删除附加日志）
python ogg_run.py  info_trandata        -----（源库查看附加日志）
