# 采集程序部署说明

采集程序目前支持文件采集和mysql数据库binlog的采集，并且都是将消息发送到RocketMQ，在运行采集程序前请确认已经在Logs服务器上安装RocketMQ，及建立好发送的Topic。  

```
RocketMQ本身支持在produce的时候自动创建topic，但考虑到给予Topic合理的配置，建议统一提前建立好Topic，不要利用produce自动创建。  

```  

dsdasdas
另外，请使用最新版的采集程序，采集程序[下载地址](ftp://192.168.1.211/%E6%8A%80%E6%9C%AF/BigData/collect-agent)，在需要采集的机器下载解压后即可。

## 1.环境准备

* jdk1.8

可下载[jdk1.8](ftp://192.168.1.211/%E6%8A%80%E6%9C%AF/BigData/jdk-8u144-linux-x64.tar.gz)  

执行tar zxvf jdk-8u144-linux-x64.tar.gz解压即可。  


## 2.文件采集  

文件采集主要是采集业务系统打印的log文件，发送给RocketMQ，由流清洗平台读取处理。  

#### 配置说明  

配置文件在conf/file-agent.properties，相应的配置参数共有两大块。  

##### 属性配置
  
* 1.采集关键参数  

属性 | 默认值 | 说明
-----|--------|-------
host.ip | 无 | 非必须配置，但是尽量配置，指定采集机ip，主要用与后续metric跟踪，可以知道每一台机器采集数据的情况 
rocketmq.nameServer | 无 | 必须配置，指定ROcketMQ的nameServer，多个可以以`;`分割  
rocketmq.producerGroup | 无 | 必须配置，指定发送给ROcketMQ的组，可以随机配置  
topics | 无 |  必须配置，指定需要采集发送的topics，此处可以多指，格式["a","b","c"]
business.systems | 无 | 必须配置，采集的系统，值的个数和格式需要同topics配置一样 
tail.dirs | 无 | 必须配置，需要采集的`目录`，值的个数需要同topics一致，格式["path1/dir1,path1/dir2","path2/dir3","path3"]，需要注意的是目前文件采集只支持目录下的文件采集，不会采集子目录文件。
include.fileName.pattern | 无 | 必须配置，指定需要采集文件名的正则匹配，值的个数同topics一致  
exclude.fileName.pattern | 无 | 非必须配置，排除不需要采集的文件，可以为空，但是一旦使用，请注意值个数需要与同topics一致  
position.file | 无 | 必须配置，指定采集文件记录采集位置的文件，该文件非常重要  
  
  
* 2. 采集优化参数  

属性 | 默认值 | 说明
-----|--------|----------
batch.bytes | 1048576 | 默认值1M，每次批量发送的最大消息
tail.process.seconds | 10 | 默认值10秒，触发消息发送有两个条件，要么采集的数据大小达到batch.bytes，要么采集时间达到tail.process.seconds  
emitter | logging,rocketmq | 默认值`logging,rocketmq`，上报采集相关的指标方式，目前支持两种方式，一种是打印文件对应**logging**，另外一种是**rocketmq**，同时支持两个以','分割 
send.bytesPerSecond | 1048576 | 默认值1M/秒，限制消息发送速度 
file.lastModify.interval.start | 197001010000 | 格式`yyyyMMddHHmm`，采集程序会获取文件最后modify time，只会采集modify time大于该值的文件  
file.refresh.peroid.seconds | 20 | 默认值20秒，当有新文件产生，会在20秒内发现，为了快速发现文件可以调小该值，但是会对性能产生影响 

##### 日志配置  

日志为采集程序运行时候的log，对应文件为conf/logback.xml。  

一般情况，使用时只需要修改`<property name="LOG_DIR" value="./logs" />`，指定log存储的位置即可。  

#### 程序运行

运行程序前线修改bin/file-agent.sh，指定JAVA_HOME路径  
```
export JAVA_HOME=/path/jdk1.8
```
保存后运行
`nohup bin/file-agent.sh > /dev/null 2>&1 &  `

## 3. MySQL采集

#### 环境配置  

采集MySQL必须确保binlog开启行模式和已经赋权限给指定用户做replication。  

##### 开启行模式

本身为master库，修改my.conf文件

```
log-bin=/path/log/mysql-bin
log-bin-index=/path/log/mysql-bin.index
binlog-format=ROW
```
**需要特别注意**，如果采集的是从库需要在my.conf文件中增加参数`log-slave-updates`。
```
log-slave-updates=1
log-bin=/path/log/mysql-bin
log-bin-index=/path/log/mysql-bin.index
binlog-format=ROW
```

##### 赋权限给指定用户 

在MySQL命令行下执行
```
GRANT SELECT, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'rocket'@'%' IDENTIFIED BY 'rocket';
flush privileges;
```

#### 配置说明 

属性 | 默认值 | 说明
-----|--------|-------
mysqlAddr | 无 | 必填，指定MySQL的服务器IP 
mysqlPort | 3306 | 默认值3306端口，指定MySQL的服务端口  
mysqlUsername | 无 | 必填，指定采集的用户名，使用具有REPLICATION权限的用户
mysqlPassword | 无 | 必填，指定对应用户密码  
mysqlTableWhiteList | 无 | 非必填，指定需要采集的表名，不填表示同步全部表，否则会同步指定的表，格式["aTable","bTable"]
storageType | FILE | 必填，采集数据库后支持写本地文件或者RocketMQ，`FILE`表示写本地文件，`RocketMQ`表示发送到RocketMQ   
fileNamePrefix | 无 | 当storageType是FILE时有效和必填，表示保存文件的文件名前缀 
dataTempPath | 无 | 当storageType是FILE时有效和必填，指定保存文件的位置
dataReservedDays | 15 | 当storageType是FILE时有效和必填，指定文件保存的天数
mqNamesrvAddr | 无 | 当storageType是ROCKETMQ时有效和必填，指定发送到RocketMQ对应的nameServer 
mqTopic | 无 | 当storageType是ROCKETMQ时有效和必填，指定发送的topic，目前只支持发送到一个topic  
startType | DEFAULT | 非必填，指定初始化binlog位置的方式，总共有四种方式`DEFAULT`、`NEW_EVENT`、`LAST_PROCESSED`和`SPECIFIED`，`DEFAULT`方式会首先尝试从MQ中获取上次处理的位置，如果没有则从binlog的最新位置开始同步；`NEW_EVENT`从binlog最新位置开始，`LAST_PROCESSED`从MQ中获取上次处理的位置，`SPECIFIED`自行指定binlog文件和位置 
binlogFilename | 无 | 只有当startType为`SPECIFIED`，才会生效 
nextPosition | 无 | 只有当startType为`SPECIFIED`，才会生效 
maxTransactionRows | 100 | 非必填，每个事务允许的最大行数，主要是考虑到大事务会导致消息过大，导致无法发送消息，才做此限制  

```
另外，日志相关的配置可以参考文件采集的日志配置 
```

#### 程序运行

运行程序前线修改bin/mysql-agent.sh，指定JAVA_HOME路径  
```
export JAVA_HOME=/path/jdk1.8
```
保存后运行  
`nohup bin/mysql-agent.sh > /dev/null 2>&1 &  `



