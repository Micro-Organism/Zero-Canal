# Zero-Canal
Zero-Canal

# 1. 概述
## 1.1 简介
canal 是阿里开源的一款 MySQL 数据库增量日志解析工具，提供增量数据订阅和消费。

# 2. 功能
## 2.1. 原理
### 2.1.1. MySQL主备复制原理
1. MySQL master 将数据变更写入二进制日志（binary log）, 日志中的记录叫做二进制日志事件（binary log events，可以通过 show binlog events 进行查看）
2. MySQL slave 将 master 的 binary log events 拷贝到它的中继日志(relay log)
3. MySQL slave 重放 relay log 中事件，将数据变更反映到它自己的数据

### 2.1.2. canal 工作原理
1. Canal 模拟 MySQL slave 的交互协议，伪装自己为 MySQL slave ，向 MySQL master 发送 dump 协议
2. MySQL master 收到 dump 请求，开始推送 binary log 给 slave (即 Canal )
3. Canal 解析 binary log 对象(原始为 byte 流)

# 3. 使用
## 3.1. 搭建环境
### 3.1.1. docker-compose.yml
```yaml
version: '3'
services:
  mysql:
    image: registry.cn-hangzhou.aliyuncs.com/zhengqing/mysql:5.7
    container_name: mysql_3306
    restart: unless-stopped
    volumes:
      - "./mysql/my.cnf:/etc/mysql/my.cnf"
      - "./mysql/init-file.sql:/etc/mysql/init-file.sql"
      - "./mysql/data:/var/lib/mysql"
#      - "./mysql/conf.d:/etc/mysql/conf.d"
      - "./mysql/log/mysql/error.log:/var/log/mysql/error.log"
      - "./mysql/docker-entrypoint-initdb.d:/docker-entrypoint-initdb.d" # init sql script directory -- tips: it can be excute  when `/var/lib/mysql` is empty
    environment:                        # set environment,equals docker run -e
      TZ: Asia/Shanghai
      LANG: en_US.UTF-8
      MYSQL_ROOT_PASSWORD: root         # set root password
      MYSQL_DATABASE: root              # init database name
    ports:                              # port mappping
      - "3306:3306"
```
### 3.1.2. my.cnf
```ini

```
# 4. 其他

# 5. 参考