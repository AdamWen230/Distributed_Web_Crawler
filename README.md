# Distributed_Web_Crawler
第四届字节跳动青训营-大数据专场项目-简易分布式爬虫系统-WGLHLZH队

## 项目介绍
基于Flink+kafka/redis+爬虫框架+存储实现一个简易分布式爬虫系统，通过Flink流式计算实现待爬取url的去重，接着将去重后的url发送到kafka或者redis消息队列里，然后再把url分发给各个爬虫节点，经过解析处理后将数据存到数据库。

## 项目分工
### 韩二帅
主要是基于分布式思想，实现url的两种分发策略，redis跟kafka
### 文俊
1. 部分基于Kafka-Flink的分布式爬虫系统设计
2. 搭建配置Hadoop集群以及Kafka-Zookeeper集群
3. 基于Flink流式计算框架实现URL的管理与分发
4. 对接负责爬虫同学进行联调测试
### 郭子毅
通过python的urllib库和lxml库编写单机爬虫
