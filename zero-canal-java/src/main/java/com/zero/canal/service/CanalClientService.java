package com.zero.canal.service;

import com.alibaba.fastjson2.JSONObject;
import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


@Slf4j
@Component
public class CanalClientService implements InitializingBean, DisposableBean {

    @Value("${canal.host}")
    private String canalHost;

    @Value("${canal.port}")
    private int canalPort;

    @Value("${canal.destination}")
    private String canalDestination;

    @Value("${canal.username}")
    private String canalUsername;

    @Value("${canal.password}")
    private String canalPassword;

    @Value("${canal.batch.size}")
    private int batchSize;


    private CanalConnector canalConnector;

    private ExecutorService executorService;

    @Bean
    public void canalConnector() {
        this.canalConnector = CanalConnectors.newSingleConnector(
                new InetSocketAddress(canalHost, canalPort),
                canalDestination,
                canalUsername,
                canalPassword
        );
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        this.canalConnector = CanalConnectors.newSingleConnector(
                new InetSocketAddress(canalHost, canalPort),
                canalDestination,
                canalUsername,
                canalPassword
        );
        this.executorService = Executors.newSingleThreadExecutor();
        this.executorService.execute(new Task());
    }

    @Override
    public void destroy() throws Exception {
        if (executorService != null) {
            executorService.shutdown();
        }
    }

    private class Task implements Runnable {
        @Override
        public void run() {
            while (true) {
                try {
                    //连接
                    canalConnector.connect();
                    //订阅
                    canalConnector.subscribe();
                    while (true) {
                        Message message = canalConnector.getWithoutAck(batchSize); // batchSize为每次获取的batchSize大小
                        long batchId = message.getId();
                        //获取批量的数量
                        int size = message.getEntries().size();
                        try {
                            //如果没有数据
                            if (batchId == -1 || size == 0) {
                                // log.info("无数据");
                                // 线程休眠2秒
                                Thread.sleep(2000);
                            } else {
                                // 如果有数据,处理数据
                                printEntry(message.getEntries());
                                // 确认处理完成
                                canalConnector.ack(batchId);
                            }
                        } catch (Exception e) {
                            log.error(e.getMessage());
                            // 程序错误，也直接确认，跳过这次偏移
                            canalConnector.ack(batchId);
                        }
                    }
                } catch (Exception e) {
                    log.error("Error occurred when running Canal Client", e);
                } finally {
                    canalConnector.disconnect();
                }
            }
        }

        private void printEntry(List<CanalEntry.Entry> entrys) {
            for (CanalEntry.Entry entry : entrys) {
                if (isTransactionEntry(entry)) {
                    //开启/关闭事务的实体类型，跳过
                    continue;
                }
                //RowChange对象，包含了一行数据变化的所有特征
                //比如isDdl 是否是ddl变更操作 sql 具体的ddl sql beforeColumns afterColumns 变更前后的数据字段等等
                CanalEntry.RowChange rowChange;
                try {
                    rowChange = CanalEntry.RowChange.parseFrom(entry.getStoreValue());
                } catch (Exception e) {
                    throw new RuntimeException("ERROR ## parser of eromanga-event has an error , data:" + entry.toString(), e);
                }
                //获取操作类型：insert/update/delete类型
                CanalEntry.EventType eventType = rowChange.getEventType();
                //打印Header信息
                log.info("================》; binlog[{} : {}] , name[{}, {}] , eventType : {}",
                        entry.getHeader().getLogfileName(), entry.getHeader().getLogfileOffset(),
                        entry.getHeader().getSchemaName(), entry.getHeader().getTableName(),
                        eventType);
                //判断是否是DDL语句
                if (rowChange.getIsDdl()) {
                    log.info("================》;isDdl: true,sql:{}", rowChange.getSql());
                }
                log.info(rowChange.getSql());
                //获取RowChange对象里的每一行数据，打印出来
                for (CanalEntry.RowData rowData : rowChange.getRowDatasList()) {
                    //如果是删除语句
                    if (eventType == CanalEntry.EventType.DELETE) {
                        log.info(">>>>>>>>>> 删除 >>>>>>>>>>");
                        printColumnAndExecute(rowData.getBeforeColumnsList(), "DELETE");
                        //如果是新增语句
                    } else if (eventType == CanalEntry.EventType.INSERT) {
                        log.info(">>>>>>>>>> 新增 >>>>>>>>>>");
                        printColumnAndExecute(rowData.getAfterColumnsList(), "INSERT");
                        //如果是更新的语句
                    } else {
                        log.info(">>>>>>>>>> 更新 >>>>>>>>>>");
                        //变更前的数据
                        log.info("------->; before");
                        printColumnAndExecute(rowData.getBeforeColumnsList(), null);
                        //变更后的数据
                        log.info("------->; after");
                        printColumnAndExecute(rowData.getAfterColumnsList(), "UPDATE");
                    }
                }
            }
        }

        /**
         * 执行数据同步
         *
         * @param columns   列
         * @param type  类型
         */
        private void printColumnAndExecute(List<CanalEntry.Column> columns, String type) {
            if (type == null) {
                return;
            }
            JSONObject jsonObject = new JSONObject();
            for (CanalEntry.Column column : columns) {
                jsonObject.put(column.getName(), column.getValue());
                log.info("{}: {}", column.getName(), column.getValue());
            }
            // 此处使用json转对象的方式进行转换
            // JSONObject.parseObject(jsonObject.toString(), xxx.class)
            if (type.equals("INSERT")) {
                // 执行新增
                log.info("新增成功->{}", jsonObject.toJSONString());
            } else if (type.equals("UPDATE")) {
                // 执行编辑
                log.info("编辑成功->{}", jsonObject.toJSONString());
            } else if (type.equals("DELETE")) {
                // 执行删除
                log.info("删除成功->{}", jsonObject.toJSONString());
            }
        }

        /**
         * 判断当前entry是否为事务日志
         */
        private boolean isTransactionEntry(CanalEntry.Entry entry) {
            if (entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONBEGIN) {
                log.info("********* 日志文件为:{}, 事务开始偏移量为:{}, 事件类型为type={}",
                        entry.getHeader().getLogfileName(),
                        entry.getHeader().getLogfileOffset(),
                        entry.getEntryType()
                );
                return true;
            } else if (entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONEND) {
                log.info("********* 日志文件为:{}, 事务结束偏移量为:{}, 事件类型为type={}",
                        entry.getHeader().getLogfileName(),
                        entry.getHeader().getLogfileOffset(),
                        entry.getEntryType()
                );
                return true;
            } else {
                return false;
            }
        }
    }
}