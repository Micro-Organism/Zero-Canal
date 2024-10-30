package com.zero.canal.common.client;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import top.javatool.canal.client.annotation.CanalTable;
import top.javatool.canal.client.context.CanalContext;
import top.javatool.canal.client.handler.EntryHandler;
import top.javatool.canal.client.model.CanalModel;

import java.util.Map;

@Slf4j
@Component
@CanalTable(value = "all")
public class AllConsumer implements EntryHandler<Map<String, Object>> {

    @Override
    public void insert(Map<String, Object> map) {
        CanalModel canal = CanalContext.getModel();
        log.info("add，{}", map);
    }

    @Override
    public void update(Map<String, Object> before, Map<String, Object> after) {
        // CanalModel可以得到当前这次的库名和表名
        CanalModel canal = CanalContext.getModel();
        log.info("update，update before={}，update after={}", before, after);
    }

    @Override
    public void delete(Map<String, Object> map) {
        log.info("delete，{}", map);
    }

}
