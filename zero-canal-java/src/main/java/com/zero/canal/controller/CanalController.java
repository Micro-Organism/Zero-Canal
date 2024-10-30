package com.zero.canal.controller;

import com.zero.canal.common.client.AllConsumer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.HashMap;
import java.util.Map;

@RestController
@RequestMapping("/canal")
public class CanalController {

    AllConsumer allConsumer;
    @Autowired
    public void setAllConsumer(AllConsumer allConsumer) {
        this.allConsumer = allConsumer;
    }

    @RequestMapping("info")
    public Map<String, Object> showHelloWorld(){
        Map<String, Object> map = new HashMap<>();
        map.put("msg", "HelloWorld");
        allConsumer.insert(map);
        return map;
    }

}