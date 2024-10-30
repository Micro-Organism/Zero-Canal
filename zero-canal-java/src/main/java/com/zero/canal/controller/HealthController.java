package com.zero.canal.controller;

import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.HashMap;
import java.util.Map;

@RestController
public class HealthController {

    @RequestMapping("/")
    public Map<String, Object> checkHealth() {
        Map<String, Object> map = new HashMap<>();
        map.put("code", 200);
        map.put("status", "success");
        map.put("message", "Welcome to Service");
        map.put("result", "服务正常运行中");
        return map;
    }

}