package com.atguigu.gamlllogger.controller;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@Slf4j
public class LoggerController {
    @Autowired
    private KafkaTemplate<String,String> kafkaTemplate;

    @RequestMapping("/applog")
    public String getLogger(@RequestParam("param") String jsonStr){
        //1 落盘
        log.info(jsonStr);

        //2 写入kafka
        kafkaTemplate.send("ods_base_log",jsonStr);

        return "success";
    }

    @RequestMapping("/test")
    public String test(@RequestParam("name") String name){
        System.out.println(name);
        return "success: " + name;
    }
}
