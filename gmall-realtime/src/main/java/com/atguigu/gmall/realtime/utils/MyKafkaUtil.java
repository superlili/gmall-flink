package com.atguigu.gmall.realtime.utils;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.Properties;

public class MyKafkaUtil {
    public static String KAFKA_SERVER = "Ahadoop102:9092,Ahadoop103:9092,Ahadoop104:9092";
    public static Properties properties = new Properties();

    static {
        properties.setProperty("bootstrap.servers",KAFKA_SERVER);
    }

    public static FlinkKafkaProducer<String> getKafkaSink(String topic){
        return new FlinkKafkaProducer<String>(topic,new SimpleStringSchema(),properties);
    }
}
