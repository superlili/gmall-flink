package com.atguigu;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.ververica.cdc.debezium.DebeziumDeserializationSchema;
import io.debezium.data.Envelope;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

public class MyStringDebeziumDeserializationSchema implements DebeziumDeserializationSchema<String> {
    @Override
    public void deserialize(SourceRecord sourceRecord, Collector<String> collector) throws Exception {
        //创建JSON对象用于存放结果数据
        JSONObject result = new JSONObject();

        String topic = sourceRecord.topic();
        String[] fields = topic.split("\\.");
        String database = fields[1];
        String tableName = fields[2];

        Struct value = (Struct)sourceRecord.value();

        Struct beforeValue = value.getStruct("before");
        JSONObject beforeJson = new JSONObject();
        //获取元数据信息 通过元素据信息获取所有字段
        Schema schema = beforeValue.schema();
        if(beforeValue != null){
            for (Field field : schema.fields()) {
                beforeJson.put(field.name(),beforeValue.get(field));
            }
        }

        Struct afterValue = value.getStruct("after");
        JSONObject afterJson = new JSONObject();
        //获取元数据信息 通过元素据信息获取所有字段
        Schema afterSchema = afterValue.schema();
        if(afterValue != null){
            for (Field field : afterSchema.fields()) {
                afterJson.put(field.name(),afterJson.get(field));
            }
        }

        //获取操作类型 READ DELETE UPDATE CREATE
        Envelope.Operation operation = Envelope.operationFor(sourceRecord);
        String type = operation.toString().toLowerCase();
        if("create".equals(type)){
            type = "insert";
        }

        //将取到的数据写入结果输出
        result.put("database",database);
        result.put("tableName",tableName);
        result.put("before",beforeJson);
        result.put("after",afterJson);
        result.put("type",type);

        collector.collect(result.toJSONString());
    }

    @Override
    public TypeInformation<String> getProducedType() {
        return null;
    }
}
