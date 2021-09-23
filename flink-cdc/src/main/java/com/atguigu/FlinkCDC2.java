package com.atguigu;

import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import com.alibaba.ververica.cdc.debezium.StringDebeziumDeserializationSchema;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FlinkCDC2 {
    public static void main(String[] args) throws Exception {
        //1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

//        //2.Flink-CDC将读取binlog的位置信息 以状态的方式保存在CK，如果想要做到断点续传，徐娅萍从Checkpoint或者Savepoint启动程序
//        //2.1 开启Checkpoint，每隔5秒钟做一次CK
//        env.enableCheckpointing(5000L);
//        //2.2 指定CK的一致性语义
//        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
//        //2.3 设置任务关闭的时候保留最后一次CK数据
//        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
//
//        //2.4 指定从CK自动重启策略
//        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,2000L));
//
//        //2.5 设置状态后端
//        env.setStateBackend(new FsStateBackend("hdfs://Ahadoop102:8020/flinkCDC"));
//        //2.6 设置访问HDFS的用户名
//        System.setProperty("HADOOP_USER_NAME","zhouli");

//        env.getCheckpointConfig().setCheckpointTimeout(1000L);
//        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
//        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(2000L);


        //3.创建Flink-MySQL-CDC的source
        DebeziumSourceFunction<String> mysqlSource = MySQLSource.<String>builder()
                .hostname("Ahadoop102")
                .username("root")
                .password("123456")
                .port(3306)
                .databaseList("gmall-flink-2021")
                .tableList("gmall-flink-2021.base_trademark")
                .startupOptions(StartupOptions.initial())
                .deserializer(new MyStringDebeziumDeserializationSchema())
                .build();

        //4.使用CDC Source 从 MySQL中读取数据
        DataStreamSource<String> mysqlDS = env.addSource(mysqlSource);

        mysqlDS.print();

        env.execute();
    }
}
