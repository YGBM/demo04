package com.fuzs;


import com.fuzs.utils.ExecutionEnvUtil;
import com.fuzs.utils.KafkaConfigUtil;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.api.common.serialization.SimpleStringSchema;



public class MsgFromKafka {
    public static void main(String[] args) throws Exception {
        final ParameterTool parameterTool = ExecutionEnvUtil.createParameterTool(args);
        StreamExecutionEnvironment env = ExecutionEnvUtil.prepare(parameterTool);
        DataStreamSource<String> data = env.addSource(new FlinkKafkaConsumer011<>(
            "xxx",
            new SimpleStringSchema(),
            KafkaConfigUtil.buildKafkaProps(parameterTool)
        ));
        data.print();

        env.execute();
    }
}
