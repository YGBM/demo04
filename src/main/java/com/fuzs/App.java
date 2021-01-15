package com.fuzs;


import com.fuzs.model.MetricEvent;
import com.fuzs.schemas.MetricEventSchema;
import com.fuzs.utils.ExecutionEnvUtil;
import com.fuzs.utils.KafkaConfigUtil;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;


/**
 * {"name":"fzs","timestamp":1610323617,"fields":{"c1":"c1"},"tags":{"c2":"c2"}}
 *
 */
public class App {
    public static void main(String[] args) throws Exception {
        final ParameterTool parameterTool = ExecutionEnvUtil.createParameterTool(args);
        StreamExecutionEnvironment env = ExecutionEnvUtil.prepare(parameterTool);
        DataStreamSource<MetricEvent> data = KafkaConfigUtil.buildSource(env);
        data.print();
        // data.addSink(new FlinkKafkaProducer011<MetricEvent>(
        //         "10.10.14.48:9092",
        //         "test",
        //         new MetricEventSchema()
        // ));
        env.execute();
        
    }
}
