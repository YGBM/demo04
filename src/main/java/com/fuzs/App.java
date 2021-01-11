package com.fuzs;


import com.fuzs.model.MetricEvent;
import com.fuzs.utils.ExecutionEnvUtil;
import com.fuzs.utils.KafkaConfigUtil;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


/**
 * Hello world!
 *
 */
public class App {
    public static void main(String[] args) throws Exception {
        final ParameterTool parameterTool = ExecutionEnvUtil.createParameterTool(args);
        StreamExecutionEnvironment env = ExecutionEnvUtil.prepare(parameterTool);
        DataStreamSource<MetricEvent> data = KafkaConfigUtil.buildSource(env);
        data.print();

        env.execute();
        
    }
}
