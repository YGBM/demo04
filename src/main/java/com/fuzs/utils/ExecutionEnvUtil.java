package com.fuzs.utils;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import static com.fuzs.constant.PropertiesConstant.*;

import java.io.IOException;

public class ExecutionEnvUtil {

    public static ParameterTool createParameterTool(final String[] args) throws IOException {
        return ParameterTool.fromPropertiesFile(ExecutionEnvUtil.class.getResourceAsStream(PROPERTIES_FILE_NAME))
                .mergeWith(ParameterTool.fromArgs(args)).mergeWith(ParameterTool.fromSystemProperties());
    }

    public static final ParameterTool PARAMETER_TOOL = createParameterTool();

    private static ParameterTool createParameterTool() {
        try {
            return ParameterTool.fromPropertiesFile(ExecutionEnvUtil.class.getResourceAsStream(PROPERTIES_FILE_NAME))
                    .mergeWith(ParameterTool.fromSystemProperties());
        } catch (IOException e) {
            e.printStackTrace();
        }
        return ParameterTool.fromSystemProperties();
    }

    public static StreamExecutionEnvironment prepare(ParameterTool parameterTool) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(parameterTool.getInt(STREAM_PARALLELISM,5));
        env.getConfig().disableSysoutLogging();
        env.getConfig().setRestartStrategy(RestartStrategies.fixedDelayRestart(4, 60000));
        
        if(parameterTool.getBoolean(STREAM_CHECKPOINT_ENABLE,true)){
            env.enableCheckpointing(parameterTool.getLong(STREAM_CHECKPOINT_INTERVAL,10000));
        }

        env.getConfig().setGlobalJobParameters(parameterTool);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        return env;
    }
}
