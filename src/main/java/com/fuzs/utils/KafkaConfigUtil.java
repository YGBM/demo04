package com.fuzs.utils;

import java.util.Properties;

import static com.fuzs.constant.PropertiesConstant.*;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

import java.util.Map;
import java.util.HashMap;
import java.util.List;

import com.fuzs.model.MetricEvent;
import com.fuzs.schemas.MetricEventSchema;

public class KafkaConfigUtil {

    public static Properties buildKafkaProps() {
        return buildKafkaProps(ParameterTool.fromSystemProperties());
    }

    public static Properties buildKafkaProps(ParameterTool parameterTool) {
        Properties props = parameterTool.getProperties();
        props.put("bootstrap.servers", parameterTool.get(KAFKA_BROKERS, DEFAULT_KAFKA_BROKERS));
        props.put("zookeeper.connect", parameterTool.get(KAFKA_ZOOKEEPER_CONNECT, DEFAULT_KAFKA_ZOOKEEPER_CONNECT));
        props.put("group.id", parameterTool.get(KAFKA_GROUP_ID, DEFAULT_KAFKA_GROUP_ID));
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("auto.offset.reset", "latest");
        return props;
    }

    public static DataStreamSource<MetricEvent> buildSource(StreamExecutionEnvironment env) {
        ParameterTool parameterTool = (ParameterTool)env.getConfig().getGlobalJobParameters();
        String topic = parameterTool.getRequired(METRICS_TOPIC);
        Long time = parameterTool.getLong(CONSUMER_FROM_TIME,0L);
        return buildSource(env,topic,time);
    }

    public static DataStreamSource<MetricEvent> buildSource(StreamExecutionEnvironment env,String topic,Long time){
        ParameterTool parameterTool = (ParameterTool) env.getConfig().getGlobalJobParameters();
        Properties props = buildKafkaProps(parameterTool);
        FlinkKafkaConsumer011<MetricEvent> consumer = new FlinkKafkaConsumer011<>(
            topic,
            new MetricEventSchema(),
            props
        );
        if(time != 0L){
            Map<KafkaTopicPartition,Long> partitionOffset = buildOffsetByTime(props,parameterTool,time);
            consumer.setStartFromSpecificOffsets(partitionOffset);
        }
        return env.addSource(consumer);
    }

    public static DataStreamSource<String> buildStringSource(StreamExecutionEnvironment env) {
        ParameterTool parameterTool = (ParameterTool)env.getConfig().getGlobalJobParameters();
        String topic = parameterTool.getRequired(METRICS_TOPIC);
        Long time = parameterTool.getLong(CONSUMER_FROM_TIME,0L);
        return buildStringSource(env,topic,time);
    }

    public static DataStreamSource<String> buildStringSource(StreamExecutionEnvironment env,String topic,Long time){
        ParameterTool parameterTool = (ParameterTool) env.getConfig().getGlobalJobParameters();
        Properties props = buildKafkaProps(parameterTool);
        FlinkKafkaConsumer011<String> consumer = new FlinkKafkaConsumer011<>(
            topic,
            new SimpleStringSchema(),
            props
        );
        if(time != 0L){
            Map<KafkaTopicPartition,Long> partitionOffset = buildOffsetByTime(props,parameterTool,time);
            consumer.setStartFromSpecificOffsets(partitionOffset);
        }
        return env.addSource(consumer);
    }

    private static Map<KafkaTopicPartition,Long> buildOffsetByTime(Properties props,ParameterTool parameterTool,Long time){
        props.setProperty("group.id", "query_time_"+time);
        KafkaConsumer consumer = new KafkaConsumer(props);
        List<PartitionInfo> partitionsFor = consumer.partitionsFor(parameterTool.getRequired(METRICS_TOPIC));
        Map<TopicPartition,Long> partitionInfoLongMap = new HashMap<>();
        for(PartitionInfo partitionInfo : partitionsFor){
            partitionInfoLongMap.put(new TopicPartition(partitionInfo.topic(),partitionInfo.partition()), time);
        }
        Map<TopicPartition,OffsetAndTimestamp> offsetResult = consumer.offsetsForTimes(partitionInfoLongMap);
        Map<KafkaTopicPartition,Long> partitionOffset = new HashMap<>();
        offsetResult.forEach((key,value) -> partitionOffset.put(new KafkaTopicPartition(key.topic(),key.partition()), value.offset()));
        consumer.close();
        return partitionOffset;
        
    }
}
