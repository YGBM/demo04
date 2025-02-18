package com.fuzs.constant;

public class PropertiesConstant {
    public static final String KAFKA_BROKERS = "kafka.brokers";
    public static final String DEFAULT_KAFKA_BROKERS = "localhost:9092";
    public static final String KAFKA_ZOOKEEPER_CONNECT = "kafka.zookeeper.connect";
    public static final String DEFAULT_KAFKA_ZOOKEEPER_CONNECT = "localhost:2181";
    public static final String KAFKA_GROUP_ID = "kafka.group.id";
    public static final String DEFAULT_KAFKA_GROUP_ID = "fuzs";

    public static final String METRICS_TOPIC = "metrics.topic";

    public static final String CONSUMER_FROM_TIME = "consumer.from.time";

    public static final String PROPERTIES_FILE_NAME = "/application.properties";
    public static final String STREAM_PARALLELISM = "stream.parallelism";
    public static final String STREAM_CHECKPOINT_ENABLE = "stream.checkpoint.enable";
    public static final String STREAM_CHECKPOINT_INTERVAL = "stream.checkpoint.interval";
    public static final String STREAM_SINK_PARALLELISM = "stream.sink.parallelism";

    public static final String ELASTICSEARCH_BULK_FLUSH_MAX_ACTIONS = "elasticsearch.bulk.flush.max.actions";
    public static final String ELASTICSEARCH_HOSTS = "elasticsearch.hosts";

    public static final String FUZS = "fuzusheng";
}
