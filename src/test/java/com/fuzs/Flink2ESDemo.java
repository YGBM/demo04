package com.fuzs;


import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch.util.RetryRejectedExecutionFailureHandler;
import org.apache.flink.streaming.connectors.elasticsearch7.ElasticsearchSink;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;
 
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
 
/**
 * author: xdoctorx.blog.csdn.net
 * date: 2020.05.31 23.02.58
 * description: flink2ES
 */
public class Flink2ESDemo {
 
        public static void main(String[] args) throws Exception {
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
 
            Map properties= new HashMap();
            properties.put("bootstrap.servers", "10.10.14.48:9092");
            properties.put("group.id", "flink-xdoctorx");
            properties.put("enable.auto.commit", "true");
            properties.put("auto.commit.interval.ms", "1000");
            properties.put("auto.offset.reset", "earliest");
            properties.put("session.timeout.ms", "30000");
            properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            properties.put("topic", "xxx");
            // parse user parameters
 
            ParameterTool parameterTool = ParameterTool.fromMap(properties);
 
            FlinkKafkaConsumer011<String> consumer  = new FlinkKafkaConsumer011<>(
                    parameterTool.getRequired("topic"), new SimpleStringSchema(), parameterTool.getProperties());
 
 
            DataStream<String> messageStream = env.addSource(consumer);
 
 
            List<HttpHost> esHttphost = new ArrayList();
            esHttphost.add(new HttpHost("10.10.14.48", 9200, "http"));
 
            ElasticsearchSink.Builder<String> esSinkBuilder = new ElasticsearchSink.Builder (
                    esHttphost,
                    new ElasticsearchSinkFunction<String>() {
                        public IndexRequest createIndexRequest(String element) {
                            Map<String, String> json = new HashMap ();
                            json.put("data", element);
                            
                            return Requests.indexRequest()
                                    .index("topic-flink-fuzusheng")
                                
                                    .source(json);
                        }
 
                        public void process(String element, RuntimeContext ctx, RequestIndexer indexer) {
                            indexer.add(createIndexRequest(element));
                        }
                    }
            );
 
            esSinkBuilder.setBulkFlushMaxActions(1);
//        esSinkBuilder.setRestClientFactory(
//                restClientBuilder -> {
//                    restClientBuilder.setDefaultHeaders()
//                }
//        );
//            esSinkBuilder.setRestClientFactory(new RestClientFactoryImpl());
            esSinkBuilder.setFailureHandler(new RetryRejectedExecutionFailureHandler());
 
            messageStream.addSink(esSinkBuilder.build());
            env.execute("flink learning connectors kafka");
 
        }
 
}