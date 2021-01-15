package com.fuzs;

import com.fuzs.model.MetricEvent;
import com.fuzs.utils.ESSinkUtil;
import com.fuzs.utils.ExecutionEnvUtil;
import com.fuzs.utils.KafkaConfigUtil;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch.util.RetryRejectedExecutionFailureHandler;
import org.apache.flink.streaming.connectors.elasticsearch7.ElasticsearchSink;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import static com.fuzs.constant.PropertiesConstant.*;



@Slf4j
public class Sink2ES7Main {

	public static void main(String[] args) throws Exception {
        final ParameterTool parameterTool = ExecutionEnvUtil.createParameterTool(args);
        StreamExecutionEnvironment env = ExecutionEnvUtil.prepare(parameterTool);

        List<HttpHost> esAddresses = ESSinkUtil.getEsAddresses(parameterTool.get(ELASTICSEARCH_HOSTS));
        int bulkSize = parameterTool.getInt(ELASTICSEARCH_BULK_FLUSH_MAX_ACTIONS, 40);
        int sinkParallelism = parameterTool.getInt(STREAM_SINK_PARALLELISM, 1);

        log.info("-----esAddresses = {}, parameterTool = {}, ", esAddresses, parameterTool);

        DataStreamSource<String> data = KafkaConfigUtil.buildStringSource(env, "xxx", 0L);
       

        data.print();
        
        List<HttpHost> esHttphost = new ArrayList();
        esHttphost.add(new HttpHost("10.10.14.48", 9200, "http"));

        ElasticsearchSink.Builder<String> esSinkBuilder = new ElasticsearchSink.Builder (
                esHttphost,
                new ElasticsearchSinkFunction<String>() {
                    public IndexRequest createIndexRequest(String element) {
                        Map<String, String> json = new HashMap ();
                        json.put("data", element);
                        
                        return Requests.indexRequest()
                                .index("fuzusheng")
                                .source(json);
                    }

                    public void process(String element, RuntimeContext ctx, RequestIndexer indexer) {
                        indexer.add(createIndexRequest(element));
                    }
                }
        );
        esSinkBuilder.setBulkFlushMaxActions(1);
        esSinkBuilder.setFailureHandler(new RetryRejectedExecutionFailureHandler());
        data.addSink(esSinkBuilder.build());
        
        env.execute("flink learning connectors es7");
    }
}
