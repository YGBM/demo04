package com.fuzs;

import com.fuzs.model.MetricEvent;
import com.fuzs.utils.ESSinkUtil;
import com.fuzs.utils.ExecutionEnvUtil;
import com.fuzs.utils.GsonUtil;
import com.fuzs.utils.KafkaConfigUtil;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.api.common.functions.RuntimeContext;
import static com.fuzs.constant.PropertiesConstant.*;
import org.apache.http.HttpHost;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.xcontent.XContentType;
import java.util.List;


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
        List<HttpHost> esAddresses = ESSinkUtil.getEsAddresses(parameterTool.get(ELASTICSEARCH_HOSTS));
        int bulkSize = parameterTool.getInt(ELASTICSEARCH_BULK_FLUSH_MAX_ACTIONS,40);
        int sinkParallelism = parameterTool.getInt(STREAM_SINK_PARALLELISM,1);

        ESSinkUtil.addSink(esAddresses, bulkSize, sinkParallelism, data,
        (MetricEvent metric, RuntimeContext runtimeContext, RequestIndexer requestIndexer) -> {
            requestIndexer.add(Requests.indexRequest()
                    .index(FUZS + "_" + metric.getName())
                    .source(GsonUtil.toJSONBytes(metric), XContentType.JSON));
        },
        parameterTool);
        env.execute();
        
    }
}
