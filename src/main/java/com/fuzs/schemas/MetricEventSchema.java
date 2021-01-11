package com.fuzs.schemas;

import java.io.IOException;
import java.nio.charset.Charset;

import com.fuzs.model.MetricEvent;
import com.google.gson.Gson;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

public class MetricEventSchema implements DeserializationSchema<MetricEvent>, SerializationSchema<MetricEvent> {

    private static final Gson gson = new Gson();

    @Override
    public TypeInformation<MetricEvent> getProducedType() {
       
        return TypeInformation.of(MetricEvent.class);
    }

    @Override
    public byte[] serialize(MetricEvent element) {
       
        return gson.toJson(element).getBytes(Charset.forName("UTF-8"));
    }

    @Override
    public MetricEvent deserialize(byte[] message) throws IOException {
       
        return gson.fromJson(new String(message), MetricEvent.class);
    }

    @Override
    public boolean isEndOfStream(MetricEvent nextElement) {
       
        return false;
    }
    
}
