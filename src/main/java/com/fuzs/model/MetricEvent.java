package com.fuzs.model;

import java.util.Map;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class MetricEvent {
    private String name;
    private Long timestamp;
    private Map<String,String> fields;
    private Map<String,String> tags;
}
