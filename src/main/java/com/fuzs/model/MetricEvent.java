package com.fuzs.model;

import java.util.Map;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
@ToString
public class MetricEvent {
    private String name;
    private Long timestamp;
    private Map<String,Object> fields;
    private Map<String,String> tags;
}
