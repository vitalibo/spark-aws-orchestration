package com.github.vitalibo.spark.cfn.model;

import com.github.vitalibo.cfn.resource.model.ResourceProperties;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

@Getter
@RequiredArgsConstructor
public enum ResourceType implements com.github.vitalibo.cfn.resource.model.ResourceType {

    SparkJob("Custom::SparkJob", SparkJobResourceProperties.class);

    private final String typeName;
    private final Class<? extends ResourceProperties> typeClass;

}