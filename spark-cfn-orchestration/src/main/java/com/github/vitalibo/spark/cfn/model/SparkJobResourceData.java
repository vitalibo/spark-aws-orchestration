package com.github.vitalibo.spark.cfn.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.github.vitalibo.cfn.resource.model.ResourceData;
import lombok.Data;

@Data
public class SparkJobResourceData extends ResourceData<SparkJobResourceData> {

    @JsonProperty(value = "BatchId")
    private Integer batchId;

    @JsonProperty(value = "ApplicationId")
    private String applicationId;

    public SparkJobResourceData withBatchId(Integer batchId) {
        this.batchId = batchId;
        return this;
    }

    public SparkJobResourceData withApplicationId(String applicationId) {
        this.applicationId = applicationId;
        return this;
    }

}