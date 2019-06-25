package com.github.vitalibo.spark.etl.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

@Data
public class SparkActivityOutput extends ActivityOutput {

    @JsonProperty(value = "applicationId")
    private String applicationId;

    @JsonProperty(value = "batchId")
    private Integer batchId;

    public SparkActivityOutput withApplicationId(String applicationId) {
        this.applicationId = applicationId;
        return this;
    }

    public SparkActivityOutput withBatchId(Integer batchId) {
        this.batchId = batchId;
        return this;
    }

}