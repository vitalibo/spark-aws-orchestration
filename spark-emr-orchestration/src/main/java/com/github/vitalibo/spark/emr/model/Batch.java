package com.github.vitalibo.spark.emr.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

import java.util.List;
import java.util.Map;

@Data
public class Batch {

    @JsonProperty(value = "id")
    private Integer batchId;

    @JsonProperty(value = "appId")
    private String applicationId;

    @JsonProperty(value = "appInfo")
    private Map<String, String> applicationInfo;

    @JsonProperty(value = "log")
    private List<String> log;

    @JsonProperty(value = "state")
    private String state;

    public Batch withBatchId(Integer batchId) {
        this.batchId = batchId;
        return this;
    }

    public Batch withApplicationId(String applicationId) {
        this.applicationId = applicationId;
        return this;
    }

    public Batch withApplicationInfo(Map<String, String> applicationInfo) {
        this.applicationInfo = applicationInfo;
        return this;
    }

    public Batch withLog(List<String> log) {
        this.log = log;
        return this;
    }

    public Batch withState(String state) {
        this.state = state;
        return this;
    }

}