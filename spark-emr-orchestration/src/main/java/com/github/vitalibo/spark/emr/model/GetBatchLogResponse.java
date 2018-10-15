package com.github.vitalibo.spark.emr.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

import java.util.List;

@Data
public class GetBatchLogResponse {

    @JsonProperty(value = "id")
    private Integer batchId;

    @JsonProperty(value = "from")
    private Integer from;

    @JsonProperty(value = "size")
    private Integer size;

    @JsonProperty(value = "log")
    private List<String> log;

    public GetBatchLogResponse withBatchId(Integer batchId) {
        this.batchId = batchId;
        return this;
    }

    public GetBatchLogResponse withFrom(Integer from) {
        this.from = from;
        return this;
    }

    public GetBatchLogResponse withSize(Integer size) {
        this.size = size;
        return this;
    }

    public GetBatchLogResponse withLog(List<String> log) {
        this.log = log;
        return this;
    }

}