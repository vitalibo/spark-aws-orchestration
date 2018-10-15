package com.github.vitalibo.spark.emr.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

@Data
public class GetBatchStateResponse {

    @JsonProperty(value = "id")
    private Integer batchId;

    @JsonProperty(value = "state")
    private String state;

    public GetBatchStateResponse withBatchId(Integer batchId) {
        this.batchId = batchId;
        return this;
    }

    public GetBatchStateResponse withState(String state) {
        this.state = state;
        return this;
    }

}