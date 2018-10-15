package com.github.vitalibo.spark.emr.model;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

@Data
public class GetBatchLogRequest {

    @JsonIgnore
    private Integer batchId;

    @JsonProperty(value = "from")
    private Integer from;

    @JsonProperty(value = "size")
    private Integer size;

    public GetBatchLogRequest withBatchId(Integer batchId) {
        this.batchId = batchId;
        return this;
    }

    public GetBatchLogRequest withFrom(Integer from) {
        this.from = from;
        return this;
    }

    public GetBatchLogRequest withSize(Integer size) {
        this.size = size;
        return this;
    }

}