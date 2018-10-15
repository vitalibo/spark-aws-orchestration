package com.github.vitalibo.spark.emr.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

@Data
public class GetBatchesRequest {

    @JsonProperty(value = "from")
    private Integer from;

    @JsonProperty(value = "size")
    private Integer size;

    public GetBatchesRequest withFrom(Integer from) {
        this.from = from;
        return this;
    }

    public GetBatchesRequest withSize(Integer size) {
        this.size = size;
        return this;
    }

}