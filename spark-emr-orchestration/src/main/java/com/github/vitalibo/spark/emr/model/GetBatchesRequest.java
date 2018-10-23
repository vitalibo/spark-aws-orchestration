package com.github.vitalibo.spark.emr.model;

import lombok.Data;

@Data
public class GetBatchesRequest {

    private Integer from;
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