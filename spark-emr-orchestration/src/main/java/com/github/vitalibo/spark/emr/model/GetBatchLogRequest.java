package com.github.vitalibo.spark.emr.model;

import lombok.Data;

@Data
public class GetBatchLogRequest {

    private Integer batchId;
    private Integer from;
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