package com.github.vitalibo.spark.emr.model;

import lombok.Data;

@Data
public class GetBatchRequest {

    private Integer batchId;

    public GetBatchRequest withBatchId(Integer batchId) {
        this.batchId = batchId;
        return this;
    }

}