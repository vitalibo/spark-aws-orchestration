package com.github.vitalibo.spark.emr.model;

import lombok.Data;

@Data
public class GetBatchStateRequest {

    private Integer batchId;

    public GetBatchStateRequest withBatchId(Integer batchId) {
        this.batchId = batchId;
        return this;
    }

}
