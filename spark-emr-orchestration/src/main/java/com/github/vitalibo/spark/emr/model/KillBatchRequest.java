package com.github.vitalibo.spark.emr.model;

import lombok.Data;

@Data
public class KillBatchRequest {

    private Integer batchId;

    public KillBatchRequest withBatchId(Integer batchId) {
        this.batchId = batchId;
        return this;
    }

}