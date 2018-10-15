package com.github.vitalibo.spark.emr.model;

import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.Data;

@Data
public class KillBatchRequest {

    @JsonIgnore
    private Integer batchId;

    public KillBatchRequest withBatchId(Integer batchId) {
        this.batchId = batchId;
        return this;
    }

}