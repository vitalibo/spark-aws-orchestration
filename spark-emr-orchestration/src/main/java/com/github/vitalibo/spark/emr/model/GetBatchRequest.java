package com.github.vitalibo.spark.emr.model;

import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.Data;

@Data
public class GetBatchRequest {

    @JsonIgnore
    private Integer batchId;

    public GetBatchRequest withBatchId(Integer batchId) {
        this.batchId = batchId;
        return this;
    }

}