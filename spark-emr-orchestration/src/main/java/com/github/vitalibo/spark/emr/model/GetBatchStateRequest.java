package com.github.vitalibo.spark.emr.model;

import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.Data;

@Data
public class GetBatchStateRequest {

    @JsonIgnore
    private Integer batchId;

    public GetBatchStateRequest withBatchId(Integer batchId) {
        this.batchId = batchId;
        return this;
    }

}
