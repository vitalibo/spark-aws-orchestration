package com.github.vitalibo.spark.emr.model;

import com.fasterxml.jackson.annotation.JsonUnwrapped;
import lombok.Data;

@Data
public class GetBatchResponse {

    @JsonUnwrapped
    private Batch batch;

    public GetBatchResponse withBatch(Batch batch) {
        this.batch = batch;
        return this;
    }

}