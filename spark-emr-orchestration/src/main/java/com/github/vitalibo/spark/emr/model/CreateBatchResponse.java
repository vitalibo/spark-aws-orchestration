package com.github.vitalibo.spark.emr.model;

import com.fasterxml.jackson.annotation.JsonUnwrapped;
import lombok.Data;

@Data
public class CreateBatchResponse {

    @JsonUnwrapped
    private Batch batch;

    public CreateBatchResponse withBatch(Batch batch) {
        this.batch = batch;
        return this;
    }

}