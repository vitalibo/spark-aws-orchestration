package com.github.vitalibo.spark.emr;

import com.github.vitalibo.spark.emr.model.*;

public interface LivyClient {

    GetBatchesResponse getBatches(GetBatchesRequest request);

    CreateBatchResponse createBatch(CreateBatchRequest request);

    GetBatchResponse getBatch(GetBatchRequest request);

    default GetBatchResponse getBatch(Integer batchId) {
        return this.getBatch(
            new GetBatchRequest()
                .withBatchId(batchId));
    }

    GetBatchStateResponse getBatchState(GetBatchStateRequest request);

    default GetBatchStateResponse getBatchState(Integer batchId) {
        return this.getBatchState(
            new GetBatchStateRequest()
                .withBatchId(batchId));
    }

    KillBatchResponse killBatch(KillBatchRequest request);

    default KillBatchResponse killBatch(Integer batchId) {
        return this.killBatch(
            new KillBatchRequest()
                .withBatchId(batchId));
    }

    GetBatchLogResponse getBatchLog(GetBatchLogRequest request);

}