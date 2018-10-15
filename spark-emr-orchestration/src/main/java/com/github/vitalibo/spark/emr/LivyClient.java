package com.github.vitalibo.spark.emr;

import com.github.vitalibo.spark.emr.model.*;

public interface LivyClient {

    /**
     * Returns all the active batch sessions.
     */
    GetBatchesResponse getBatches(GetBatchesRequest request);

    /**
     * The created {@link com.github.vitalibo.spark.emr.model.Batch} object.
     */
    CreateBatchResponse createBatch(CreateBatchRequest request);

    /**
     * Returns the batch session information.
     */
    GetBatchResponse getBatch(GetBatchesRequest request);

    /**
     * Returns the state of batch session
     */
    GetBatchStateResponse getBatchState(GetBatchStateRequest request);

    /**
     * Kills the Batch job.
     */
    KillBatchResponse killBatch(KillBatchRequest request);

    /**
     * Gets the log lines from this batch.
     */
    GetBatchLogResponse getBatchLog(GetBatchLogRequest request);

}