package com.github.vitalibo.spark.emr;

import com.github.vitalibo.spark.emr.model.*;
import lombok.RequiredArgsConstructor;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.MediaType;

@RequiredArgsConstructor
public class LivyHttpClient implements LivyClient {

    private final Client client;
    private final String target;

    @Override
    public GetBatchesResponse getBatches(GetBatchesRequest request) {
        return client
            .target(target)
            .path("/batches")
            .queryParam("from", request.getFrom())
            .queryParam("size", request.getSize())
            .request(MediaType.APPLICATION_JSON)
            .get(GetBatchesResponse.class);
    }

    @Override
    public CreateBatchResponse createBatch(CreateBatchRequest request) {
        return client
            .target(target)
            .path("/batches")
            .request(MediaType.APPLICATION_JSON)
            .post(Entity.entity(request, MediaType.APPLICATION_JSON))
            .readEntity(CreateBatchResponse.class);
    }

    @Override
    public GetBatchResponse getBatch(GetBatchRequest request) {
        return client
            .target(target)
            .path(String.format("/batches/%s", request.getBatchId()))
            .request(MediaType.APPLICATION_JSON)
            .get(GetBatchResponse.class);
    }

    @Override
    public GetBatchStateResponse getBatchState(GetBatchStateRequest request) {
        return client
            .target(target)
            .path(String.format("/batches/%s/state", request.getBatchId()))
            .request(MediaType.APPLICATION_JSON)
            .get(GetBatchStateResponse.class);
    }

    @Override
    public KillBatchResponse killBatch(KillBatchRequest request) {
        return client
            .target(target)
            .path(String.format("/batches/%s", request.getBatchId()))
            .request(MediaType.APPLICATION_JSON)
            .delete(KillBatchResponse.class);
    }

    @Override
    public GetBatchLogResponse getBatchLog(GetBatchLogRequest request) {
        return client
            .target(target)
            .path(String.format("/batches/%s/log", request.getBatchId()))
            .queryParam("from", request.getFrom())
            .queryParam("size", request.getSize())
            .request(MediaType.APPLICATION_JSON)
            .get(GetBatchLogResponse.class);
    }

}