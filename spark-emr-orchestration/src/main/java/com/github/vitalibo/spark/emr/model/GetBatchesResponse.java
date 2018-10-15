package com.github.vitalibo.spark.emr.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

import java.util.List;

@Data
public class GetBatchesResponse {

    @JsonProperty(value = "from")
    private Integer from;

    @JsonProperty(value = "total")
    private Integer total;

    @JsonProperty(value = "sessions")
    private List<Batch> sessions;

    public GetBatchesResponse withFrom(Integer from) {
        this.from = from;
        return this;
    }

    public GetBatchesResponse withTotal(Integer total) {
        this.total = total;
        return this;
    }

    public GetBatchesResponse withSessions(List<Batch> sessions) {
        this.sessions = sessions;
        return this;
    }

}