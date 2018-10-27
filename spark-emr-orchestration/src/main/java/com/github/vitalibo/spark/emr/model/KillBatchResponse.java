package com.github.vitalibo.spark.emr.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

@Data
public class KillBatchResponse {

    @JsonProperty(value = "msg")
    private String message;

}