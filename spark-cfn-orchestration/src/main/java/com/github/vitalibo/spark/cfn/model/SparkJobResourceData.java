package com.github.vitalibo.spark.cfn.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.github.vitalibo.cfn.resource.model.ResourceData;
import lombok.Data;

@Data
public class SparkJobResourceData extends ResourceData<SparkJobResourceData> {

    @JsonProperty(value = "SessionId")
    private String sessionId;

    @JsonProperty(value = "ApplicationId")
    private String applicationId;

    public SparkJobResourceData withSessionId(String sessionId) {
        this.sessionId = sessionId;
        return this;
    }

    public SparkJobResourceData withApplicationId(String applicationId) {
        this.applicationId = applicationId;
        return this;
    }

}