package com.github.vitalibo.spark.etl.model;

import lombok.Data;

@Data
public class Activity {

    private String arn;
    private String taskToken;
    private String jsonInput;

    public Activity withArn(String arn) {
        this.arn = arn;
        return this;
    }

    public Activity withTaskToken(String taskToken) {
        this.taskToken = taskToken;
        return this;
    }

    public Activity withJsonInput(String jsonInput) {
        this.jsonInput = jsonInput;
        return this;
    }

    public String getName() {
        return arn.split(":")[6];
    }

}