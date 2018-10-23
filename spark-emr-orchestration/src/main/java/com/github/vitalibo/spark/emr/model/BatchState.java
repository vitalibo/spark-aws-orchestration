package com.github.vitalibo.spark.emr.model;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.github.vitalibo.spark.emr.model.transform.BatchStateDeserializer;

@JsonDeserialize(using = BatchStateDeserializer.class)
public enum BatchState {

    STARTING, RECOVERING, RUNNING, DEAD, SUCCESS

}