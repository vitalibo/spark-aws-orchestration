package com.github.vitalibo.spark.etl;

import lombok.Getter;

public class ActivityException extends RuntimeException {

    @Getter
    private final String errorMessage;
    @Getter
    private final String causeMessage;

    public ActivityException(String errorMessage, String causeMessage) {
        super(errorMessage);
        this.errorMessage = errorMessage;
        this.causeMessage = causeMessage;
    }

}