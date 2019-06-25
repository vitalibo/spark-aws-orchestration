package com.github.vitalibo.spark.etl;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.services.stepfunctions.AWSStepFunctions;
import com.amazonaws.services.stepfunctions.AWSStepFunctionsClientBuilder;
import com.github.vitalibo.spark.emr.LivyClientBuilder;
import com.github.vitalibo.spark.etl.facade.SparkJobFacade;
import lombok.Getter;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;

public final class Factory {

    private static final String AWS_REGION = "AWS_REGION";
    private static final String ACTIVITY_ARNS = "ACTIVITY_ARNS";

    @Getter(lazy = true)
    private static final Factory instance = new Factory(System.getenv());

    private final AWSStepFunctions stepFunctionsClient;
    private final List<String> activityArns;

    Factory(Map<String, String> env) {
        ClientConfiguration clientConfiguration = new ClientConfiguration();
        clientConfiguration.setSocketTimeout((int) TimeUnit.SECONDS.toMillis(70));

        this.stepFunctionsClient = AWSStepFunctionsClientBuilder.standard()
            .withRegion(env.get(AWS_REGION))
            .withClientConfiguration(clientConfiguration)
            .build();
        this.activityArns = Arrays.asList(
            env.get(ACTIVITY_ARNS).split(";"));
    }

    public ActivityWorker createWorker() {
        return new ActivityWorker(
            stepFunctionsClient, 10);
    }

    public ActivityDispatcher createDispatcher() {
        return new ActivityDispatcher(
            stepFunctionsClient, activityArns);
    }

    public Facade createSparkJob() {
        return new SparkJobFacade((host, port) ->
            new LivyClientBuilder()
                .withHost(host)
                .withPort(port)
                .build(),
            5000, 100);
    }

    public interface LivyClient extends BiFunction<String, Integer, com.github.vitalibo.spark.emr.LivyClient> {

        default com.github.vitalibo.spark.emr.LivyClient create(String host, Integer port) {
            return apply(String.format("http://%s", host), Objects.nonNull(port) ? port : 8998);
        }
    }
}