package com.github.vitalibo.spark.etl;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.services.stepfunctions.AWSStepFunctions;
import com.amazonaws.services.stepfunctions.AWSStepFunctionsClientBuilder;
import com.github.vitalibo.spark.emr.LivyClientBuilder;
import com.github.vitalibo.spark.etl.facade.SparkJobFacade;
import com.github.vitalibo.spark.etl.util.Rules;
import lombok.Getter;

import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;

public final class Factory {

    private static final String AWS_REGION = "AWS_REGION";

    @Getter(lazy = true)
    private static final Factory instance = new Factory(System.getenv());

    private final AWSStepFunctions stepFunctionsClient;

    Factory(Map<String, String> env) {
        this.stepFunctionsClient = createAWSStepFunctions(env.get(AWS_REGION));
    }

    public ActivityDispatcher createDispatcher(String... activityArns) {
        return new ActivityDispatcher(
            stepFunctionsClient,
            Arrays.asList(activityArns));
    }

    public ActivityWorker createWorker() {
        return new ActivityWorker(
            stepFunctionsClient, 10);
    }

    public Facade createSparkJob() {
        return new SparkJobFacade(
            new Rules<>(
                ValidationRules::verifyLivyHost,
                ValidationRules::verifyLivyPort),
            (host, port) -> new LivyClientBuilder()
                .withHost(host)
                .withPort(port)
                .build(),
            30000);
    }

    private AWSStepFunctions createAWSStepFunctions(String awsRegion) {
        ClientConfiguration clientConfiguration = new ClientConfiguration();
        clientConfiguration.setSocketTimeout((int) TimeUnit.SECONDS.toMillis(70));

        return AWSStepFunctionsClientBuilder.standard()
            .withRegion(awsRegion)
            .withClientConfiguration(clientConfiguration)
            .build();
    }

    public interface LivyClient extends BiFunction<String, Integer, com.github.vitalibo.spark.emr.LivyClient> {

        default com.github.vitalibo.spark.emr.LivyClient create(String host, Integer port) {
            return apply(String.format("http://%s", host), Objects.nonNull(port) ? port : 8998);
        }
    }
}