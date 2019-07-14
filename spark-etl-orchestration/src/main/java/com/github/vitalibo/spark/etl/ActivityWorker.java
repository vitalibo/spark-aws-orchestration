package com.github.vitalibo.spark.etl;

import com.amazonaws.services.stepfunctions.AWSStepFunctions;
import com.amazonaws.util.json.Jackson;
import com.github.vitalibo.spark.emr.ValidationException;
import com.github.vitalibo.spark.etl.model.Activity;
import com.github.vitalibo.spark.etl.model.ActivityException;
import com.github.vitalibo.spark.etl.model.ActivityInput;
import com.github.vitalibo.spark.etl.model.ActivityOutput;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.ParameterizedType;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@RequiredArgsConstructor
public class ActivityWorker implements AutoCloseable {

    private static final Logger logger = LoggerFactory.getLogger(ActivityWorker.class);

    private final AWSStepFunctions client;
    private final ExecutorService executorService;

    ActivityWorker(AWSStepFunctions client, int numberOfThreads) {
        this(client, Executors.newFixedThreadPool(numberOfThreads));
    }

    public void submit(Facade facade, Activity activity) {
        StepFunctionProxy proxy = new StepFunctionProxy(
            client, activity.getTaskToken());

        executorService.submit(() -> submit(proxy, facade, activity));
    }

    void submit(StepFunctionProxy proxy, Facade<?, ?> facade, Activity activity) {
        try {
            ActivityOutput output = facade.process(
                proxy, castActivityInput(
                    facade.getClass(), activity));

            proxy.taskSuccess(
                Jackson.toJsonString(output));
        } catch (ValidationException | ActivityException e) {
            proxy.taskFailure(
                e.getClass().getName(), e.getMessage());
        } catch (Exception e) {
            proxy.taskFailure(
                e.getClass().getName(), e.getMessage());
            logger.error(e.getMessage(), e);
        }
    }

    @Override
    public void close() {
        executorService.shutdown();
    }

    @SuppressWarnings("unchecked")
    private static <T extends ActivityInput> T castActivityInput(Class<? extends Facade> facadeClass, Activity activity) {
        return Jackson.fromJsonString(activity.getJsonInput(), (Class<T>) ((ParameterizedType)
            facadeClass.getGenericInterfaces()[0]).getActualTypeArguments()[0]);
    }

}