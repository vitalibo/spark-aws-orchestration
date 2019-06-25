package com.github.vitalibo.spark.etl;

import com.amazonaws.services.stepfunctions.AWSStepFunctions;
import com.amazonaws.services.stepfunctions.model.SendTaskFailureRequest;
import com.amazonaws.services.stepfunctions.model.SendTaskHeartbeatRequest;
import com.amazonaws.services.stepfunctions.model.SendTaskSuccessRequest;
import com.amazonaws.util.json.Jackson;
import com.github.vitalibo.spark.etl.model.Activity;
import com.github.vitalibo.spark.etl.model.ActivityInput;
import com.github.vitalibo.spark.etl.model.ActivityOutput;
import lombok.RequiredArgsConstructor;

import java.lang.reflect.ParameterizedType;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@RequiredArgsConstructor
public class ActivityWorker implements AutoCloseable {

    private final AWSStepFunctions client;
    private final ExecutorService executorService;

    public ActivityWorker(AWSStepFunctions client, int numberOfThreads) {
        this(client, Executors.newFixedThreadPool(numberOfThreads));
    }

    public void submit(Facade facade, Activity activity) {
        Runnable newTask = () -> {
            final String taskToken = activity.getTaskToken();

            try {
                final ActivityOutput output = facade.process(
                    () -> client.sendTaskHeartbeat(
                        new SendTaskHeartbeatRequest()
                            .withTaskToken(taskToken)),

                    castActivityInput(
                        facade.getClass(), activity));

                client.sendTaskSuccess(
                    new SendTaskSuccessRequest()
                        .withOutput(Jackson.toJsonString(output))
                        .withTaskToken(taskToken));

            } catch (ActivityException e) {
                client.sendTaskFailure(
                    new SendTaskFailureRequest()
                        .withError(e.getErrorMessage())
                        .withCause(e.getCauseMessage())
                        .withTaskToken(taskToken));

            } catch (Exception e) {
                client.sendTaskFailure(
                    new SendTaskFailureRequest()
                        .withError(e.getClass().getName())
                        .withCause(e.getMessage())
                        .withTaskToken(taskToken));
            }
        };

        executorService.submit(newTask);
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

    public interface Context {

        void heartbeat();

    }

}