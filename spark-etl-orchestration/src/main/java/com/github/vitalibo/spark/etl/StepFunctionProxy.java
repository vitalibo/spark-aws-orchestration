package com.github.vitalibo.spark.etl;

import com.amazonaws.services.stepfunctions.AWSStepFunctions;
import com.amazonaws.services.stepfunctions.model.SendTaskFailureRequest;
import com.amazonaws.services.stepfunctions.model.SendTaskHeartbeatRequest;
import com.amazonaws.services.stepfunctions.model.SendTaskSuccessRequest;
import com.amazonaws.services.stepfunctions.model.TaskTimedOutException;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RequiredArgsConstructor
public class StepFunctionProxy {

    private static final Logger logger = LoggerFactory.getLogger(StepFunctionProxy.class);

    private final AWSStepFunctions client;
    private final int maxRetryAttempts;

    private final String taskToken;

    public StepFunctionProxy(AWSStepFunctions client, String taskToken) {
        this(client, 3, taskToken);
    }

    public void taskSuccess(String outputJson) {
        client.sendTaskSuccess(
            new SendTaskSuccessRequest()
                .withOutput(outputJson)
                .withTaskToken(taskToken));
    }

    public boolean taskSyncHeartbeat() {
        int retryAttempts = 0;
        while (true) {
            if (++retryAttempts > maxRetryAttempts) {
                return false;
            }

            try {
                client.sendTaskHeartbeat(
                    new SendTaskHeartbeatRequest()
                        .withTaskToken(taskToken));

            } catch (TaskTimedOutException e) {
                logger.warn(e.getMessage());
                continue;
            }

            return true;
        }
    }

    public void taskFailure(String error, String cause) {
        client.sendTaskFailure(
            new SendTaskFailureRequest()
                .withError(error)
                .withCause(cause)
                .withTaskToken(taskToken));
    }

}