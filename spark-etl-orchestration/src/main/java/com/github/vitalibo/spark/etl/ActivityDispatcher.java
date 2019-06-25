package com.github.vitalibo.spark.etl;

import com.amazonaws.services.stepfunctions.AWSStepFunctions;
import com.amazonaws.services.stepfunctions.model.GetActivityTaskRequest;
import com.amazonaws.services.stepfunctions.model.GetActivityTaskResult;
import com.github.vitalibo.spark.etl.model.Activity;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;

import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.*;

@RequiredArgsConstructor
public class ActivityDispatcher {

    private final AWSStepFunctions client;
    private final List<String> activityArns;

    public Iterable<Activity> fetch() {
        return () -> new DistributedIterator(client, activityArns);
    }

    static class DistributedIterator extends ExecutorCompletionService<Activity> implements Iterator<Activity> {

        private final AWSStepFunctions client;

        public DistributedIterator(AWSStepFunctions client, List<String> activityArns) {
            super(Executors.newFixedThreadPool(activityArns.size()));
            this.client = client;
            activityArns.forEach(arn -> this.submit(new ActivityFetcher(arn)));
        }

        @Override
        public boolean hasNext() {
            return true;
        }

        @Override
        @SneakyThrows({InterruptedException.class, ExecutionException.class})
        public Activity next() {
            Future<Activity> future = this.take();
            return future.get();
        }

        @RequiredArgsConstructor
        class ActivityFetcher implements Callable<Activity> {

            private final String activityArn;

            @Override
            public Activity call() {
                GetActivityTaskResult activityTask;
                do {
                    activityTask = client.getActivityTask(
                        new GetActivityTaskRequest()
                            .withActivityArn(activityArn));

                } while (Objects.isNull(activityTask.getTaskToken()));

                DistributedIterator.this.submit(this);
                return new Activity()
                    .withTaskToken(activityTask.getTaskToken())
                    .withJsonInput(activityTask.getInput())
                    .withArn(activityArn);
            }
        }
    }
}