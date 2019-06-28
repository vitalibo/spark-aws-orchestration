package com.github.vitalibo.spark.etl;

import com.amazonaws.services.stepfunctions.AWSStepFunctions;
import com.amazonaws.services.stepfunctions.model.GetActivityTaskRequest;
import com.amazonaws.services.stepfunctions.model.GetActivityTaskResult;
import com.github.vitalibo.spark.etl.model.Activity;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.*;

@RequiredArgsConstructor
public class ActivityDispatcher {

    private static final Logger logger = LoggerFactory.getLogger(ActivityDispatcher.class);

    private final AWSStepFunctions client;
    private final List<String> activities;

    public Iterable<Activity> fetch() {
        ConcurrentIterator iterator = new ConcurrentIterator(client, activities.size());
        iterator.init(activities);
        return () -> iterator;
    }

    @RequiredArgsConstructor
    static class ConcurrentIterator implements Iterator<Activity> {

        private final CompletionService<Activity> executor;
        private final AWSStepFunctions client;

        ConcurrentIterator(AWSStepFunctions client, int numberOfThreads) {
            this(new ExecutorCompletionService<>(Executors.newFixedThreadPool(numberOfThreads)), client);
        }

        void init(List<String> activities) {
            activities.forEach(arn ->
                executor.submit(new ActivityFetcher(arn)));
        }

        @Override
        public boolean hasNext() {
            return true;
        }

        @Override
        @SneakyThrows({InterruptedException.class, ExecutionException.class})
        public Activity next() {
            Future<Activity> future = executor.take();
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

                logger.info("{} - Fetch new activity task", activityArn);
                executor.submit(this);
                return new Activity()
                    .withTaskToken(activityTask.getTaskToken())
                    .withJsonInput(activityTask.getInput())
                    .withArn(activityArn);
            }
        }
    }
}