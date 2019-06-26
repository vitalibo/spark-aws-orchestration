package com.github.vitalibo.spark.etl;

import com.github.vitalibo.spark.etl.model.Activity;

public class Application {

    private static final Factory factory = Factory.getInstance();

    public static void main(String[] args) {
        final ActivityDispatcher dispatcher = factory.createDispatcher(args);

        try (ActivityWorker worker = factory.createWorker()) {
            for (Activity activity : dispatcher.fetch()) {

                final Facade facade;
                switch (activity.getName()) {
                    case "spark-job":
                        facade = factory.createSparkJob();
                        break;
                    default:
                        throw new IllegalStateException("Unsupported activity");
                }

                worker.submit(facade, activity);
            }
        }
    }
}