package com.github.vitalibo.spark.etl.model;

import com.github.vitalibo.spark.etl.TestHelper;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Collections;

public class SparkActivityInputTest {

    @Test
    public void testAsJson() {
        SparkActivityInput activityInput = makeSparkActivityInput();

        String actual = TestHelper.asJson(activityInput);

        Assert.assertEquals(
            actual, TestHelper.resourceAsJson("/SparkActivityInput.json"));
    }

    @Test
    public void testFromJson() {
        SparkActivityInput actual = TestHelper
            .resourceAsObject("/SparkActivityInput.json", SparkActivityInput.class);

        Assert.assertEquals(actual, makeSparkActivityInput());
    }

    private static SparkActivityInput makeSparkActivityInput() {
        SparkActivityInput activityInput = new SparkActivityInput();
        activityInput.setLivyHost("ip-10-1-234-56.us-west-1.compute.internal");
        activityInput.setLivyPort(8998);
        SparkActivityInput.Parameters parameters = new SparkActivityInput.Parameters();
        parameters.setFile("s3://spark/examples/jars/spark-examples-2.3.2.jar");
        parameters.setProxyUser("hadoop");
        parameters.setClassName("org.apache.spark.examples.SparkPi");
        parameters.setArgs(Arrays.asList("10"));
        parameters.setJars(Arrays.asList("s3://spark/examples/jars/spark-examples.jar"));
        parameters.setPyFiles(Arrays.asList("s3://spark/examples/jars/spark-examples.py"));
        parameters.setFiles(Arrays.asList("s3://spark/examples/jars/spark-examples.ini"));
        parameters.setDriverMemory("512m");
        parameters.setDriverCores(1);
        parameters.setExecutorMemory("2G");
        parameters.setExecutorCores(2);
        parameters.setNumExecutors(3);
        parameters.setArchives(Arrays.asList("s3://spark/examples/jars/spark-examples.zip"));
        parameters.setQueue("default");
        parameters.setName("SparkPi");
        parameters.setConfiguration(Collections.singletonMap(
            "spark.master", "spark://5.6.7.8:7077"));
        activityInput.setParameters(parameters);
        return activityInput;
    }

}