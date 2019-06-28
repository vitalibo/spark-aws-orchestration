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
        SparkActivityInput.Properties properties = new SparkActivityInput.Properties();
        properties.setFile("s3://spark/examples/jars/spark-examples-2.3.2.jar");
        properties.setProxyUser("hadoop");
        properties.setClassName("org.apache.spark.examples.SparkPi");
        properties.setArgs(Arrays.asList("10"));
        properties.setJars(Arrays.asList("s3://spark/examples/jars/spark-examples.jar"));
        properties.setPyFiles(Arrays.asList("s3://spark/examples/jars/spark-examples.py"));
        properties.setFiles(Arrays.asList("s3://spark/examples/jars/spark-examples.ini"));
        properties.setDriverMemory("512m");
        properties.setDriverCores(1);
        properties.setExecutorMemory("2G");
        properties.setExecutorCores(2);
        properties.setNumExecutors(3);
        properties.setArchives(Arrays.asList("s3://spark/examples/jars/spark-examples.zip"));
        properties.setQueue("default");
        properties.setName("SparkPi");
        properties.setConfiguration(Collections.singletonMap(
            "spark.master", "spark://5.6.7.8:7077"));
        activityInput.setProperties(properties);
        return activityInput;
    }

}