package com.github.vitalibo.spark.cfn.model;

import com.github.vitalibo.spark.cfn.TestHelper;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Collections;

public class SparkJobResourcePropertiesTest {

    @Test
    public void testAsJson() {
        SparkJobResourceProperties properties = makeSparkJobResourceProperties();

        String actual = TestHelper.asJson(properties);

        Assert.assertEquals(
            actual, TestHelper.resourceAsJson("/SparkJobResourceProperties.json"));
    }

    @Test
    public void testFromJson() {
        SparkJobResourceProperties actual = TestHelper
            .resourceAsObject("/SparkJobResourceProperties.json", SparkJobResourceProperties.class);

        Assert.assertEquals(actual, makeSparkJobResourceProperties());
    }

    private static SparkJobResourceProperties makeSparkJobResourceProperties() {
        SparkJobResourceProperties properties = new SparkJobResourceProperties();
        properties.setLivyHost("ip-10-1-234-56.us-west-1.compute.internal");
        properties.setLivyPort(8998);
        SparkJobResourceProperties.Parameters parameters = new SparkJobResourceProperties.Parameters();
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
        properties.setParameters(parameters);
        properties.setServiceToken("arn:aws:lambda:us-west-1:1234567890:function:spark");
        return properties;
    }

}