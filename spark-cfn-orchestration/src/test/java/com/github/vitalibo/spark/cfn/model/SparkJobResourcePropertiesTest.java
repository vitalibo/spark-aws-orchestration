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
        properties.setServiceToken("arn:aws:lambda:us-west-1:1234567890:function:spark");
        return properties;
    }

}