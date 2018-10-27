package com.github.vitalibo.spark.emr.model;

import com.github.vitalibo.spark.emr.TestHelper;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Collections;

public class CreateBatchRequestTest {

    @Test
    public void testWith() {
        CreateBatchRequest request = new CreateBatchRequest()
            .withFile("s3://spark/examples/jars/spark-examples-2.3.2.jar")
            .withProxyUser("hadoop")
            .withClassName("org.apache.spark.examples.SparkPi")
            .withArgs(Arrays.asList("10"))
            .withJars(Arrays.asList("s3://spark/examples/jars/spark-examples.jar"))
            .withPyFiles(Arrays.asList("s3://spark/examples/jars/spark-examples.py"))
            .withFiles(Arrays.asList("s3://spark/examples/jars/spark-examples.ini"))
            .withDriverMemory("512m")
            .withDriverCores(1)
            .withExecutorMemory("2G")
            .withExecutorCores(2)
            .withNumExecutors(3)
            .withArchives(Arrays.asList("s3://spark/examples/jars/spark-examples.zip"))
            .withQueue("default")
            .withName("SparkPi")
            .withConfiguration(Collections.singletonMap(
                "spark.master", "spark://5.6.7.8:7077"));

        String actual = TestHelper.asJson(request);

        Assert.assertEquals(actual, TestHelper.resourceAsJson("/CreateBatchRequest.json"));
    }

}