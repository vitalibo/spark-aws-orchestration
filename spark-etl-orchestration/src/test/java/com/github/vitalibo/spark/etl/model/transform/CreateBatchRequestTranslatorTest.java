package com.github.vitalibo.spark.etl.model.transform;

import com.github.vitalibo.spark.emr.model.CreateBatchRequest;
import com.github.vitalibo.spark.etl.TestHelper;
import com.github.vitalibo.spark.etl.model.SparkActivityInput;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Collections;

public class CreateBatchRequestTranslatorTest {

    @Test
    public void testFrom() {
        SparkActivityInput activityInput = TestHelper
            .resourceAsObject("/SparkActivityInput.json", SparkActivityInput.class);

        CreateBatchRequest actual = CreateBatchRequestTranslator.from(activityInput.getParameters());

        Assert.assertNotNull(actual);
        Assert.assertEquals(actual.getFile(), "s3://spark/examples/jars/spark-examples-2.3.2.jar");
        Assert.assertEquals(actual.getProxyUser(), "hadoop");
        Assert.assertEquals(actual.getClassName(), "org.apache.spark.examples.SparkPi");
        Assert.assertEquals(actual.getArgs(), Arrays.asList("10"));
        Assert.assertEquals(actual.getJars(), Arrays.asList("s3://spark/examples/jars/spark-examples.jar"));
        Assert.assertEquals(actual.getPyFiles(), Arrays.asList("s3://spark/examples/jars/spark-examples.py"));
        Assert.assertEquals(actual.getFiles(), Arrays.asList("s3://spark/examples/jars/spark-examples.ini"));
        Assert.assertEquals(actual.getDriverMemory(), "512m");
        Assert.assertEquals(actual.getDriverCores(), (Integer) 1);
        Assert.assertEquals(actual.getExecutorMemory(), "2G");
        Assert.assertEquals(actual.getExecutorCores(), (Integer) 2);
        Assert.assertEquals(actual.getNumExecutors(), (Integer) 3);
        Assert.assertEquals(actual.getArchives(), Arrays.asList("s3://spark/examples/jars/spark-examples.zip"));
        Assert.assertEquals(actual.getQueue(), "default");
        Assert.assertEquals(actual.getName(), "SparkPi");
        Assert.assertEquals(actual.getConfiguration(), Collections.singletonMap(
            "spark.master", "spark://5.6.7.8:7077"));
    }

}