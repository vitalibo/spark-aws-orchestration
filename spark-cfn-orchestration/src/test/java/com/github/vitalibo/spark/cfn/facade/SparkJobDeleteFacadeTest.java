package com.github.vitalibo.spark.cfn.facade;

import com.amazonaws.services.lambda.runtime.Context;
import com.github.vitalibo.spark.cfn.LivyClientSync;
import com.github.vitalibo.spark.cfn.model.SparkJobResourceData;
import com.github.vitalibo.spark.cfn.model.SparkJobResourceProperties;
import com.github.vitalibo.spark.emr.model.KillBatchResponse;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class SparkJobDeleteFacadeTest {

    @Mock
    private Context mockContext;
    @Mock
    private LivyClientSync.Factory mockLivyFactory;
    @Mock
    private LivyClientSync mockLivyClientSync;

    private SparkJobDeleteFacade facade;

    @BeforeMethod
    public void setUp() {
        MockitoAnnotations.initMocks(this);
        facade = new SparkJobDeleteFacade(mockLivyFactory);
    }

    @Test
    public void testDelete() {
        Mockito.when(mockLivyFactory.create(Mockito.anyString(), Mockito.anyInt()))
            .thenReturn(mockLivyClientSync);
        Mockito.when(mockLivyClientSync.killBatch(Mockito.anyInt()))
            .thenReturn(new KillBatchResponse());
        SparkJobResourceProperties properties = makeSparkJobResourceProperties();

        SparkJobResourceData actual = facade.delete(
            properties, "application_1234567890_0123#123", mockContext);

        Assert.assertNotNull(actual);
        Assert.assertEquals(actual.getPhysicalResourceId(), "application_1234567890_0123#123");
        Mockito.verify(mockLivyFactory).create(properties.getLivyHost(), properties.getLivyPort());
        Mockito.verify(mockLivyClientSync).killBatch(123);
    }

    private static SparkJobResourceProperties makeSparkJobResourceProperties() {
        SparkJobResourceProperties properties = new SparkJobResourceProperties();
        properties.setLivyHost("Livy Host");
        properties.setLivyPort(12456);
        return properties;
    }

}