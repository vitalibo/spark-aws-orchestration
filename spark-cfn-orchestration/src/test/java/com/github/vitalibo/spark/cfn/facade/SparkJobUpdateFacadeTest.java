package com.github.vitalibo.spark.cfn.facade;

import com.amazonaws.services.lambda.runtime.Context;
import com.github.vitalibo.cfn.resource.util.Rules;
import com.github.vitalibo.spark.cfn.LivyClientSync;
import com.github.vitalibo.spark.cfn.model.SparkJobResourceData;
import com.github.vitalibo.spark.cfn.model.SparkJobResourceProperties;
import com.github.vitalibo.spark.emr.model.Batch;
import com.github.vitalibo.spark.emr.model.BatchState;
import com.github.vitalibo.spark.emr.model.CreateBatchRequest;
import com.github.vitalibo.spark.emr.model.GetBatchResponse;
import org.mockito.*;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class SparkJobUpdateFacadeTest {

    @Mock
    private Rules<SparkJobResourceProperties> mockRules;
    @Mock
    private LivyClientSync.Factory mockLivyFactory;
    @Mock
    private LivyClientSync mockLivyClientSync;
    @Mock
    private Context mockContext;
    @Captor
    private ArgumentCaptor<CreateBatchRequest> captorCreateBatchRequest;
    private SparkJobUpdateFacade spyFacade;

    @BeforeMethod
    public void setUp() {
        MockitoAnnotations.initMocks(this);
        spyFacade = Mockito.spy(new SparkJobUpdateFacade(mockRules, mockLivyFactory));
    }

    @Test
    public void testUpdate() {
        Mockito.when(mockLivyFactory.create(Mockito.anyString(), Mockito.anyInt()))
            .thenReturn(mockLivyClientSync);
        Mockito.when(mockLivyClientSync.createBatchSync(Mockito.any(CreateBatchRequest.class), Mockito.anyLong()))
            .thenReturn(makeGetBatchResponse(
                makeBatch("application_1234567890_0123", 123, BatchState.SUCCESS)));
        Mockito.when(mockContext.getRemainingTimeInMillis())
            .thenReturn(9876);
        SparkJobResourceProperties properties =
            makeSparkJobResourceProperties("host", 1234, "app#1");
        SparkJobResourceProperties oldProperties =
            makeSparkJobResourceProperties("old host", 2345, "app#2");

        SparkJobResourceData actual = spyFacade.update(
            properties, oldProperties, "application_1234567890_0123#123", mockContext);

        Assert.assertNotNull(actual);
        Assert.assertEquals(actual.getPhysicalResourceId(), "application_1234567890_0123#123");
        Assert.assertEquals(actual.getApplicationId(), "application_1234567890_0123");
        Assert.assertEquals(actual.getBatchId(), (Integer) 123);
        Mockito.verify(mockLivyFactory).create(properties.getLivyHost(), properties.getLivyPort());
        Mockito.verify(mockLivyClientSync).createBatchSync(captorCreateBatchRequest.capture(), Mockito.eq(9876L));
        CreateBatchRequest request = captorCreateBatchRequest.getValue();
        Assert.assertEquals(request.getName(), properties.getParameters().getName());
        Mockito.verify(spyFacade, Mockito.never()).deleteOld(Mockito.any(), Mockito.anyString());
    }

    @Test
    public void testUpdateWithDeleteOldApplication() {
        Mockito.when(mockLivyFactory.create(Mockito.anyString(), Mockito.anyInt()))
            .thenReturn(mockLivyClientSync);
        Mockito.when(mockLivyClientSync.createBatchSync(Mockito.any(CreateBatchRequest.class), Mockito.anyLong()))
            .thenReturn(makeGetBatchResponse(
                makeBatch("application_1234567890_0123", 123, BatchState.RUNNING)));
        Mockito.when(mockContext.getRemainingTimeInMillis())
            .thenReturn(9876);
        SparkJobResourceProperties properties =
            makeSparkJobResourceProperties("host", 1234, "app#1");
        SparkJobResourceProperties oldProperties =
            makeSparkJobResourceProperties("old host", 2345, "app#2");

        SparkJobResourceData actual = spyFacade.update(
            properties, oldProperties, "application_1234567890_0123#123", mockContext);

        Assert.assertNotNull(actual);
        Assert.assertEquals(actual.getPhysicalResourceId(), "application_1234567890_0123#123");
        Assert.assertEquals(actual.getApplicationId(), "application_1234567890_0123");
        Assert.assertEquals(actual.getBatchId(), (Integer) 123);
        Mockito.verify(mockLivyFactory).create(properties.getLivyHost(), properties.getLivyPort());
        Mockito.verify(mockLivyClientSync).createBatchSync(captorCreateBatchRequest.capture(), Mockito.eq(9876L));
        CreateBatchRequest request = captorCreateBatchRequest.getValue();
        Assert.assertEquals(request.getName(), properties.getParameters().getName());
        Mockito.verify(spyFacade).deleteOld(oldProperties, "application_1234567890_0123#123");
    }

    @Test
    public void testDeleteOld() {
        Mockito.when(mockLivyFactory.create(Mockito.anyString(), Mockito.anyInt()))
            .thenReturn(mockLivyClientSync);
        SparkJobResourceProperties oldProperties =
            makeSparkJobResourceProperties("old host", 2345, "app#2");

        spyFacade.deleteOld(oldProperties, "application_1234567890_0123#123");

        Mockito.verify(mockLivyFactory).create(oldProperties.getLivyHost(), oldProperties.getLivyPort());
        Mockito.verify(mockLivyClientSync).killBatch(123);
    }

    @Test
    public void testDelegateRules() {
        SparkJobResourceProperties properties = makeSparkJobResourceProperties("host", 1234, "app#1");

        spyFacade.forEach(properties);

        Mockito.verify(mockRules).forEach(properties);
    }

    private static SparkJobResourceProperties makeSparkJobResourceProperties(String host, Integer port, String name) {
        SparkJobResourceProperties properties = new SparkJobResourceProperties();
        properties.setLivyHost(host);
        properties.setLivyPort(port);
        SparkJobResourceProperties.Parameters parameters = new SparkJobResourceProperties.Parameters();
        parameters.setName(name);
        properties.setParameters(parameters);
        return properties;
    }

    private static GetBatchResponse makeGetBatchResponse(Batch batch) {
        return new GetBatchResponse()
            .withBatch(batch);
    }

    private static Batch makeBatch(String applicationId, Integer batchId, BatchState state) {
        return new Batch()
            .withApplicationId(applicationId)
            .withBatchId(batchId)
            .withState(state);
    }

}