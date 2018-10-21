package com.github.vitalibo.spark.cfn.facade;

import com.amazonaws.services.lambda.runtime.Context;
import com.github.vitalibo.cfn.resource.util.Rules;
import com.github.vitalibo.spark.cfn.LivyClientSync;
import com.github.vitalibo.spark.cfn.model.SparkJobResourceData;
import com.github.vitalibo.spark.cfn.model.SparkJobResourceProperties;
import com.github.vitalibo.spark.emr.model.Batch;
import com.github.vitalibo.spark.emr.model.CreateBatchRequest;
import com.github.vitalibo.spark.emr.model.GetBatchResponse;
import org.mockito.*;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class SparkJobCreateFacadeTest {

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

    private SparkJobCreateFacade facade;

    @BeforeMethod
    public void setUp() {
        MockitoAnnotations.initMocks(this);
        facade = new SparkJobCreateFacade(mockRules, mockLivyFactory);
    }

    @Test
    public void testCreate() {
        Mockito.when(mockLivyFactory.create(Mockito.anyString(), Mockito.anyInt()))
            .thenReturn(mockLivyClientSync);
        Mockito.when(mockLivyClientSync.createBatchSync(Mockito.any(CreateBatchRequest.class), Mockito.anyLong()))
            .thenReturn(makeGetBatchResponse(makeBatch("application_1234567890_0123", 123)));
        Mockito.when(mockContext.getRemainingTimeInMillis())
            .thenReturn(9876);
        SparkJobResourceProperties properties = makeSparkJobResourceProperties();

        SparkJobResourceData actual = facade.create(properties, mockContext);

        Assert.assertNotNull(actual);
        Assert.assertEquals(actual.getPhysicalResourceId(), "application_1234567890_0123#123");
        Assert.assertEquals(actual.getApplicationId(), "application_1234567890_0123");
        Assert.assertEquals(actual.getBatchId(), (Integer) 123);
        Mockito.verify(mockLivyFactory).create(properties.getLivyHost(), properties.getLivyPort());
        Mockito.verify(mockLivyClientSync).createBatchSync(captorCreateBatchRequest.capture(), Mockito.eq(9876L));
        CreateBatchRequest request = captorCreateBatchRequest.getValue();
        Assert.assertEquals(request.getName(), properties.getName());
    }

    @Test
    public void testDelegateRules() {
        SparkJobResourceProperties properties = makeSparkJobResourceProperties();

        facade.forEach(properties);

        Mockito.verify(mockRules).forEach(properties);
    }

    private static SparkJobResourceProperties makeSparkJobResourceProperties() {
        SparkJobResourceProperties properties = new SparkJobResourceProperties();
        properties.setLivyHost("Livy Host");
        properties.setLivyPort(12456);
        properties.setName("Application Name");
        return properties;
    }

    private static GetBatchResponse makeGetBatchResponse(Batch batch) {
        return new GetBatchResponse()
            .withBatch(batch);
    }

    private static Batch makeBatch(String applicationId, Integer batchId) {
        return new Batch()
            .withApplicationId(applicationId)
            .withBatchId(batchId);
    }

}