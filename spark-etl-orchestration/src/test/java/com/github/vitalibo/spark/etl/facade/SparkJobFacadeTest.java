package com.github.vitalibo.spark.etl.facade;

import com.github.vitalibo.spark.emr.LivyClient;
import com.github.vitalibo.spark.emr.ValidationException;
import com.github.vitalibo.spark.emr.model.*;
import com.github.vitalibo.spark.etl.Factory;
import com.github.vitalibo.spark.etl.StepFunctionProxy;
import com.github.vitalibo.spark.etl.model.ActivityException;
import com.github.vitalibo.spark.etl.model.SparkActivityInput;
import com.github.vitalibo.spark.etl.model.SparkActivityInput.Properties;
import com.github.vitalibo.spark.etl.model.SparkActivityOutput;
import com.github.vitalibo.spark.etl.util.Rules;
import org.mockito.*;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static com.github.vitalibo.spark.emr.model.BatchState.*;

public class SparkJobFacadeTest {

    @Mock
    private Rules<SparkActivityInput> mockRules;
    @Mock
    private Factory.LivyClient mockFactoryLivyClient;
    @Mock
    private LivyClient mockLivyClient;
    @Mock
    private StepFunctionProxy mockStepFunctionProxy;
    @Mock
    private CreateBatchResponse mockCreateBatchResponse;
    @Mock
    private GetBatchStateResponse mockGetBatchStateResponse;
    @Mock
    private GetBatchResponse mockGetBatchResponse;
    @Captor
    private ArgumentCaptor<CreateBatchRequest> captorCreateBatchRequest;

    private SparkJobFacade facade;

    @BeforeMethod
    public void setUp() {
        MockitoAnnotations.initMocks(this);
        facade = new SparkJobFacade(mockRules, mockFactoryLivyClient, 100);
        Mockito.when(mockFactoryLivyClient.create(Mockito.anyString(), Mockito.anyInt()))
            .thenReturn(mockLivyClient);
    }

    @Test
    public void testFailRules() {
        Mockito.doThrow(ValidationException.class)
            .when(mockRules).verify(Mockito.any());

        Assert.assertThrows(ValidationException.class,
            () -> facade.process(mockStepFunctionProxy, makeSparkActivityInput()));
        Mockito.verify(mockFactoryLivyClient, Mockito.never())
            .create(Mockito.anyString(), Mockito.anyInt());
    }

    @Test
    public void testProcess() {
        Mockito.when(mockLivyClient.createBatch(Mockito.any()))
            .thenReturn(mockCreateBatchResponse);
        Mockito.when(mockCreateBatchResponse.getBatch())
            .thenReturn(new Batch()
                .withBatchId(1).withState(STARTING));
        Mockito.when(mockStepFunctionProxy.taskSyncHeartbeat())
            .thenReturn(true);
        Mockito.when(mockLivyClient.getBatchState(Mockito.anyInt()))
            .thenReturn(mockGetBatchStateResponse);
        Mockito.when(mockGetBatchStateResponse.getState())
            .thenReturn(STARTING, RUNNING, RUNNING, SUCCESS);
        Mockito.when(mockLivyClient.getBatch(Mockito.anyInt()))
            .thenReturn(mockGetBatchResponse);
        Mockito.when(mockGetBatchResponse.getBatch())
            .thenReturn(new Batch()
                .withBatchId(1).withState(SUCCESS).withApplicationId("app_1"));

        SparkActivityOutput actual = facade.process(mockStepFunctionProxy, makeSparkActivityInput());

        Assert.assertNotNull(actual);
        Assert.assertEquals(actual.getBatchId(), (Integer) 1);
        Assert.assertEquals(actual.getApplicationId(), "app_1");
        Mockito.verify(mockFactoryLivyClient).create("livy-host", 8998);
        Mockito.verify(mockLivyClient).createBatch(captorCreateBatchRequest.capture());
        CreateBatchRequest createBatchRequest = captorCreateBatchRequest.getValue();
        Assert.assertEquals(createBatchRequest.getName(), "SparkJobName");
        Mockito.verify(mockStepFunctionProxy, Mockito.times(4)).taskSyncHeartbeat();
        Mockito.verify(mockLivyClient, Mockito.never()).killBatch(Mockito.anyInt());
        Mockito.verify(mockLivyClient, Mockito.times(4)).getBatchState(1);
        Mockito.verify(mockLivyClient).getBatch(1);
    }

    @Test
    public void testProcessJobFailure() {
        Mockito.when(mockLivyClient.createBatch(Mockito.any()))
            .thenReturn(mockCreateBatchResponse);
        Mockito.when(mockCreateBatchResponse.getBatch())
            .thenReturn(new Batch()
                .withBatchId(1).withState(STARTING));
        Mockito.when(mockStepFunctionProxy.taskSyncHeartbeat())
            .thenReturn(true);
        Mockito.when(mockLivyClient.getBatchState(Mockito.anyInt()))
            .thenReturn(mockGetBatchStateResponse);
        Mockito.when(mockGetBatchStateResponse.getState())
            .thenReturn(STARTING, RUNNING, RUNNING, DEAD);
        Mockito.when(mockLivyClient.getBatch(Mockito.anyInt()))
            .thenReturn(mockGetBatchResponse);
        Mockito.when(mockGetBatchResponse.getBatch())
            .thenReturn(new Batch()
                .withBatchId(1).withState(SUCCESS).withApplicationId("app_1"));

        ActivityException actual = Assert.expectThrows(ActivityException.class,
            () -> facade.process(mockStepFunctionProxy, makeSparkActivityInput()));

        Assert.assertNotNull(actual);
        Assert.assertEquals(actual.getMessage(), "Application app_1 failed (BatchId: 1)");
        Mockito.verify(mockFactoryLivyClient).create("livy-host", 8998);
        Mockito.verify(mockLivyClient).createBatch(captorCreateBatchRequest.capture());
        CreateBatchRequest createBatchRequest = captorCreateBatchRequest.getValue();
        Assert.assertEquals(createBatchRequest.getName(), "SparkJobName");
        Mockito.verify(mockStepFunctionProxy, Mockito.times(4)).taskSyncHeartbeat();
        Mockito.verify(mockLivyClient, Mockito.never()).killBatch(Mockito.anyInt());
        Mockito.verify(mockLivyClient, Mockito.times(4)).getBatchState(1);
        Mockito.verify(mockLivyClient).getBatch(1);
    }

    @Test
    public void testProcessActivityAborted() {
        Mockito.when(mockLivyClient.createBatch(Mockito.any()))
            .thenReturn(mockCreateBatchResponse);
        Mockito.when(mockCreateBatchResponse.getBatch())
            .thenReturn(new Batch()
                .withBatchId(1).withState(STARTING));
        Mockito.when(mockStepFunctionProxy.taskSyncHeartbeat())
            .thenReturn(true, true, false);
        Mockito.when(mockLivyClient.getBatchState(Mockito.anyInt()))
            .thenReturn(mockGetBatchStateResponse);
        Mockito.when(mockGetBatchStateResponse.getState())
            .thenReturn(STARTING, RUNNING, RUNNING, DEAD);
        Mockito.when(mockLivyClient.getBatch(Mockito.anyInt()))
            .thenReturn(mockGetBatchResponse);
        Mockito.when(mockGetBatchResponse.getBatch())
            .thenReturn(new Batch()
                .withBatchId(1).withState(SUCCESS).withApplicationId("app_1"));

        ActivityException actual = Assert.expectThrows(ActivityException.class,
            () -> facade.process(mockStepFunctionProxy, makeSparkActivityInput()));

        Assert.assertNotNull(actual);
        Assert.assertEquals(actual.getMessage(), "Application was killed");
        Mockito.verify(mockFactoryLivyClient).create("livy-host", 8998);
        Mockito.verify(mockLivyClient).createBatch(captorCreateBatchRequest.capture());
        CreateBatchRequest createBatchRequest = captorCreateBatchRequest.getValue();
        Assert.assertEquals(createBatchRequest.getName(), "SparkJobName");
        Mockito.verify(mockStepFunctionProxy, Mockito.times(3)).taskSyncHeartbeat();
        Mockito.verify(mockLivyClient).killBatch(Mockito.anyInt());
        Mockito.verify(mockLivyClient, Mockito.times(2)).getBatchState(1);
        Mockito.verify(mockLivyClient, Mockito.never()).getBatch(Mockito.anyInt());
    }

    private static SparkActivityInput makeSparkActivityInput() {
        SparkActivityInput input = new SparkActivityInput();
        input.setLivyHost("livy-host");
        input.setLivyPort(8998);
        Properties properties = new Properties();
        properties.setName("SparkJobName");
        input.setProperties(properties);
        return input;
    }

}