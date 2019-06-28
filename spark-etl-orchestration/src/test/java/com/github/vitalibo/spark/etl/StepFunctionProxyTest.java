package com.github.vitalibo.spark.etl;

import com.amazonaws.services.stepfunctions.AWSStepFunctions;
import com.amazonaws.services.stepfunctions.model.*;
import org.mockito.*;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class StepFunctionProxyTest {

    @Mock
    private AWSStepFunctions mockAWSStepFunctions;
    @Captor
    private ArgumentCaptor<SendTaskSuccessRequest> captorSendTaskSuccessRequest;
    @Captor
    private ArgumentCaptor<SendTaskHeartbeatRequest> captorSendTaskHeartbeatRequest;
    @Captor
    private ArgumentCaptor<SendTaskFailureRequest> captorSendTaskFailureRequest;

    private StepFunctionProxy proxy;

    @BeforeMethod
    public void setUp() {
        MockitoAnnotations.initMocks(this);
        proxy = new StepFunctionProxy(mockAWSStepFunctions, "foo");
    }

    @Test
    public void testTaskSuccess() {
        proxy.taskSuccess("json");

        Mockito.verify(mockAWSStepFunctions).sendTaskSuccess(captorSendTaskSuccessRequest.capture());
        SendTaskSuccessRequest request = captorSendTaskSuccessRequest.getValue();
        Assert.assertEquals(request.getOutput(), "json");
        Assert.assertEquals(request.getTaskToken(), "foo");
    }

    @Test
    public void testTaskSyncHeartbeat() {
        boolean actual = proxy.taskSyncHeartbeat();

        Assert.assertTrue(actual);
        Mockito.verify(mockAWSStepFunctions).sendTaskHeartbeat(captorSendTaskHeartbeatRequest.capture());
        SendTaskHeartbeatRequest request = captorSendTaskHeartbeatRequest.getValue();
        Assert.assertEquals(request.getTaskToken(), "foo");
    }

    @Test
    public void testTaskSyncHeartbeatWhenActivityAborted() {
        Mockito.when(mockAWSStepFunctions.sendTaskHeartbeat(Mockito.any()))
            .thenThrow(TaskTimedOutException.class);

        boolean actual = proxy.taskSyncHeartbeat();

        Assert.assertFalse(actual);
        Mockito.verify(mockAWSStepFunctions, Mockito.times(3)).sendTaskHeartbeat(Mockito.any());
    }

    @Test
    public void testTaskSyncHeartbeatRetry() {
        Mockito.when(mockAWSStepFunctions.sendTaskHeartbeat(Mockito.any()))
            .thenThrow(TaskTimedOutException.class)
            .thenReturn(new SendTaskHeartbeatResult());

        boolean actual = proxy.taskSyncHeartbeat();

        Assert.assertTrue(actual);
        Mockito.verify(mockAWSStepFunctions, Mockito.times(2)).sendTaskHeartbeat(captorSendTaskHeartbeatRequest.capture());
        SendTaskHeartbeatRequest request = captorSendTaskHeartbeatRequest.getValue();
        Assert.assertEquals(request.getTaskToken(), "foo");
    }

    @Test
    public void testTaskFailure() {
        proxy.taskFailure("bar", "baz");

        Mockito.verify(mockAWSStepFunctions).sendTaskFailure(captorSendTaskFailureRequest.capture());
        SendTaskFailureRequest request = captorSendTaskFailureRequest.getValue();
        Assert.assertEquals(request.getError(), "bar");
        Assert.assertEquals(request.getCause(), "baz");
        Assert.assertEquals(request.getTaskToken(), "foo");
    }

}