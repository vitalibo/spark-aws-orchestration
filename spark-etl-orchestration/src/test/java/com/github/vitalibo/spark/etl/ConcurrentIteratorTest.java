package com.github.vitalibo.spark.etl;

import com.amazonaws.services.stepfunctions.AWSStepFunctions;
import com.amazonaws.services.stepfunctions.model.GetActivityTaskRequest;
import com.amazonaws.services.stepfunctions.model.GetActivityTaskResult;
import com.github.vitalibo.spark.etl.ActivityDispatcher.ConcurrentIterator;
import com.github.vitalibo.spark.etl.ActivityDispatcher.ConcurrentIterator.ActivityFetcher;
import com.github.vitalibo.spark.etl.model.Activity;
import org.mockito.*;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class ConcurrentIteratorTest {

    @Mock
    private AWSStepFunctions mockAWSStepFunctions;
    @Mock
    private CompletionService<Activity> mockCompletionService;
    @Captor
    private ArgumentCaptor<GetActivityTaskRequest> captorGetActivityTaskRequest;
    @Mock
    private Future<Activity> mockFuture;

    private ConcurrentIterator iterator;

    @BeforeMethod
    public void setUp() {
        MockitoAnnotations.initMocks(this);
        iterator = new ConcurrentIterator(mockCompletionService, mockAWSStepFunctions);
    }

    @Test
    public void testFetcherInit() {
        iterator.init(Arrays.asList("foo", "bar"));

        Mockito.verify(mockCompletionService, Mockito.times(2)).submit(Mockito.any());
    }

    @Test
    public void testHasNext() {
        boolean actual = iterator.hasNext();

        Assert.assertTrue(actual);
    }

    @Test
    public void testNext() throws InterruptedException, ExecutionException {
        Mockito.when(mockCompletionService.take()).thenReturn(mockFuture);
        Activity activity = new Activity().withArn("foo");
        Mockito.when(mockFuture.get()).thenReturn(activity);

        Activity actual = iterator.next();

        Assert.assertNotNull(actual);
        Assert.assertEquals(actual, activity);
    }

    @Test
    public void testFetch() {
        Mockito.when(mockAWSStepFunctions.getActivityTask(Mockito.any()))
            .thenReturn(new GetActivityTaskResult())
            .thenReturn(new GetActivityTaskResult()
                .withInput("json").withTaskToken("token"));
        ActivityFetcher fetcher = iterator.new ActivityFetcher("foo");

        Activity actual = fetcher.call();

        Assert.assertNotNull(actual);
        Assert.assertEquals(actual.getTaskToken(), "token");
        Assert.assertEquals(actual.getJsonInput(), "json");
        Assert.assertEquals(actual.getArn(), "foo");
        Mockito.verify(mockAWSStepFunctions, Mockito.times(2)).getActivityTask(captorGetActivityTaskRequest.capture());
        GetActivityTaskRequest request = captorGetActivityTaskRequest.getValue();
        Assert.assertEquals(request.getActivityArn(), "foo");
    }

}