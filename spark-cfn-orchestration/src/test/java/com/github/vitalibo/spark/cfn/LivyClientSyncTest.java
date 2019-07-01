package com.github.vitalibo.spark.cfn;

import com.github.vitalibo.cfn.resource.ResourceProvisionException;
import com.github.vitalibo.spark.emr.LivyClient;
import com.github.vitalibo.spark.emr.model.*;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.stubbing.OngoingStubbing;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.function.Supplier;

public class LivyClientSyncTest {

    @Mock
    private LivyClient mockLivyClient;
    @Mock
    private Supplier<Long> mockNow;

    private LivyClientSync spyLivyClientSync;

    @BeforeMethod
    public void setUp() {
        MockitoAnnotations.initMocks(this);
        spyLivyClientSync = Mockito.spy(new LivyClientSync(mockLivyClient, mockNow));
        Mockito.when(mockNow.get()).thenReturn(1000L);
    }

    @Test
    public void testSuccessStartApplication() {
        Mockito.when(mockLivyClient.createBatch(Mockito.any(CreateBatchRequest.class)))
            .thenReturn(makeCreateBatchResponse(makeBatch(12)));
        CreateBatchRequest request = makeCreateBatchRequest("app#1");
        Mockito.doReturn(BatchState.RUNNING)
            .when(spyLivyClientSync).waitRunning(Mockito.anyInt(), Mockito.anyLong());
        GetBatchResponse response = makeGetBatchResponse(makeBatch(12));
        Mockito.when(mockLivyClient.getBatch(12))
            .thenReturn(response);

        GetBatchResponse actual = spyLivyClientSync.createBatchSync(request, 1);

        Assert.assertNotNull(actual);
        Assert.assertEquals(actual, response);
        Mockito.verify(mockLivyClient).createBatch(request);
        Mockito.verify(spyLivyClientSync).waitRunning(12, 1001L);
        Mockito.verify(mockLivyClient).getBatch(12);
    }

    @Test
    public void testApplicationNotStartedWithin5Minutes() {
        Mockito.when(mockLivyClient.createBatch(Mockito.any(CreateBatchRequest.class)))
            .thenReturn(makeCreateBatchResponse(makeBatch(12)));
        CreateBatchRequest request = makeCreateBatchRequest("app#1");
        Mockito.doReturn(BatchState.STARTING)
            .when(spyLivyClientSync).waitRunning(Mockito.anyInt(), Mockito.anyLong());

        ResourceProvisionException actual = Assert.expectThrows(ResourceProvisionException.class, () ->
            spyLivyClientSync.createBatchSync(request, 1));

        Assert.assertTrue(actual.getMessage().matches(".*within 5 minutes.*"));
        Mockito.verify(mockLivyClient).createBatch(request);
        Mockito.verify(spyLivyClientSync).waitRunning(Mockito.eq(12), Mockito.anyLong());
    }

    @Test
    public void testFailedStartApplication() {
        Mockito.when(mockLivyClient.createBatch(Mockito.any(CreateBatchRequest.class)))
            .thenReturn(makeCreateBatchResponse(makeBatch(12)));
        CreateBatchRequest request = makeCreateBatchRequest("app#1");
        Mockito.doReturn(BatchState.DEAD)
            .when(spyLivyClientSync).waitRunning(Mockito.anyInt(), Mockito.anyLong());

        ResourceProvisionException actual = Assert.expectThrows(ResourceProvisionException.class, () ->
            spyLivyClientSync.createBatchSync(request, 1));

        Assert.assertTrue(actual.getMessage().matches(".*starting failed.*"));
        Mockito.verify(mockLivyClient).createBatch(request);
        Mockito.verify(spyLivyClientSync).waitRunning(Mockito.eq(12), Mockito.anyLong());
    }

    @Test
    public void testWaitRunning() {
        Mockito.when(mockLivyClient.getBatchState(Mockito.anyInt()))
            .thenReturn(makeGetBatchStateResponse(BatchState.STARTING))
            .thenReturn(makeGetBatchStateResponse(BatchState.STARTING))
            .thenReturn(makeGetBatchStateResponse(BatchState.RUNNING));
        OngoingStubbing<Long> when = Mockito.when(mockNow.get());
        for (int i = 0; i < 10; i++) {
            when = when.thenReturn(i * 1000L);
        }

        BatchState actual = spyLivyClientSync.waitRunning(123, 10000);

        Assert.assertNotNull(actual);
        Assert.assertEquals(actual, BatchState.RUNNING);
        Mockito.verify(mockLivyClient, Mockito.times(4)).getBatchState(123);
        Mockito.verify(mockNow, Mockito.times(2)).get();
    }

    @Test
    public void testFailWaitByTimeout() {
        Mockito.when(mockLivyClient.getBatchState(Mockito.anyInt()))
            .thenReturn(makeGetBatchStateResponse(BatchState.STARTING));
        OngoingStubbing<Long> when = Mockito.when(mockNow.get());
        for (int i = 0; i < 10; i++) {
            when = when.thenReturn(i * 1000L);
        }

        BatchState actual = spyLivyClientSync.waitRunning(123, 7000);

        Assert.assertNotNull(actual);
        Assert.assertEquals(actual, BatchState.STARTING);
        Mockito.verify(mockLivyClient, Mockito.times(4)).getBatchState(123);
        Mockito.verify(mockNow, Mockito.times(3)).get();
    }

    @Test
    public void testFactoryCreateDefaultLivyPort() {
        LivyClientSync.Factory spyFactory = Mockito.spy(LivyClientSync.Factory.class);

        spyFactory.create("host", null);

        Mockito.verify(spyFactory).apply("http://host", 8998);
    }

    @Test
    public void testFactoryCreate() {
        LivyClientSync.Factory spyFactory = Mockito.spy(LivyClientSync.Factory.class);

        spyFactory.create("host", 1234);

        Mockito.verify(spyFactory).apply("http://host", 1234);
    }

    private static CreateBatchRequest makeCreateBatchRequest(String name) {
        CreateBatchRequest request = new CreateBatchRequest();
        request.setName(name);
        return request;
    }

    private static CreateBatchResponse makeCreateBatchResponse(Batch batch) {
        return new CreateBatchResponse()
            .withBatch(batch);
    }

    private static GetBatchResponse makeGetBatchResponse(Batch batch) {
        return new GetBatchResponse()
            .withBatch(batch);
    }

    private static GetBatchStateResponse makeGetBatchStateResponse(BatchState state) {
        return new GetBatchStateResponse()
            .withState(state);
    }

    private static Batch makeBatch(Integer id) {
        return new Batch()
            .withBatchId(id);
    }

}