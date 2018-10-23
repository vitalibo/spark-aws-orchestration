package com.github.vitalibo.spark.emr;

import com.github.vitalibo.spark.emr.model.*;
import org.mockito.*;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class LivyClientTest {

    @Mock
    private GetBatchResponse mockGetBatchResponse;
    @Captor
    private ArgumentCaptor<GetBatchRequest> captorGetBatchRequest;
    @Mock
    private GetBatchStateResponse mockGetBatchStateResponse;
    @Captor
    private ArgumentCaptor<GetBatchStateRequest> captorGetBatchStateRequest;
    @Mock
    private KillBatchResponse mockKillBatchResponse;
    @Captor
    private ArgumentCaptor<KillBatchRequest> captorKillBatchRequest;

    @Spy
    private LivyClient spyLivyClient;

    @BeforeMethod
    public void setUp() {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testGetBatch() {
        Mockito.doReturn(mockGetBatchResponse)
            .when(spyLivyClient).getBatch(Mockito.any(GetBatchRequest.class));

        GetBatchResponse actual = spyLivyClient.getBatch(123);

        Assert.assertNotNull(actual);
        Assert.assertEquals(actual, mockGetBatchResponse);
        Mockito.verify(spyLivyClient).getBatch(captorGetBatchRequest.capture());
        GetBatchRequest request = captorGetBatchRequest.getValue();
        Assert.assertEquals(request.getBatchId(), (Integer) 123);
    }

    @Test
    public void testGetBatchState() {
        Mockito.doReturn(mockGetBatchStateResponse)
            .when(spyLivyClient).getBatchState(Mockito.any(GetBatchStateRequest.class));

        GetBatchStateResponse actual = spyLivyClient.getBatchState(123);

        Assert.assertNotNull(actual);
        Assert.assertEquals(actual, mockGetBatchStateResponse);
        Mockito.verify(spyLivyClient).getBatchState(captorGetBatchStateRequest.capture());
        GetBatchStateRequest request = captorGetBatchStateRequest.getValue();
        Assert.assertEquals(request.getBatchId(), (Integer) 123);
    }

    @Test
    public void testKillBatch() {
        Mockito.doReturn(mockKillBatchResponse)
            .when(spyLivyClient).killBatch(Mockito.any(KillBatchRequest.class));

        KillBatchResponse actual = spyLivyClient.killBatch(123);

        Assert.assertNotNull(actual);
        Assert.assertEquals(actual, mockKillBatchResponse);
        Mockito.verify(spyLivyClient).killBatch(captorKillBatchRequest.capture());
        KillBatchRequest request = captorKillBatchRequest.getValue();
        Assert.assertEquals(request.getBatchId(), (Integer) 123);
    }

}