package com.github.vitalibo.spark.emr;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.client.ResponseDefinitionBuilder;
import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import com.github.vitalibo.spark.emr.model.*;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import javax.ws.rs.client.ClientBuilder;
import java.util.List;
import java.util.Map;

public class LivyHttpClientTest {

    private WireMockServer wireMockServer;
    private LivyHttpClient livyClient;

    @BeforeClass
    public void setUp() {
        wireMockServer = new WireMockServer(WireMockConfiguration.DYNAMIC_PORT);
        wireMockServer.start();
        WireMock.configureFor("localhost", wireMockServer.port());
        livyClient = new LivyHttpClient(ClientBuilder.newClient(), wireMockServer.baseUrl());
    }

    @BeforeMethod
    public void resetMock() {
        wireMockServer.resetAll();
    }

    @Test
    public void testGetBatches() {
        WireMock.stubFor(WireMock.get(WireMock.anyUrl())
            .willReturn(resourceAsResponse("/GetBatchesResponse.json")));
        GetBatchesRequest request = new GetBatchesRequest()
            .withFrom(12).withSize(34);

        GetBatchesResponse actual = livyClient.getBatches(request);

        Assert.assertNotNull(actual);
        Assert.assertEquals(actual.getFrom(), (Integer) 10);
        Assert.assertEquals(actual.getTotal(), (Integer) 98);
        Assert.assertEquals(actual.getSessions().size(), 1);
        assertBatch(actual.getSessions().get(0));
        WireMock.verify(WireMock.getRequestedFor(WireMock.urlPathEqualTo("/batches"))
            .withQueryParam("from", WireMock.equalTo("12"))
            .withQueryParam("size", WireMock.equalTo("34")));
    }

    @Test
    public void testCreateBatch() {
        WireMock.stubFor(WireMock.post(WireMock.anyUrl())
            .willReturn(resourceAsResponse("/CreateBatchResponse.json")));
        CreateBatchRequest request = new CreateBatchRequest()
            .withName("app#1");

        CreateBatchResponse actual = livyClient.createBatch(request);

        Assert.assertNotNull(actual);
        assertBatch(actual.getBatch());
        WireMock.verify(WireMock.postRequestedFor(WireMock.urlPathEqualTo("/batches"))
            .withRequestBody(WireMock.equalTo("{\"name\":\"app#1\"}")));
    }

    @Test
    public void testGetBatch() {
        WireMock.stubFor(WireMock.get(WireMock.anyUrl())
            .willReturn(resourceAsResponse("/GetBatchResponse.json")));
        GetBatchRequest request = new GetBatchRequest()
            .withBatchId(123);

        GetBatchResponse actual = livyClient.getBatch(request);

        Assert.assertNotNull(actual);
        assertBatch(actual.getBatch());
        WireMock.verify(WireMock.getRequestedFor(WireMock.urlPathEqualTo("/batches/123")));
    }

    @Test
    public void testGetBatchState() {
        WireMock.stubFor(WireMock.get(WireMock.anyUrl())
            .willReturn(resourceAsResponse("/GetBatchStateResponse.json")));
        GetBatchStateRequest request = new GetBatchStateRequest()
            .withBatchId(124);

        GetBatchStateResponse actual = livyClient.getBatchState(request);

        Assert.assertNotNull(actual);
        Assert.assertEquals(actual.getBatchId(), (Integer) 111);
        Assert.assertEquals(actual.getState(), BatchState.STARTING);
        WireMock.verify(WireMock.getRequestedFor(WireMock.urlPathEqualTo("/batches/124/state")));
    }

    @Test
    public void testKillBatch() {
        WireMock.stubFor(WireMock.delete(WireMock.anyUrl())
            .willReturn(resourceAsResponse("/KillBatchResponse.json")));
        KillBatchRequest request = new KillBatchRequest()
            .withBatchId(124);

        KillBatchResponse actual = livyClient.killBatch(request);

        Assert.assertNotNull(actual);
        WireMock.verify(WireMock.deleteRequestedFor(WireMock.urlPathEqualTo("/batches/124")));
    }

    @Test
    public void testGetBatchLog() {
        WireMock.stubFor(WireMock.get(WireMock.anyUrl())
            .willReturn(resourceAsResponse("/GetBatchLogResponse.json")));
        GetBatchLogRequest request = new GetBatchLogRequest()
            .withBatchId(124).withFrom(12).withSize(23);

        GetBatchLogResponse actual = livyClient.getBatchLog(request);

        Assert.assertNotNull(actual);
        Assert.assertEquals(actual.getBatchId(), (Integer) 111);
        Assert.assertEquals(actual.getFrom(), (Integer) 0);
        Assert.assertEquals(actual.getTotal(), (Integer) 5);
        Assert.assertEquals(actual.getLog().size(), 5);
        WireMock.verify(WireMock.getRequestedFor(WireMock.urlPathEqualTo("/batches/124/log")));
    }

    @AfterClass
    public void cleanUp() {
        wireMockServer.stop();
    }

    private static void assertBatch(Batch actual) {
        Assert.assertEquals(actual.getId(), (Integer) 111);
        Assert.assertEquals(actual.getApplicationId(), "application_1540212897167_0054");
        Assert.assertEquals(actual.getState(), BatchState.RUNNING);
        Map<String, String> appInfo = actual.getApplicationInfo();
        Assert.assertTrue(appInfo.containsKey("driverLogUrl"));
        Assert.assertTrue(appInfo.containsKey("sparkUiUrl"));
        List<String> log = actual.getLog();
        Assert.assertEquals(log.size(), 5);
    }

    private static ResponseDefinitionBuilder resourceAsResponse(String resource) {
        return WireMock.aResponse()
            .withHeader("Content-Type", "application/json")
            .withBody(TestHelper.resourceAsString(resource));
    }

}