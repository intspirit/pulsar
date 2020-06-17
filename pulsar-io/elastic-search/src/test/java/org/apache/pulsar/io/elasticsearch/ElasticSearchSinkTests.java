/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pulsar.io.elasticsearch;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.*;

import com.google.common.collect.Maps;
import org.apache.http.HttpHost;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.core.SinkContext;
import org.apache.pulsar.io.elasticsearch.data.Profile;
import org.apache.pulsar.io.elasticsearch.data.UserProfile;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import net.andreinc.mockneat.MockNeat;

import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.action.search.SearchResponse;

public class ElasticSearchSinkTests {

    protected static MockNeat mockNeat;
    protected static Gson gson;
    protected static RestHighLevelClient client;

    @Mock
    protected Record<byte[]> mockRecord;

    @Mock
    protected SinkContext mockSinkContext;
    protected Map<String, Object> map;
    protected ElasticSearchSink sink;

    @BeforeClass
    public static final void init() {
        mockNeat = MockNeat.threadLocal();
        gson = new GsonBuilder()
                .setPrettyPrinting()
                .create();
    }

    @SuppressWarnings("unchecked")
    @BeforeMethod
    public final void setUp() throws Exception {
        map = new HashMap<String, Object> ();
        map.put("elasticSearchUrl", "http://localhost:9200");
        sink = new ElasticSearchSink();
        client = new RestHighLevelClient(RestClient.builder(new HttpHost("localhost", 9200, "http")));

        mockRecord = mock(Record.class);
        mockSinkContext = mock(SinkContext.class);

        when(mockRecord.getKey()).thenAnswer(new Answer<Optional<String>>() {
            long sequenceCounter = 0;
            public Optional<String> answer(InvocationOnMock invocation) throws Throwable {
                return Optional.of( "key-" + sequenceCounter++);
            }});

        when(mockRecord.getValue()).thenAnswer(new Answer<byte[]>() {
            public byte[] answer(InvocationOnMock invocation) throws Throwable {
                return getJSON().getBytes();
            }});
    }

    @AfterMethod
    public final void tearDown() throws Exception {
        sink.close();
        client.close();
    }

    @Test(enabled = true, expectedExceptions = ElasticsearchStatusException.class)
    public final void invalidIndexNameTest() throws Exception {
        map.put("indexName", "myIndex");
        sink.open(map, mockSinkContext);
    }

    @Test(enabled = true)
    public final void createIndexTest() throws Exception {
        map.put("indexName", "test-index");
        sink.open(map, mockSinkContext);
//        QueryBuilders.matchAllQuery()
    }

    @Test(enabled = true)
    public final void CreateSingleRecordTest() throws Exception {
        map.put("indexName", "test-index");
        sink.open(map, mockSinkContext);
        send(1);
        verify(mockRecord, times(1)).ack();
    }

    @Test(enabled = true)
    public final void UpdateRecordTest() throws Exception {
        map.put("indexName", "test-index");
        sink.open(map, mockSinkContext);

        //First let's fetch some record from elastic
        Map<String, Object> searchResult = doSearch();
        String id = (String) searchResult.get("id");
        //Then let's try to update the record
        doUpdate(id);
        Map<String, Object> updatedResult = doGetById(id);

        Assert.assertEquals((String) searchResult.get("id"), (String) updatedResult.get("id"));
        Assert.assertNotEquals((String) searchResult.get("userName"), (String) updatedResult.get("userName"));
    }

    @Test(enabled = true)
    public final void DeleteRecordTest() throws Exception {
        map.put("indexName", "test-index");
        sink.open(map, mockSinkContext);

        //First let's fetch some record from elastic
        Map<String, Object> searchResult = doSearch();
        String id = (String) searchResult.get("id");
        //Then let's try to delete the record
        doDelete(id);

        Map<String, Object> updatedResult = doGetById(id);

        Assert.assertEquals(updatedResult, null);
    }

    @Test(enabled = true)
    public final void send100Test() throws Exception {
        map.put("indexName", "test-index");
        sink.open(map, mockSinkContext);
        send(100);
        verify(mockRecord, times(100)).ack();
    }

    protected void doDelete(String id) throws Exception{
        Map<String, String> updateProperties = Maps.newHashMap();
        updateProperties.put("ACTION", "DELETE");
        updateProperties.put("ID", id);
        when(mockRecord.getProperties()).thenReturn(updateProperties);
        send(1);
    }

    protected void doUpdate(String id) throws Exception{
        Map<String, String> updateProperties = Maps.newHashMap();
        updateProperties.put("ACTION", "UPSERT");
        updateProperties.put("ID", id);
        when(mockRecord.getProperties()).thenReturn(updateProperties);
        send(1);
    }

    protected Map <String, Object> doGetById(String id) throws Exception{
        Map <String, Object> resultAsMap = null;
        GetRequest getRequest = new GetRequest();
        getRequest.index("test-index");
        getRequest.id(id);
        GetResponse updatedRes = client.get(getRequest);

        if(updatedRes.isExists()){
            resultAsMap = updatedRes.getSourceAsMap();
            resultAsMap.put("id", id);
        }
        return resultAsMap;
    }

    protected Map<String, Object> doSearch() throws Exception{
        SearchRequest searchRequest = new SearchRequest("test-index");
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchAllQuery());
        searchSourceBuilder.size(1);
        searchRequest.source(searchSourceBuilder);
        SearchResponse res = client.search(searchRequest);

        SearchHit hit = res.getHits().getAt(0);
        Map<String, Object> sourceAsMap = hit.getSourceAsMap();
        sourceAsMap.put("id", hit.getId());
        return sourceAsMap;
    }

    protected final void send(int numRecords) throws Exception {
        for (int idx = 0; idx < numRecords; idx++) {
            sink.write(mockRecord);
        }
    }

    private static String getJSON() {
        return mockNeat
                .reflect(UserProfile.class)
                .field("name", mockNeat.names().full())
                .field("userName", mockNeat.users())
                .field("email", mockNeat.emails())
                .field("profiles",
                        mockNeat.reflect(Profile.class)
                                .field("profileId", mockNeat.ints().range(100, 1000))
                                .field("profileAdded", mockNeat.localDates().toUtilDate())
                                .list(2))
                .map(gson::toJson)
                .val();
    }
}
