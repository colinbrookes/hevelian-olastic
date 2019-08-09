package com.hevelian.olastic.core.processors;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.*;

import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.apache.olingo.commons.api.edmx.EdmxReference;
import org.apache.olingo.server.api.OData;
import org.apache.olingo.server.api.ServiceMetadata;
import org.apache.olingo.server.api.uri.UriInfo;
import org.apache.olingo.server.core.uri.parser.Parser;
import org.apache.olingo.server.core.uri.parser.UriParserException;
import org.apache.olingo.server.core.uri.validator.UriValidationException;
import org.elasticsearch.action.ListenableActionFuture;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.Before;

import com.hevelian.olastic.core.ElasticOData;
import com.hevelian.olastic.core.ElasticServiceMetadata;
import com.hevelian.olastic.core.elastic.mappings.MappingMetaDataProvider;
import com.hevelian.olastic.core.stub.TestProvider;

/**
 * Base class for processors tests.
 */
public abstract class BaseProcessorTest {
    protected String defaultRawODataPath = "/book('13')/character('113')/book/author";
    protected String defaultRawQueryPath = "$count=true&$top=2&$skip=10&$orderby=age,_id desc&$filter=age gt 30";
    protected UriInfo defaultUriInfo;
    protected ElasticServiceMetadata defaultMetadata;
    protected ElasticOData defaultOData;
    protected Client defaultClient;

    @Before
    public void baseSetUp() throws UriParserException, UriValidationException {
        defaultOData = ElasticOData.newInstance();
        defaultMetadata = defaultOData.createServiceMetadata(
                new TestProvider(mock(MappingMetaDataProvider.class)),
                new ArrayList<EdmxReference>());
        defaultClient = mockClient();
    }

    public static Client mockClient() {
        return mockClient(new ArrayList<Map<String, Object>>());
    }

    public static Client mockClient(List<Map<String, Object>> dataToReturn) {
        SearchRequestBuilder builder = mockBuilder();
        return mockClient(dataToReturn, builder);
    }

    public static Client mockClient(SearchRequestBuilder builder) {
        return mockClient(new ArrayList<Map<String, Object>>(), builder);
    }

    @SuppressWarnings("unchecked")
    public static Client mockClient(List<Map<String, Object>> dataToReturn,
            SearchRequestBuilder builder) {
        Client client = mock(Client.class);
        SearchHit[] internalHitsArr = new SearchHit[dataToReturn.size()];
        for (int i = 0; i < dataToReturn.size(); i++) {
            Map<String, Object> data = dataToReturn.get(i);
            SearchHit hit = mock(SearchHit.class);
            internalHitsArr[i] = hit;
            when(hit.getSourceAsMap()).thenReturn(data);
            when(hit.getId()).thenReturn(Integer.toString(i));
        }
        SearchHits hits = new SearchHits(internalHitsArr, internalHitsArr.length,
                0);

        SearchResponse response = mock(SearchResponse.class);
        when(response.getHits()).thenReturn(hits);

        ListenableActionFuture<SearchResponse> action = mock(ListenableActionFuture.class);

        when(builder.execute()).thenReturn(action);
        when(builder.setQuery(any(QueryBuilder.class))).thenReturn(builder);
        when(action.actionGet()).thenReturn(response);
        when(client.prepareSearch(anyString())).thenReturn(builder);
        return client;
    }

    public static SearchRequestBuilder mockBuilder() {
        SearchRequestBuilder builder = mock(SearchRequestBuilder.class);
        when(builder.setTypes(anyString())).thenReturn(builder);
        when(builder.setFrom(anyInt())).thenReturn(builder);
        when(builder.setSize(anyInt())).thenReturn(builder);
        return builder;
    }

    public static UriInfo buildUriInfo(ServiceMetadata metadata, OData odata, String rawODataPath,
            String rawQueryPath) throws UriParserException, UriValidationException {
        return new Parser(metadata.getEdm(), odata).parseUri(rawODataPath, rawQueryPath, null);
    }

    public static void validateSerializerResult(InputStream result, List<Map<String, Object>> hits)
            throws IOException {
        StringWriter writer = new StringWriter();
        IOUtils.copy(result, writer);
        String theString = writer.toString();
        JSONObject obj = new JSONObject(theString);
        JSONArray arr = obj.getJSONArray("value");
        for (int i = 0; i < hits.size(); i++) {
            Map<String, Object> data = hits.get(i);
            for (Map.Entry<String, Object> entry : data.entrySet()) {
                JSONObject jsonObj = (JSONObject) arr.get(i);
                assertEquals(entry.getValue(), jsonObj.get(entry.getKey()));
            }
        }
        int count = obj.getInt("@odata.count");
        assertEquals(hits.size(), count);
    }

    public static void validateOneEntitySerializerResult(InputStream result,
            List<Map<String, Object>> hits) throws IOException {
        StringWriter writer = new StringWriter();
        IOUtils.copy(result, writer);
        String theString = writer.toString();
        JSONObject obj = new JSONObject(theString);
        for (int i = 0; i < hits.size(); i++) {
            Map<String, Object> data = hits.get(i);
            for (Map.Entry<String, Object> entry : data.entrySet()) {
                assertEquals(entry.getValue(), obj.get(entry.getKey()));
            }
        }
    }

    public static void validateOneFieldSerializerResult(InputStream result, Object value)
            throws IOException {
        StringWriter writer = new StringWriter();
        IOUtils.copy(result, writer);
        String theString = writer.toString();
        JSONObject obj = new JSONObject(theString);
        Object valueObj = obj.get("value");
        assertEquals(value, valueObj);
    }
}
