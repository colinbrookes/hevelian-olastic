package com.hevelian.olastic.core.processors.data;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.olingo.commons.api.format.ContentType;
import org.apache.olingo.server.api.ODataApplicationException;
import org.apache.olingo.server.api.serializer.SerializerException;
import org.apache.olingo.server.api.serializer.SerializerResult;
import org.apache.olingo.server.api.uri.UriInfo;
import org.apache.olingo.server.core.uri.parser.UriParserException;
import org.apache.olingo.server.core.uri.validator.UriValidationException;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.junit.Before;
import org.junit.Test;

import com.hevelian.olastic.core.edm.ElasticEdmEntitySet;
import com.hevelian.olastic.core.processors.BaseProcessorTest;

/**
 * Tests for #FieldDataRetriever
 */
public class FieldDataRetrieverTest extends BaseProcessorTest {
    private String defaultRawBaseUri = "http://localhost:8080/OData.svc";
    private DataRetriever defaultRetriever;
    private ContentType defaultContentType = ContentType.JSON;

    @Before
    public void setUp() throws Exception {
        defaultRawODataPath = "/book('13')/character('113')/book/author/name";
        defaultRawQueryPath = "$top=2&$skip=10";
        defaultUriInfo = buildUriInfo(defaultMetadata, defaultOData, defaultRawODataPath,
                defaultRawQueryPath);
        defaultRetriever = new FieldDataRetriever(defaultUriInfo, defaultOData, defaultClient,
                defaultRawBaseUri, defaultMetadata, defaultContentType);
    }

    @Test
    public void testGetUsefulPartsSize() throws UriParserException, UriValidationException {
        assertEquals(4, defaultRetriever.getUsefulPartsSize());
        UriInfo uriInfo = buildUriInfo(defaultMetadata, defaultOData, defaultRawODataPath, null);
        DataRetriever retriever = new FieldDataRetriever(uriInfo, defaultOData, defaultClient,
                defaultRawBaseUri, defaultMetadata, defaultContentType);
        assertEquals(4, retriever.getUsefulPartsSize());
    }

    @Test
    public void testGetSelectList() {
        List<String> selectList = defaultRetriever.getSelectList();
        assertEquals(1, selectList.size());
        assertEquals("name", selectList.get(0));
    }

    @Test
    public void testSerialize() throws SerializerException, ODataApplicationException, IOException,
            UriParserException, UriValidationException {
        Map<String, Object> data = new HashMap<>();
        String name = "Dawkins";
        data.put("name", name);
        List<Map<String, Object>> hits = new ArrayList<>();
        hits.add(data);

        ElasticEdmEntitySet entitySet = (ElasticEdmEntitySet) defaultMetadata.getEdm()
                .getEntityContainer().getEntitySet("author");
        Client client = mockClient(hits);
        SearchResponse response = client.prepareSearch("").execute().actionGet();
        SerializerResult result = defaultRetriever.serialize(response, entitySet);
        validateOneFieldSerializerResult(result.getContent(), name);

        String rawODataPath = "/book('13')/character('113')/book/author/age";
        data = new HashMap<>();
        int age = 70;
        data.put("age", age);
        hits = new ArrayList<>();
        hits.add(data);
        client = mockClient(hits);
        response = client.prepareSearch("").execute().actionGet();
        UriInfo uriInfo = buildUriInfo(defaultMetadata, defaultOData, rawODataPath, null);
        DataRetriever retriever = new FieldDataRetriever(uriInfo, defaultOData, defaultClient,
                defaultRawBaseUri, defaultMetadata, defaultContentType);

        result = retriever.serialize(response, entitySet);
        validateOneFieldSerializerResult(result.getContent(), age);

    }
}