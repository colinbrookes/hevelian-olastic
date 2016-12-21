package com.hevelian.olastic.core.processors.data;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.olingo.commons.api.edm.EdmEntitySet;
import org.apache.olingo.commons.api.format.ContentType;
import org.apache.olingo.server.api.ODataApplicationException;
import org.apache.olingo.server.api.serializer.SerializerException;
import org.apache.olingo.server.api.serializer.SerializerResult;
import org.apache.olingo.server.api.uri.UriInfo;
import org.apache.olingo.server.core.uri.parser.UriParserException;
import org.apache.olingo.server.core.uri.validator.UriValidationException;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatchers;

import com.hevelian.olastic.core.edm.ElasticEdmEntitySet;
import com.hevelian.olastic.core.elastic.builders.ESQueryBuilder;
import com.hevelian.olastic.core.elastic.pagination.Pagination;
import com.hevelian.olastic.core.elastic.pagination.Sort;
import com.hevelian.olastic.core.processors.BaseProcessorTest;

/**
 * Tests for #EntityCollectionRetriever.
 */
public class EntityCollectionRetrieverTest extends BaseProcessorTest {
    private String defaultRawBaseUri = "http://localhost:8080/OData.svc";
    private EntityCollectionRetriever defaultRetriever;
    private ContentType defaultContentType = ContentType.JSON;

    @Before
    public void setUp() throws Exception {
        defaultUriInfo = buildUriInfo(defaultMetadata, defaultOData, defaultRawODataPath,
                defaultRawQueryPath);
        defaultRetriever = new EntityCollectionRetriever(defaultUriInfo, defaultOData,
                defaultClient, defaultRawBaseUri, defaultMetadata, defaultContentType);
    }

    @Test
    public void testIsCount() throws Exception {
        assertTrue(defaultRetriever.isCount());

        UriInfo uriInfo = buildUriInfo(defaultMetadata, defaultOData, defaultRawODataPath, null);
        EntityCollectionRetriever retriever = new EntityCollectionRetriever(uriInfo, defaultOData,
                defaultClient, defaultRawBaseUri, defaultMetadata, defaultContentType);
        assertFalse(retriever.isCount());

        String rawQueryPath = "$count=false";
        uriInfo = buildUriInfo(defaultMetadata, defaultOData, defaultRawODataPath, rawQueryPath);
        retriever = new EntityCollectionRetriever(uriInfo, defaultOData, defaultClient,
                defaultRawBaseUri, defaultMetadata, defaultContentType);
        assertFalse(retriever.isCount());

    }

    @Test
    public void testGetUsefulPartSize() throws Exception {
        assertEquals(4, defaultRetriever.getUsefulPartsSize());
        UriInfo uriInfo = buildUriInfo(defaultMetadata, defaultOData, defaultRawODataPath, null);
        EntityCollectionRetriever retriever = new EntityCollectionRetriever(uriInfo, defaultOData,
                defaultClient, defaultRawBaseUri, defaultMetadata, defaultContentType);
        assertEquals(4, retriever.getUsefulPartsSize());
    }

    @Test
    public void testGetPagination() throws Exception {
        Pagination pagination = defaultRetriever.getPagination();
        assertEquals(10, pagination.getSkip());
        assertEquals(2, pagination.getTop());

        List<Sort> orderBy = pagination.getOrderBy();

        assertEquals("age", orderBy.get(0).getProperty());
        assertEquals(Sort.Direction.ASC, orderBy.get(0).getDirection());

        assertEquals("_id", orderBy.get(1).getProperty());
        assertEquals(Sort.Direction.DESC, orderBy.get(1).getDirection());
    }

    @Test
    public void testGetSerializedData()
            throws ODataApplicationException, SerializerException, IOException {
        Map<String, Object> data1 = new HashMap<>();
        data1.put("age", 35);
        Map<String, Object> data2 = new HashMap<>();
        data2.put("age", 25);
        List<Map<String, Object>> hits = new ArrayList<>();
        hits.add(data1);
        hits.add(data2);

        Client client = mockClient(hits);
        EntityCollectionRetriever retriever = new EntityCollectionRetriever(defaultUriInfo,
                defaultOData, client, defaultRawBaseUri, defaultMetadata, defaultContentType);
        SerializerResult result = retriever.getSerializedData();

        validateSerializerResult(result.getContent(), hits);
    }

    @Test
    public void testSerialize() throws SerializerException, ODataApplicationException, IOException {
        Map<String, Object> data1 = new HashMap<>();
        data1.put("age", 35);
        Map<String, Object> data2 = new HashMap<>();
        data2.put("age", 25);
        List<Map<String, Object>> hits = new ArrayList<>();
        hits.add(data1);
        hits.add(data2);

        ElasticEdmEntitySet entitySet = (ElasticEdmEntitySet) defaultMetadata.getEdm()
                .getEntityContainer().getEntitySet("author");
        Client client = mockClient(hits);
        SearchResponse response = client.prepareSearch("").execute().actionGet();
        SerializerResult result = defaultRetriever.serialize(response, entitySet);
        validateSerializerResult(result.getContent(), hits);
    }

    @Test
    public void testGetSelectList() throws UriParserException, UriValidationException {
        List<String> selectList = defaultRetriever.getSelectList();
        assertTrue(selectList.isEmpty());

        String rawQueryPath = "$select=age,_id";
        UriInfo uriInfo = buildUriInfo(defaultMetadata, defaultOData, defaultRawODataPath,
                rawQueryPath);
        EntityCollectionRetriever retriever = new EntityCollectionRetriever(uriInfo, defaultOData,
                defaultClient, defaultRawBaseUri, defaultMetadata, defaultContentType);
        selectList = retriever.getSelectList();
        assertEquals(2, selectList.size());
        assertEquals("age", selectList.get(0));
        assertEquals("_id", selectList.get(1));
    }

    @Test
    public void testGetQueryWithEntitySet() throws ODataApplicationException {
        EntityCollectionRetriever.QueryWithEntity queryWithEntitySet = defaultRetriever
                .getQueryWithEntity();
        EdmEntitySet entitySet = defaultMetadata.getEdm().getEntityContainer()
                .getEntitySet("author");
        assertEquals(entitySet, queryWithEntitySet.getEntitySet());
        assertTrue(queryWithEntitySet.getQuery().getQuery().hasClauses());
    }

    @Test
    public void testCollectIds() throws ODataApplicationException {
        List<String> ids = defaultRetriever.collectIds(defaultUriInfo.getUriResourceParts().get(0));
        List<String> expected = new ArrayList<>();
        expected.add("13");
        assertEquals(expected, ids);
    }

    @Test(expected = ODataApplicationException.class)
    public void testCollectCompositeIds()
            throws ODataApplicationException, UriParserException, UriValidationException {
        String rawODataPath = "/address(_city='lviv',_id='13')";
        UriInfo uriInfo = buildUriInfo(defaultMetadata, defaultOData, rawODataPath, null);
        EntityCollectionRetriever retriever = new EntityCollectionRetriever(uriInfo, defaultOData,
                defaultClient, defaultRawBaseUri, defaultMetadata, defaultContentType);
        retriever.collectIds(uriInfo.getUriResourceParts().get(0));
    }

    @Test
    public void testGetData()
            throws UriParserException, UriValidationException, ODataApplicationException {
        ArgumentCaptor<Integer> sizeCaptor = ArgumentCaptor.forClass(Integer.class);
        ArgumentCaptor<Integer> fromCaptor = ArgumentCaptor.forClass(Integer.class);
        ESQueryBuilder query = new ESQueryBuilder();
        SearchRequestBuilder builder = mockBuilder();
        query.setIndex("sometype");
        query.setType("sometype");
        Client client = mockClient(builder);
        EntityCollectionRetriever retriever = new EntityCollectionRetriever(defaultUriInfo,
                defaultOData, client, defaultRawBaseUri, defaultMetadata, defaultContentType);
        retriever.retrieveData(query);

        verify(client, times(1)).prepareSearch(anyString());
        verify(builder, times(1)).setSize(sizeCaptor.capture());
        verify(builder, times(1)).setFrom(fromCaptor.capture());
        verify(builder, times(0)).setFetchSource(ArgumentMatchers.<String[]> any(),
                ArgumentMatchers.<String[]> any());

        assertEquals(2, sizeCaptor.getValue().intValue());
        assertEquals(10, fromCaptor.getValue().intValue());
    }

    @Test
    public void testGetDataWithSelect()
            throws UriParserException, UriValidationException, ODataApplicationException {
        String rawQueryPath = "$select=age,_id";
        UriInfo uriInfo = buildUriInfo(defaultMetadata, defaultOData, defaultRawODataPath,
                rawQueryPath);
        ArgumentCaptor<String[]> sourceCaptor = ArgumentCaptor.forClass(String[].class);
        ESQueryBuilder query = new ESQueryBuilder();
        SearchRequestBuilder builder = mockBuilder();
        query.setIndex("sometype");
        query.setType("sometype");
        query.addField("age").addField("_id");
        Client client = mockClient(builder);
        EntityCollectionRetriever retriever = new EntityCollectionRetriever(uriInfo, defaultOData,
                client, defaultRawBaseUri, defaultMetadata, defaultContentType);
        retriever.retrieveData(query);
        verify(client, times(1)).prepareSearch(anyString());

        verify(builder, times(1)).setFetchSource(sourceCaptor.capture(),
                ArgumentMatchers.<String[]> any());
        assertArrayEquals(new String[] { "_id", "age" }, sourceCaptor.getValue());
    }
}