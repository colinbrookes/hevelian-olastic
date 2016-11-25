package com.hevelian.olastic.core.processors;

import org.apache.olingo.commons.api.format.ContentType;
import org.apache.olingo.server.api.ODataApplicationException;
import org.apache.olingo.server.api.ODataLibraryException;
import org.apache.olingo.server.api.ODataRequest;
import org.apache.olingo.server.api.ODataResponse;
import org.apache.olingo.server.core.uri.parser.UriParserException;
import org.apache.olingo.server.core.uri.validator.UriValidationException;
import org.elasticsearch.client.Client;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.*;

/**
 * Created by tkoh on 24.11.2016.
 */
public class ESEntityCollectionProcessorTest extends BaseProcessorTest{
    private String defaultRawBaseUri = "http://localhost:8080/OData.svc";
    private ESEntityCollectionProcessor defaultProcessor;
    private ContentType defaultContentType = ContentType.JSON;
    private ODataRequest defaultRequest;
    private ODataResponse defaultResponse;

    @Before
    public void setUp() throws UriParserException, UriValidationException {
        defaultRawODataPath = "author";
        defaultUriInfo = buildUriInfo(defaultMetadata, defaultOData, defaultRawODataPath, defaultRawQueryPath);
        defaultRequest = mock(ODataRequest.class);
        defaultResponse = mock(ODataResponse.class);
        when(defaultRequest.getRawBaseUri()).thenReturn(defaultRawBaseUri);
    }

    @Test
    public void testReadEntityCollection() throws ODataApplicationException, ODataLibraryException, IOException {
        Map<String, Object> data1 = new HashMap<>();
        data1.put("age", 35);
        Map<String, Object> data2 = new HashMap<>();
        data2.put("age", 25);
        List<Map<String, Object>> hits = new ArrayList<>();
        hits.add(data1);
        hits.add(data2);

        Client client = mockClient(hits);
        defaultProcessor = new ESEntityCollectionProcessor(client);
        defaultProcessor.init(defaultOData, defaultMetadata);

        ArgumentCaptor<InputStream> inputStreamCaptor = ArgumentCaptor.forClass(InputStream.class);
        defaultProcessor.readEntityCollection(defaultRequest, defaultResponse, defaultUriInfo, defaultContentType);
        verify(defaultResponse, times(1)).setContent(inputStreamCaptor.capture());

        validateSerializerResult(inputStreamCaptor.getValue(), hits);
    }
}