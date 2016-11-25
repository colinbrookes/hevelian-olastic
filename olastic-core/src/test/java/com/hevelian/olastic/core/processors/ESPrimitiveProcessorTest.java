package com.hevelian.olastic.core.processors;

import org.apache.olingo.commons.api.format.ContentType;
import org.apache.olingo.commons.api.http.HttpStatusCode;
import org.apache.olingo.server.api.ODataApplicationException;
import org.apache.olingo.server.api.ODataLibraryException;
import org.apache.olingo.server.api.ODataRequest;
import org.apache.olingo.server.api.ODataResponse;
import org.apache.olingo.server.api.serializer.SerializerException;
import org.apache.olingo.server.core.uri.UriInfoImpl;
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
 * Tests for #ESPrimitiveProcessor
 */
public class ESPrimitiveProcessorTest extends BaseProcessorTest{
    private String defaultRawBaseUri = "http://localhost:8080/OData.svc";
    private ESPrimitiveProcessor defaultProcessor;
    private ContentType defaultContentType = ContentType.JSON;
    private ODataRequest defaultRequest;
    private ODataResponse defaultResponse;

    @Before
    public void setUp() throws UriParserException, UriValidationException {
        defaultRawODataPath = "/book('13')/character('113')/book/author/name";
        defaultRawQueryPath = "$top=2&$skip=10";
        defaultUriInfo = buildUriInfo(defaultMetadata, defaultOData, defaultRawODataPath, defaultRawQueryPath);
        defaultProcessor = new ESPrimitiveProcessor(defaultClient);
        defaultUriInfo = buildUriInfo(defaultMetadata, defaultOData, defaultRawODataPath, defaultRawQueryPath);

        defaultRequest = mock(ODataRequest.class);
        defaultResponse = mock(ODataResponse.class);
        when(defaultRequest.getRawBaseUri()).thenReturn(defaultRawBaseUri);
    }

    @Test
    public void testReadPrimitive() throws ODataApplicationException, SerializerException, IOException {
        Map<String, Object> data = new HashMap<>();
        String name = "Dawkins";
        data.put("name", name);
        List<Map<String, Object>> hits = new ArrayList<>();
        hits.add(data);

        Client client = mockClient(hits);
        defaultProcessor = new ESPrimitiveProcessor(client);
        defaultProcessor.init(defaultOData, defaultMetadata);

        ArgumentCaptor<InputStream> inputStreamCaptor = ArgumentCaptor.forClass(InputStream.class);
        defaultProcessor.readPrimitive(defaultRequest, defaultResponse, defaultUriInfo, defaultContentType);
        verify(defaultResponse, times(1)).setContent(inputStreamCaptor.capture());
        validateOneFieldSerializerResult(inputStreamCaptor.getValue(), name);
    }

    @Test(expected = ODataApplicationException.class)
    public void testReadPrimitiveNoEntity() throws ODataApplicationException, SerializerException, IOException {
        List<Map<String, Object>> hits = new ArrayList<>();
        Client client = mockClient(hits);
        defaultProcessor = new ESPrimitiveProcessor(client);
        defaultProcessor.init(defaultOData, defaultMetadata);

        defaultProcessor.readPrimitive(defaultRequest, defaultResponse, defaultUriInfo, defaultContentType);
    }

    @Test
    public void testReadPrimitiveEmptyResponse() throws ODataApplicationException, SerializerException, IOException {
        List<Map<String, Object>> hits = new ArrayList<>();
        Map<String, Object> data = new HashMap<>();
        hits.add(data);
        Client client = mockClient(hits);
        defaultProcessor = new ESPrimitiveProcessor(client);
        defaultProcessor.init(defaultOData, defaultMetadata);

        defaultProcessor.readPrimitive(defaultRequest, defaultResponse, defaultUriInfo, defaultContentType);
        verify(defaultResponse, times(0)).setContent(any(InputStream.class));
        verify(defaultResponse, times(1)).setStatusCode(HttpStatusCode.NO_CONTENT.getStatusCode());
    }

    @Test(expected = ODataApplicationException.class)
    public void testUpdatePrimitive() throws ODataApplicationException, ODataLibraryException {
        defaultProcessor.updatePrimitive(new ODataRequest(), new ODataResponse(), new UriInfoImpl(),
                ContentType.JSON, ContentType.JSON);
    }

    @Test(expected = ODataApplicationException.class)
    public void testDeletePrimitive() throws ODataApplicationException, ODataLibraryException {
        defaultProcessor.deletePrimitive(new ODataRequest(), new ODataResponse(), new UriInfoImpl());
    }
}