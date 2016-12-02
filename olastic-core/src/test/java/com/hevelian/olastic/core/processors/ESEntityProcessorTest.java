package com.hevelian.olastic.core.processors;

import org.apache.olingo.commons.api.format.ContentType;
import org.apache.olingo.server.api.ODataApplicationException;
import org.apache.olingo.server.api.ODataLibraryException;
import org.apache.olingo.server.api.ODataRequest;
import org.apache.olingo.server.api.ODataResponse;
import org.apache.olingo.server.core.uri.UriInfoImpl;
import org.apache.olingo.server.core.uri.parser.UriParserException;
import org.apache.olingo.server.core.uri.validator.UriValidationException;
import org.elasticsearch.client.Client;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import com.hevelian.olastic.core.processors.impl.ESEntityProcessorImpl;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.*;

/**
 * Tests for #ESEntityProcessor
 */
public class ESEntityProcessorTest extends BaseProcessorTest{

    private String defaultRawBaseUri = "http://localhost:8080/OData.svc";
    private ESEntityProcessorImpl defaultProcessor;
    private ContentType defaultContentType = ContentType.JSON;
    private ODataRequest defaultRequest;
    private ODataResponse defaultResponse;

    @Before
    public void setUp() throws UriParserException, UriValidationException {
        defaultUriInfo = buildUriInfo(defaultMetadata, defaultOData, defaultRawODataPath, defaultRawQueryPath);
        defaultProcessor = new ESEntityProcessorImpl(defaultClient);
        defaultRequest = mock(ODataRequest.class);
        defaultResponse = mock(ODataResponse.class);
        when(defaultRequest.getRawBaseUri()).thenReturn(defaultRawBaseUri);
    }

    @Test
    public void testReadEntity() throws ODataApplicationException, ODataLibraryException, IOException {
        Map<String, Object> data = new HashMap<>();
        data.put("age", 25);
        List<Map<String, Object>> hits = new ArrayList<>();
        hits.add(data);
        hits.add(data);

        Client client = mockClient(hits);
        defaultProcessor = new ESEntityProcessorImpl(client);
        defaultProcessor.init(defaultOData, defaultMetadata);

        ArgumentCaptor<InputStream> inputStreamCaptor = ArgumentCaptor.forClass(InputStream.class);
        defaultProcessor.readEntity(defaultRequest, defaultResponse, defaultUriInfo, defaultContentType);
        verify(defaultResponse, times(1)).setContent(inputStreamCaptor.capture());

        validateSerializerResult(inputStreamCaptor.getValue(), hits);
    }

    @Test(expected = ODataApplicationException.class)
    public void testCreateEntity() throws ODataApplicationException, ODataLibraryException {
        defaultProcessor.createEntity(new ODataRequest(), new ODataResponse(), new UriInfoImpl(),
                ContentType.JSON, ContentType.JSON);
    }

    @Test(expected = ODataApplicationException.class)
    public void testUpdateEntity() throws ODataApplicationException, ODataLibraryException {
        defaultProcessor.updateEntity(new ODataRequest(), new ODataResponse(), new UriInfoImpl(),
                ContentType.JSON, ContentType.JSON);
    }

    @Test(expected = ODataApplicationException.class)
    public void testDeleteEntity() throws ODataApplicationException, ODataLibraryException {
        defaultProcessor.deleteEntity(new ODataRequest(), new ODataResponse(), new UriInfoImpl());
    }
}