package com.hevelian.olastic.core.processors.impl;

import java.util.Locale;

import org.apache.olingo.commons.api.format.ContentType;
import org.apache.olingo.commons.api.http.HttpHeader;
import org.apache.olingo.commons.api.http.HttpStatusCode;
import org.apache.olingo.server.api.ODataApplicationException;
import org.apache.olingo.server.api.ODataLibraryException;
import org.apache.olingo.server.api.ODataRequest;
import org.apache.olingo.server.api.ODataResponse;
import org.apache.olingo.server.api.serializer.SerializerResult;
import org.apache.olingo.server.api.uri.UriInfo;
import org.elasticsearch.client.Client;

import com.hevelian.olastic.core.ElasticOData;
import com.hevelian.olastic.core.ElasticServiceMetadata;
import com.hevelian.olastic.core.processors.ESEntityProcessor;
import com.hevelian.olastic.core.processors.data.DataRetriever;
import com.hevelian.olastic.core.processors.data.EntityRetriever;

/**
 * Processes single entity.
 */
public class ESEntityProcessorImpl extends ESEntityProcessor {

    private Client client;
    private ElasticOData odata;
    private ElasticServiceMetadata serviceMetadata;

    public ESEntityProcessorImpl(Client client) {
        this.client = client;
    }

    @Override
    public void init(ElasticOData odata, ElasticServiceMetadata serviceMetadata) {
        this.odata = odata;
        this.serviceMetadata = serviceMetadata;
    }

    @Override
    public void readEntity(ODataRequest request, ODataResponse response, UriInfo uriInfo,
            ContentType responseFormat) throws ODataApplicationException, ODataLibraryException {
        DataRetriever dataRetriever = new EntityRetriever(uriInfo, odata, client,
                request.getRawBaseUri(), serviceMetadata, responseFormat);

        SerializerResult serializerResult = dataRetriever.getSerializedData();
        response.setContent(serializerResult.getContent());
        response.setStatusCode(HttpStatusCode.OK.getStatusCode());
        response.setHeader(HttpHeader.CONTENT_TYPE, responseFormat.toContentTypeString());
    }

    @Override
    public void createEntity(ODataRequest request, ODataResponse response, UriInfo uriInfo,
            ContentType requestFormat, ContentType responseFormat)
            throws ODataApplicationException, ODataLibraryException {
        throw new ODataApplicationException("Not supported.",
                HttpStatusCode.NOT_IMPLEMENTED.getStatusCode(), Locale.ROOT);
    }

    @Override
    public void updateEntity(ODataRequest request, ODataResponse response, UriInfo uriInfo,
            ContentType requestFormat, ContentType responseFormat)
            throws ODataApplicationException, ODataLibraryException {
        throw new ODataApplicationException("Not supported.",
                HttpStatusCode.NOT_IMPLEMENTED.getStatusCode(), Locale.ROOT);
    }

    @Override
    public void deleteEntity(ODataRequest request, ODataResponse response, UriInfo uriInfo)
            throws ODataApplicationException, ODataLibraryException {
        throw new ODataApplicationException("Not supported.",
                HttpStatusCode.NOT_IMPLEMENTED.getStatusCode(), Locale.ROOT);
    }

}