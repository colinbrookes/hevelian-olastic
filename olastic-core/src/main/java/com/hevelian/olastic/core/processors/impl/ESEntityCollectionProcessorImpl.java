package com.hevelian.olastic.core.processors.impl;

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
import com.hevelian.olastic.core.processors.ESEntityCollectionProcessor;
import com.hevelian.olastic.core.processors.data.DataRetriever;

/**
 * Processes entity collection.
 */
public class ESEntityCollectionProcessorImpl extends ESEntityCollectionProcessor {

    private Client client;
    private ElasticOData odata;
    private ElasticServiceMetadata serviceMetadata;

    public ESEntityCollectionProcessorImpl(Client client) {
        this.client = client;
    }

    @Override
    public void init(ElasticOData odata, ElasticServiceMetadata serviceMetadata) {
        this.odata = odata;
        this.serviceMetadata = serviceMetadata;
    }

    @Override
    public void readEntityCollection(ODataRequest request, ODataResponse response, UriInfo uriInfo,
            ContentType responseFormat) throws ODataApplicationException, ODataLibraryException {
        DataRetriever dataRetriever = new DataRetriever(uriInfo, odata, client,
                request.getRawBaseUri(), serviceMetadata, responseFormat);

        SerializerResult serializerResult = dataRetriever.getSerializedData();
        response.setContent(serializerResult.getContent());
        response.setStatusCode(HttpStatusCode.OK.getStatusCode());
        response.setHeader(HttpHeader.CONTENT_TYPE, responseFormat.toContentTypeString());
    }

}
