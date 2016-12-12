package com.hevelian.olastic.core.processors.impl;

import java.util.Locale;

import org.apache.olingo.commons.api.format.ContentType;
import org.apache.olingo.commons.api.http.HttpHeader;
import org.apache.olingo.commons.api.http.HttpStatusCode;
import org.apache.olingo.server.api.ODataApplicationException;
import org.apache.olingo.server.api.ODataLibraryException;
import org.apache.olingo.server.api.ODataRequest;
import org.apache.olingo.server.api.ODataResponse;
import org.apache.olingo.server.api.serializer.SerializerException;
import org.apache.olingo.server.api.serializer.SerializerResult;
import org.apache.olingo.server.api.uri.UriInfo;
import org.elasticsearch.client.Client;

import com.hevelian.olastic.core.ElasticOData;
import com.hevelian.olastic.core.ElasticServiceMetadata;
import com.hevelian.olastic.core.processors.ESPrimitiveProcessor;
import com.hevelian.olastic.core.processors.data.DataRetriever;
import com.hevelian.olastic.core.processors.data.FieldRetriever;

/**
 * Processes primitive value.
 */
public class ESPrimitiveProcessorImpl extends ESPrimitiveProcessor {

    private Client client;
    private ElasticOData odata;
    private ElasticServiceMetadata serviceMetadata;

    public ESPrimitiveProcessorImpl(Client client) {
        this.client = client;
    }

    @Override
    public void init(ElasticOData odata, ElasticServiceMetadata serviceMetadata) {
        this.odata = odata;
        this.serviceMetadata = serviceMetadata;
    }

    @Override
    public void readPrimitive(ODataRequest request, ODataResponse response, UriInfo uriInfo,
            ContentType responseFormat) throws ODataApplicationException, SerializerException {
        DataRetriever dataRetriever = new FieldRetriever(uriInfo, odata, client,
                request.getRawBaseUri(), serviceMetadata, responseFormat);
        SerializerResult serializerResult = dataRetriever.getSerializedData();
        if (serializerResult != null) {
            response.setContent(serializerResult.getContent());
            response.setStatusCode(HttpStatusCode.OK.getStatusCode());
            response.setHeader(HttpHeader.CONTENT_TYPE, responseFormat.toContentTypeString());
        } else {
            response.setStatusCode(HttpStatusCode.NO_CONTENT.getStatusCode());
        }
    }

    @Override
    public void updatePrimitive(ODataRequest request, ODataResponse response, UriInfo uriInfo,
            ContentType requestFormat, ContentType responseFormat)
            throws ODataApplicationException, ODataLibraryException {
        throw new ODataApplicationException("Not supported.",
                HttpStatusCode.NOT_IMPLEMENTED.getStatusCode(), Locale.ROOT);
    }

    @Override
    public void deletePrimitive(ODataRequest request, ODataResponse response, UriInfo uriInfo)
            throws ODataApplicationException, ODataLibraryException {
        throw new ODataApplicationException("Not supported.",
                HttpStatusCode.NOT_IMPLEMENTED.getStatusCode(), Locale.ROOT);
    }
}
