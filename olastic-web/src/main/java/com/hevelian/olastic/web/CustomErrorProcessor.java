package com.hevelian.olastic.web;

import org.apache.olingo.commons.api.format.ContentType;
import org.apache.olingo.commons.api.http.HttpHeader;
import org.apache.olingo.server.api.OData;
import org.apache.olingo.server.api.ODataRequest;
import org.apache.olingo.server.api.ODataResponse;
import org.apache.olingo.server.api.ODataServerError;
import org.apache.olingo.server.api.ServiceMetadata;
import org.apache.olingo.server.api.processor.ErrorProcessor;
import org.apache.olingo.server.api.serializer.ODataSerializer;

import lombok.extern.log4j.Log4j2;
import org.apache.olingo.server.api.serializer.SerializerException;

/**
 * Processor that is triggered when error occurs.
 */
@Log4j2
public class CustomErrorProcessor implements ErrorProcessor {

    private OData odata;

    @Override
    public void processError(ODataRequest request, ODataResponse response,
                             ODataServerError serverError, ContentType responseFormat) {
        ODataSerializer serializer;
        log.error(serverError.getException()
                .getMessage(), serverError.getException());
        response.setStatusCode(serverError.getStatusCode());
        try {
            serializer = odata.createSerializer(responseFormat);
            response.setContent(serializer.error(serverError)
                    .getContent());
        } catch (SerializerException e) {
            log.error(e);
        }
        response.setHeader(HttpHeader.CONTENT_TYPE, responseFormat.toContentTypeString());
    }

    @Override
    public void init(OData odata, ServiceMetadata serviceMetadata) {
        this.odata = odata;
    }
}
