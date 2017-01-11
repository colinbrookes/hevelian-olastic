package com.hevelian.olastic.core.processors;

import org.apache.olingo.commons.api.format.ContentType;
import org.apache.olingo.server.api.ODataApplicationException;
import org.apache.olingo.server.api.ODataLibraryException;
import org.apache.olingo.server.api.ODataRequest;
import org.apache.olingo.server.api.ODataResponse;
import org.apache.olingo.server.api.uri.UriInfo;

/**
 * Processor interface for handling read method for processors.
 * 
 * @author rdidyk
 */
public interface ESReadProcessor extends ESProcessor {

    /**
     * Reads data from persistence and returns serialized content.
     * 
     * @param request
     *            OData request object containing raw HTTP information
     * @param response
     *            OData response object for collecting response data
     * @param uriInfo
     *            information of a parsed OData URI
     * @param responseFormat
     *            requested content type after content negotiation
     * @throws ODataApplicationException
     *             if the service implementation encounters a failure
     * @throws ODataLibraryException
     */
    void read(ODataRequest request, ODataResponse response, UriInfo uriInfo,
            ContentType responseFormat) throws ODataApplicationException, ODataLibraryException;

}
