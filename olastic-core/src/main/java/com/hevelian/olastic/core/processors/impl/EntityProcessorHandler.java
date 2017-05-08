package com.hevelian.olastic.core.processors.impl;

import com.hevelian.olastic.core.ElasticOData;
import com.hevelian.olastic.core.ElasticServiceMetadata;
import com.hevelian.olastic.core.processors.ESProcessor;
import com.hevelian.olastic.core.processors.ESReadProcessor;
import org.apache.olingo.commons.api.format.ContentType;
import org.apache.olingo.server.api.ODataApplicationException;
import org.apache.olingo.server.api.ODataLibraryException;
import org.apache.olingo.server.api.ODataRequest;
import org.apache.olingo.server.api.ODataResponse;
import org.apache.olingo.server.api.processor.EntityProcessor;
import org.apache.olingo.server.api.uri.UriInfo;

import static com.hevelian.olastic.core.utils.ProcessorUtils.throwNotImplemented;

/**
 * Custom Elastic processor for handling all request to retrieve data for single
 * entity.
 * 
 * @author Taras Kohut
 * @author rdidyk
 */
public class EntityProcessorHandler implements ESProcessor, EntityProcessor {

    protected ElasticOData odata;
    protected ElasticServiceMetadata serviceMetadata;

    @Override
    public void init(ElasticOData odata, ElasticServiceMetadata serviceMetadata) {
        this.odata = odata;
        this.serviceMetadata = serviceMetadata;
    }

    @Override
    public void readEntity(ODataRequest request, ODataResponse response, UriInfo uriInfo,
            ContentType responseFormat) throws ODataApplicationException, ODataLibraryException {
        ESReadProcessor collectionProcessor = getEntityReadProcessor(uriInfo);
        collectionProcessor.init(odata, serviceMetadata);
        collectionProcessor.read(request, response, uriInfo, responseFormat);
    }

    @Override
    public void createEntity(ODataRequest request, ODataResponse response, UriInfo uriInfo,
            ContentType requestFormat, ContentType responseFormat)
            throws ODataApplicationException, ODataLibraryException {
        throwNotImplemented();
    }

    @Override
    public void updateEntity(ODataRequest request, ODataResponse response, UriInfo uriInfo,
            ContentType requestFormat, ContentType responseFormat)
            throws ODataApplicationException, ODataLibraryException {
        throwNotImplemented();
    }

    @Override
    public void deleteEntity(ODataRequest request, ODataResponse response, UriInfo uriInfo)
            throws ODataApplicationException, ODataLibraryException {
        throwNotImplemented();
    }

    /**
     * Gets specific entity processor based on items from query options in URL.
     *
     * @param uriInfo
     *            URI info
     * @return processor to get data
     * @throws ODataApplicationException
     *             if any error occurred
     */
    protected ESReadProcessor getEntityReadProcessor(UriInfo uriInfo)
            throws ODataApplicationException {
        return new EntityProcessorImpl();
    }

}
