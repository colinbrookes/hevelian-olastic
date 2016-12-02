package com.hevelian.olastic.core;

import java.util.List;

import org.apache.olingo.commons.api.edm.provider.CsdlEdmProvider;
import org.apache.olingo.commons.api.edmx.EdmxReference;
import org.apache.olingo.commons.api.ex.ODataRuntimeException;
import org.apache.olingo.server.api.OData;
import org.apache.olingo.server.api.etag.ServiceMetadataETagSupport;
import org.apache.olingo.server.core.ODataImpl;

import com.hevelian.olastic.core.api.edm.provider.ElasticCsdlEdmProvider;

/**
 * Custom implementation of {@link OData} to override behavior of creating
 * entities and provide other side mappings from CSDL to ELasticsearch.
 * 
 * @author rdidyk
 */
public class ElasticOData extends ODataImpl {

    private ElasticOData() {
    }

    /**
     * @return a new OData instance
     */
    public static ElasticOData newInstance() {
        return new ElasticOData();
    }

    @Override
    public ElasticServiceMetadata createServiceMetadata(CsdlEdmProvider edmProvider,
            List<EdmxReference> references) {
        return createServiceMetadata(edmProvider, references, null);
    }

    @Override
    public ElasticServiceMetadata createServiceMetadata(CsdlEdmProvider edmProvider,
            List<EdmxReference> references, ServiceMetadataETagSupport serviceMetadataETagSupport) {
        if (!(edmProvider instanceof ElasticCsdlEdmProvider)) {
            throw new ODataRuntimeException(String.format(
                    "Invalid service metadata provider. Only %s instance is supported.",
                    ElasticCsdlEdmProvider.class.getName()));
        }
        return new ElasticServiceMetadata((ElasticCsdlEdmProvider) edmProvider, references,
                serviceMetadataETagSupport);
    }

}
