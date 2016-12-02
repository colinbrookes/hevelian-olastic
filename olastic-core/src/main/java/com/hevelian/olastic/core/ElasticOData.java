package com.hevelian.olastic.core;

import static com.hevelian.olastic.core.utils.MetaDataUtils.castToType;

import java.util.List;

import org.apache.olingo.commons.api.edm.provider.CsdlEdmProvider;
import org.apache.olingo.commons.api.edmx.EdmxReference;
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
        return new ElasticServiceMetadata(castToType(edmProvider, ElasticCsdlEdmProvider.class),
                references, serviceMetadataETagSupport);
    }

}
