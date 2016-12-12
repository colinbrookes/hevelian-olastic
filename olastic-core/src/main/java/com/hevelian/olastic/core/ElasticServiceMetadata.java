package com.hevelian.olastic.core;

import java.util.List;

import org.apache.olingo.commons.api.edmx.EdmxReference;
import org.apache.olingo.server.api.ServiceMetadata;
import org.apache.olingo.server.api.etag.ServiceMetadataETagSupport;
import org.apache.olingo.server.core.ServiceMetadataImpl;

import com.hevelian.olastic.core.api.edm.provider.ElasticCsdlEdmProvider;
import com.hevelian.olastic.core.edm.ElasticEdmProvider;

/**
 * Custom implementation of {@link ServiceMetadata} to provide own
 * implementation of Edm.
 * 
 * @author rdidyk
 */
public class ElasticServiceMetadata extends ServiceMetadataImpl {

    private ElasticEdmProvider edm;

    public ElasticServiceMetadata(ElasticCsdlEdmProvider edmProvider,
            List<EdmxReference> references, ServiceMetadataETagSupport serviceMetadataETagSupport) {
        super(edmProvider, references, serviceMetadataETagSupport);
        this.edm = new ElasticEdmProvider(edmProvider);
    }

    @Override
    public ElasticEdmProvider getEdm() {
        return edm;
    }
}
