package com.hevelian.olastic.core;

import java.util.Collections;
import java.util.List;

import org.apache.olingo.commons.api.edm.Edm;
import org.apache.olingo.commons.api.edm.constants.ODataServiceVersion;
import org.apache.olingo.commons.api.edmx.EdmxReference;
import org.apache.olingo.server.api.ServiceMetadata;
import org.apache.olingo.server.api.etag.ServiceMetadataETagSupport;

import com.hevelian.olastic.core.api.edm.provider.ElasticCsdlEdmProvider;
import com.hevelian.olastic.core.edm.ElasticEdmProvider;

/**
 * Custom implementation of {@link ServiceMetadata} to provide own
 * implementation of Edm.
 * 
 * @author rdidyk
 */
public class ElasticServiceMetadata implements ServiceMetadata {

    private ElasticEdmProvider edm;
    private final List<EdmxReference> references;
    private final ServiceMetadataETagSupport serviceMetadataETagSupport;

    public ElasticServiceMetadata(ElasticCsdlEdmProvider edmProvider,
            List<EdmxReference> references, ServiceMetadataETagSupport serviceMetadataETagSupport) {
        this.edm = new ElasticEdmProvider(edmProvider);
        this.references = references;
        this.serviceMetadataETagSupport = serviceMetadataETagSupport;
    }

    @Override
    public Edm getEdm() {
        return edm;
    }

    @Override
    public ODataServiceVersion getDataServiceVersion() {
        return ODataServiceVersion.V40;
    }

    @Override
    public List<EdmxReference> getReferences() {
        return Collections.unmodifiableList(references);
    }

    @Override
    public ServiceMetadataETagSupport getServiceMetadataETagSupport() {
        return serviceMetadataETagSupport;
    }
}
