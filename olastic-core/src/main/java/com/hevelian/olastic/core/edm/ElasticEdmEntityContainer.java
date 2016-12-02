package com.hevelian.olastic.core.edm;

import org.apache.olingo.commons.api.edm.EdmException;
import org.apache.olingo.commons.api.edm.provider.CsdlEntityContainerInfo;
import org.apache.olingo.commons.api.ex.ODataException;
import org.apache.olingo.commons.core.edm.EdmEntityContainerImpl;

import com.hevelian.olastic.core.api.edm.provider.ElasticCsdlEdmProvider;
import com.hevelian.olastic.core.api.edm.provider.ElasticCsdlEntitySet;

/**
 * Custom implementation of EDM Entity container.
 * 
 * @author rdidyk
 */
public class ElasticEdmEntityContainer extends EdmEntityContainerImpl {

    private ElasticCsdlEdmProvider csdlProvider;
    private ElasticEdmProvider edmProvider;

    /**
     * Constructor to initialize values.
     * 
     * @param edmProvider
     *            EDM provider
     * @param csdlProvider
     *            CSDL provider
     * @param entityContainerInfo
     *            entity container info
     */
    public ElasticEdmEntityContainer(ElasticEdmProvider edmProvider,
            ElasticCsdlEdmProvider csdlProvider, CsdlEntityContainerInfo entityContainerInfo) {
        super(edmProvider, csdlProvider, entityContainerInfo);
        this.edmProvider = edmProvider;
        this.csdlProvider = csdlProvider;
    }

    @Override
    protected ElasticEdmEntitySet createEntitySet(String entitySetName) {
        ElasticEdmEntitySet entitySet = null;
        try {
            ElasticCsdlEntitySet providerEntitySet = csdlProvider
                    .getEntitySet(getFullQualifiedName(), entitySetName);
            if (providerEntitySet != null) {
                entitySet = new ElasticEdmEntitySet(edmProvider, this, providerEntitySet);
            }
        } catch (ODataException e) {
            throw new EdmException(e);
        }
        return entitySet;
    }

}
