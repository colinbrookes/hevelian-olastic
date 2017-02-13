package com.hevelian.olastic.core.edm;

import org.apache.olingo.commons.api.edm.EdmComplexType;
import org.apache.olingo.commons.api.edm.EdmEntityContainer;
import org.apache.olingo.commons.api.edm.EdmException;
import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.apache.olingo.commons.api.edm.provider.CsdlEntityContainerInfo;
import org.apache.olingo.commons.api.ex.ODataException;
import org.apache.olingo.commons.core.edm.EdmProviderImpl;

import com.hevelian.olastic.core.api.edm.provider.ElasticCsdlComplexType;
import com.hevelian.olastic.core.api.edm.provider.ElasticCsdlEdmProvider;
import com.hevelian.olastic.core.api.edm.provider.ElasticCsdlEntityType;

/**
 * Custom implementation of EDM provider.
 * 
 * @author rdidyk
 */
public class ElasticEdmProvider extends EdmProviderImpl {

    private ElasticCsdlEdmProvider csdlProvider;

    /**
     * Constructor to initialize provider.
     * 
     * @param provider
     *            CSDL provider
     */
    public ElasticEdmProvider(ElasticCsdlEdmProvider provider) {
        super(provider);
        this.csdlProvider = provider;
    }

    @Override
    public ElasticEdmEntityType createEntityType(FullQualifiedName entityTypeName) {
        try {
            ElasticCsdlEntityType entityType = csdlProvider.getEntityType(entityTypeName);
            if (entityType != null) {
                return new ElasticEdmEntityType(this, entityTypeName, entityType);
            }
            return null;
        } catch (ODataException e) {
            throw new EdmException(e);
        }
    }

    @Override
    public EdmEntityContainer createEntityContainer(FullQualifiedName containerName) {
        CsdlEntityContainerInfo entityContainerInfo = csdlProvider
                .getEntityContainerInfo(containerName);
        if (entityContainerInfo != null) {
            return new ElasticEdmEntityContainer(this, csdlProvider, entityContainerInfo);
        }
        return null;
    }

    @Override
    public EdmComplexType createComplexType(FullQualifiedName complexTypeName) {
        try {
            ElasticCsdlComplexType complexType = csdlProvider.getComplexType(complexTypeName);
            if (complexType != null) {
                return new ElasticEdmComplexType(this, complexTypeName, complexType);
            }
            return null;
        } catch (ODataException e) {
            throw new EdmException(e);
        }
    }
}
