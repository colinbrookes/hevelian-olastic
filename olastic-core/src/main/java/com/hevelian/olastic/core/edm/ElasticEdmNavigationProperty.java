package com.hevelian.olastic.core.edm;

import org.apache.olingo.commons.api.edm.Edm;
import org.apache.olingo.commons.api.edm.EdmEntityType;
import org.apache.olingo.commons.api.edm.EdmException;
import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.apache.olingo.commons.core.edm.EdmNavigationPropertyImpl;

import com.hevelian.olastic.core.api.edm.provider.ElasticCsdlNavigationProperty;

/**
 * Custom implementation of {@link EdmNavigationPropertyImpl} to provide
 * behavior from {@link ElasticCsdlNavigationProperty} object.
 * 
 * @author rdidyk
 */
public class ElasticEdmNavigationProperty extends EdmNavigationPropertyImpl {

    private ElasticCsdlNavigationProperty navigationProperty;
    private EdmEntityType typeImpl;

    /**
     * Constructor to initialize navigation property.
     * 
     * @param edm
     *            EDM provider
     * @param navigationProperty
     *            CSDL navigation property
     */
    public ElasticEdmNavigationProperty(Edm edm, ElasticCsdlNavigationProperty navigationProperty) {
        super(edm, navigationProperty);
        this.navigationProperty = navigationProperty;
    }

    @Override
    public EdmEntityType getType() {
        if (typeImpl == null) {
            FullQualifiedName navigationFQN = null;
            if (navigationProperty.getESType() == null) {
                // in case custom entity types
                navigationFQN = navigationProperty.getTypeFQN();
            } else {
                navigationFQN = new FullQualifiedName(
                        navigationProperty.getTypeFQN().getNamespace(),
                        navigationProperty.getESType());
            }
            typeImpl = edm.getEntityType(navigationFQN);
            if (typeImpl == null) {
                throw new EdmException("Cannot find type with name: " + navigationFQN);
            }
        }
        return typeImpl;
    }
}
