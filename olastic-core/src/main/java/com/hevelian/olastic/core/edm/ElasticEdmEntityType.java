package com.hevelian.olastic.core.edm;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.olingo.commons.api.edm.Edm;
import org.apache.olingo.commons.api.edm.EdmNavigationProperty;
import org.apache.olingo.commons.api.edm.EdmProperty;
import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.apache.olingo.commons.api.edm.provider.CsdlNavigationProperty;
import org.apache.olingo.commons.api.edm.provider.CsdlProperty;
import org.apache.olingo.commons.core.edm.EdmEntityTypeImpl;

import com.hevelian.olastic.core.api.edm.provider.ElasticCsdlEntityType;
import com.hevelian.olastic.core.api.edm.provider.ElasticCsdlNavigationProperty;
import com.hevelian.olastic.core.api.edm.provider.ElasticCsdlProperty;

/**
 * Custom implementation of {@link EdmEntityTypeImpl}.
 * 
 * @author rdidyk
 */
public class ElasticEdmEntityType extends EdmEntityTypeImpl {

    private ElasticCsdlEntityType csdlEntityType;
    private Map<String, ElasticEdmProperty> propertiesCash;
    private Map<String, ElasticEdmNavigationProperty> navigationPropertiesCash;

    /**
     * Constructor to initialize entity type.
     * 
     * @param edm
     *            EDM provider
     * @param name
     *            entity type FQN
     * @param entityType
     *            CSDL entity type
     */
    public ElasticEdmEntityType(Edm edm, FullQualifiedName name, ElasticCsdlEntityType entityType) {
        super(edm, name, entityType);
        this.csdlEntityType = entityType;
    }

    /**
     * Get's index name in Elasticsearch.
     * 
     * @return index name
     */
    public String getEIndex() {
        return csdlEntityType.getESIndex();
    }

    /**
     * Get's type name in Elasticsearch.
     * 
     * @return type name
     */
    public String getEType() {
        return csdlEntityType.getESType();
    }

    /**
     * Get's CSDL property name by Elasticsearch field name.
     * 
     * @param esFieldName
     *            Elasticsearch field name
     * @return found property
     */
    public ElasticEdmProperty findPropertyByEField(String esFieldName) {
        for (Entry<String, ElasticEdmProperty> entry : getEProperties().entrySet()) {
            ElasticEdmProperty property = entry.getValue();
            if (property.getEField().equals(esFieldName)) {
                return property;
            }
        }
        return null;
    }

    /**
     * Gets Elasticsearch properties map.
     * 
     * @return properties map, with key - property name and value -
     *         {@link ElasticEdmProperty} instance
     */
    public Map<String, ElasticEdmProperty> getEProperties() {
        if (propertiesCash == null) {
            Map<String, ElasticEdmProperty> localPorperties = new LinkedHashMap<>();
            List<CsdlProperty> typeProperties = csdlEntityType.getProperties();
            for (CsdlProperty property : typeProperties) {
                if (property instanceof ElasticCsdlProperty) {
                    localPorperties.put(property.getName(),
                            new ElasticEdmProperty(edm, (ElasticCsdlProperty) property));
                }
            }
            propertiesCash = Collections.unmodifiableMap(localPorperties);
        }
        return propertiesCash;
    }

    @Override
    public Map<String, EdmProperty> getProperties() {
        Map<String, EdmProperty> properties = new HashMap<>();
        for (Entry<String, ElasticEdmProperty> entry : getEProperties().entrySet()) {
            properties.put(entry.getKey(), entry.getValue());
        }
        return properties;
    }

    /**
     * Gets Elasticsearch navigation properties map.
     * 
     * @return properties map, with key - property name and value -
     *         {@link ElasticEdmNavigationProperty} instance
     */
    public Map<String, ElasticEdmNavigationProperty> getENavigationProperties() {
        if (navigationPropertiesCash == null) {
            Map<String, ElasticEdmNavigationProperty> localNavigationProperties = new LinkedHashMap<>();
            List<CsdlNavigationProperty> structuredTypeNavigationProperties = csdlEntityType
                    .getNavigationProperties();
            for (CsdlNavigationProperty property : structuredTypeNavigationProperties) {
                if (property instanceof ElasticCsdlNavigationProperty) {
                    localNavigationProperties.put(property.getName(),
                            new ElasticEdmNavigationProperty(edm,
                                    (ElasticCsdlNavigationProperty) property));
                }
            }
            navigationPropertiesCash = Collections.unmodifiableMap(localNavigationProperties);
        }
        return navigationPropertiesCash;
    }

    @Override
    public Map<String, EdmNavigationProperty> getNavigationProperties() {
        Map<String, EdmNavigationProperty> navigationProperties = new HashMap<>();
        for (Entry<String, ElasticEdmNavigationProperty> entry : getENavigationProperties()
                .entrySet()) {
            navigationProperties.put(entry.getKey(), entry.getValue());
        }
        return navigationProperties;
    }

}
