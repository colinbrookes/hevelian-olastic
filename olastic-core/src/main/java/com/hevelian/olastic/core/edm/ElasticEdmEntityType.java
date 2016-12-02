package com.hevelian.olastic.core.edm;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.olingo.commons.api.edm.Edm;
import org.apache.olingo.commons.api.edm.EdmEntityType;
import org.apache.olingo.commons.api.edm.EdmProperty;
import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.apache.olingo.commons.api.edm.provider.CsdlProperty;
import org.apache.olingo.commons.core.edm.EdmEntityTypeImpl;

import com.hevelian.olastic.core.api.edm.provider.ElasticCsdlEntityType;
import com.hevelian.olastic.core.api.edm.provider.ElasticCsdlProperty;

/**
 * Custom implementation of {@link EdmEntityType}.
 * 
 * @author rdidyk
 */
public class ElasticEdmEntityType extends EdmEntityTypeImpl {

    private ElasticCsdlEntityType csdlEntityType;
    private Map<String, ElasticEdmProperty> propertiesCash;

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
        return csdlEntityType.getEIndex();
    }

    /**
     * Get's type name in Elasticsearch.
     * 
     * @return type name
     */
    public String getEType() {
        return csdlEntityType.getEType();
    }

    /**
     * Get's CSDL property name by Elasticsearch field name.
     * 
     * @param eFieldName
     *            Elasticsearch field name
     * @return found property
     */
    public ElasticEdmProperty findPropertyByEField(String eFieldName) {
        for (Entry<String, ElasticEdmProperty> entry : getEProperties().entrySet()) {
            ElasticEdmProperty property = entry.getValue();
            if (property.getEField().equals(eFieldName)) {
                return property;
            }
        }
        return null;
    }

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

}
