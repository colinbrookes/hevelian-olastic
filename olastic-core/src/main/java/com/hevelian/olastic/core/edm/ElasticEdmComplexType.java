package com.hevelian.olastic.core.edm;

import java.util.Map.Entry;

import org.apache.olingo.commons.api.edm.Edm;
import org.apache.olingo.commons.api.edm.EdmElement;
import org.apache.olingo.commons.api.edm.EdmProperty;
import org.apache.olingo.commons.api.edm.EdmType;
import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.apache.olingo.commons.core.edm.EdmComplexTypeImpl;

import com.hevelian.olastic.core.api.edm.provider.ElasticCsdlComplexType;

/**
 * Custom implementation of {@link EdmComplexTypeImpl}.
 * 
 * @author rdidyk
 */
public class ElasticEdmComplexType extends EdmComplexTypeImpl {

    private ElasticCsdlComplexType csdlComplexType;

    /**
     * Constructor to initialize entity type.
     * 
     * @param edm
     *            EDM provider
     * @param name
     *            complex type FQN
     * @param complexType
     *            CSDL complex type
     */
    public ElasticEdmComplexType(Edm edm, FullQualifiedName name,
            ElasticCsdlComplexType complexType) {
        super(edm, name, complexType);
        this.csdlComplexType = complexType;
    }

    /**
     * Get's index name in Elasticsearch.
     * 
     * @return index name
     */
    public String getEIndex() {
        return csdlComplexType.getESIndex();
    }

    /**
     * Get's type name in Elasticsearch.
     * 
     * @return type name
     */
    public String getEType() {
        return csdlComplexType.getESType();
    }

    /**
     * Get's type name in Elasticsearch.
     * 
     * @return type name
     */
    public String getENestedType() {
        return csdlComplexType.getENestedType();
    }

    /**
     * Get's property by nested property name.
     * 
     * @param nestedName
     *            nested property name
     * @return found property, or null
     */
    public EdmElement getPropertyByNestedName(String nestedName) {
        for (Entry<String, EdmProperty> entry : getProperties().entrySet()) {
            EdmType type = entry.getValue().getType();
            if (type instanceof ElasticEdmComplexType
                    && ((ElasticEdmComplexType) type).getENestedType().equals(nestedName)) {
                return entry.getValue();
            }
        }
        return null;
    }

}
