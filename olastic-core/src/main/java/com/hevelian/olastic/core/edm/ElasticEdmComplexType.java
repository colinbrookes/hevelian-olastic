package com.hevelian.olastic.core.edm;

import org.apache.olingo.commons.api.edm.Edm;
import org.apache.olingo.commons.api.edm.EdmComplexType;
import org.apache.olingo.commons.api.edm.EdmElement;
import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.apache.olingo.commons.core.edm.EdmComplexTypeImpl;

import com.hevelian.olastic.core.api.edm.provider.ElasticCsdlComplexType;

/**
 * Custom implementation of {@link EdmComplexType}.
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
        return csdlComplexType.getEIndex();
    }

    /**
     * Get's type name in Elasticsearch.
     * 
     * @return type name
     */
    public String getEType() {
        return csdlComplexType.getEType();
    }

    /**
     * Get's type name in Elasticsearch.
     * 
     * @return type name
     */
    public String getENestedType() {
        return csdlComplexType.geteNestedType();
    }

    /**
     * Get's property by nested property name.
     * 
     * @param nestedName
     *            nested property name
     * @return found property, or null
     */
    public EdmElement getPropertyByNestedName(String nestedName) {
        return getProperties().entrySet().stream()
                .filter(entry -> ((ElasticEdmComplexType) entry.getValue().getType())
                        .getENestedType().equals(nestedName))
                .findFirst().get().getValue();
    }

}
