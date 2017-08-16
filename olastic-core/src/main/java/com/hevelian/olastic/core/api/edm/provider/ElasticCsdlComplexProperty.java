package com.hevelian.olastic.core.api.edm.provider;

import org.apache.olingo.commons.api.edm.provider.CsdlProperty;

import com.google.common.base.Objects;

/**
 * Elasticsearch CSDL property for complex type implementation.
 * 
 * @author rdidyk
 */
public class ElasticCsdlComplexProperty extends CsdlProperty
        implements ElasticCsdlEdmItem<ElasticCsdlComplexProperty> {

    private String esIndex;
    private String esType;
    private String esNestedType;

    @Override
    public String getESIndex() {
        return esIndex;
    }

    @Override
    public String getESType() {
        return esType;
    }

    public String getESNestedType() {
        return esNestedType;
    }

    @Override
    public ElasticCsdlComplexProperty setESIndex(String esIndex) {
        this.esIndex = esIndex;
        return this;
    }

    @Override
    public ElasticCsdlComplexProperty setESType(String esType) {
        this.esType = esType;
        return this;
    }

    /**
     * Sets elasticsearch nested type name.
     * 
     * @param esNestedType
     *            nested type name
     * @return current instance
     */
    public ElasticCsdlComplexProperty setESNestedType(String esNestedType) {
        this.esNestedType = esNestedType;
        return this;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(esIndex, esNestedType, getName());
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        ElasticCsdlComplexProperty other = (ElasticCsdlComplexProperty) obj;
        return Objects.equal(this.esIndex, other.esIndex)
                && Objects.equal(this.esNestedType, other.esNestedType)
                && Objects.equal(this.getName(), other.getName());
    }

}
