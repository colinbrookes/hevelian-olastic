package com.hevelian.olastic.core.api.edm.provider;

import org.apache.olingo.commons.api.edm.provider.CsdlComplexType;

import com.google.common.base.Objects;

/**
 * Elasticsearch CSDL complex type implementation.
 * 
 * @author rdidyk
 */
public class ElasticCsdlComplexType extends CsdlComplexType
        implements ElasticCsdlEdmItem<ElasticCsdlComplexType> {

    private String esIndex;
    private String esType;
    private String esNestedType;

    @Override
    public String getESType() {
        return esType;
    }

    @Override
    public String getESIndex() {
        return esIndex;
    }

    public String getENestedType() {
        return esNestedType;
    }

    @Override
    public ElasticCsdlComplexType setESIndex(String esIndex) {
        this.esIndex = esIndex;
        return this;
    }

    @Override
    public ElasticCsdlComplexType setESType(String esType) {
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
    public ElasticCsdlComplexType setENestedType(String esNestedType) {
        this.esNestedType = esNestedType;
        return this;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(getName());
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
        ElasticCsdlComplexType other = (ElasticCsdlComplexType) obj;
        return Objects.equal(this.getName(), other.getName());
    }

}
