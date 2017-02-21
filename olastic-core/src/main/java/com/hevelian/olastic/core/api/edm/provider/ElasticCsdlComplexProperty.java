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

    private String eIndex;
    private String eType;
    private String eNestedType;

    @Override
    public String getEIndex() {
        return eIndex;
    }

    @Override
    public String getEType() {
        return eType;
    }

    public String getENestedType() {
        return eNestedType;
    }

    @Override
    public ElasticCsdlComplexProperty setEIndex(String eIndex) {
        this.eIndex = eIndex;
        return this;
    }

    @Override
    public ElasticCsdlComplexProperty setEType(String eType) {
        this.eType = eType;
        return this;
    }

    public ElasticCsdlComplexProperty setENestedType(String eNestedType) {
        this.eNestedType = eNestedType;
        return this;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(eIndex, eNestedType, getName());
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
        return Objects.equal(this.eIndex, other.eIndex)
                && Objects.equal(this.eNestedType, other.eNestedType)
                && Objects.equal(this.getName(), other.getName());
    }

}
