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

    private String eIndex;
    private String eType;
    private String eNestedType;

    @Override
    public String getEType() {
        return eType;
    }

    @Override
    public String getEIndex() {
        return eIndex;
    }

    public String geteNestedType() {
        return eNestedType;
    }

    @Override
    public ElasticCsdlComplexType setEIndex(String eIndex) {
        this.eIndex = eIndex;
        return this;
    }

    @Override
    public ElasticCsdlComplexType setEType(String eType) {
        this.eType = eType;
        return this;
    }

    public ElasticCsdlComplexType setENestedType(String eNestedType) {
        this.eNestedType = eNestedType;
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
