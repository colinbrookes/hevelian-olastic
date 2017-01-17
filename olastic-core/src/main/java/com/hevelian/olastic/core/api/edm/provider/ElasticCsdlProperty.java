package com.hevelian.olastic.core.api.edm.provider;

import org.apache.olingo.commons.api.edm.provider.CsdlProperty;

/**
 * Elasticsearch CSDL property implementation.
 * 
 * @author rdidyk
 */
public class ElasticCsdlProperty extends CsdlProperty
        implements ElasticCsdlEdmItem<ElasticCsdlProperty> {

    private String eIndex;
    private String eType;
    private String eField;

    public String getEField() {
        return eField;
    }

    public ElasticCsdlProperty setEField(String eField) {
        this.eField = eField;
        return this;
    }

    @Override
    public String getEType() {
        return eType;
    }

    @Override
    public String getEIndex() {
        return eIndex;
    }

    @Override
    public ElasticCsdlProperty setEIndex(String eIndex) {
        this.eIndex = eIndex;
        return this;
    }

    @Override
    public ElasticCsdlProperty setEType(String eType) {
        this.eType = eType;
        return this;
    }

    @Override
    public CsdlProperty setName(String name) {
        // To avoid call setEField() in case names are the same.
        if (eField == null) {
            setEField(name);
        }
        return super.setName(name);
    }

    //TODO Use guava hash code instead.
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((eField == null) ? 0 : eField.hashCode());
        result = prime * result + ((eIndex == null) ? 0 : eIndex.hashCode());
        result = prime * result + ((eType == null) ? 0 : eType.hashCode());
        return result;
    }

    //TODO Use guava equals code instead.
    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        ElasticCsdlProperty other = (ElasticCsdlProperty) obj;
        if (eField == null) {
            if (other.eField != null)
                return false;
        } else if (!eField.equals(other.eField))
            return false;
        if (eIndex == null) {
            if (other.eIndex != null)
                return false;
        } else if (!eIndex.equals(other.eIndex))
            return false;
        if (eType == null) {
            if (other.eType != null)
                return false;
        } else if (!eType.equals(other.eType))
            return false;
        return true;
    }

}
