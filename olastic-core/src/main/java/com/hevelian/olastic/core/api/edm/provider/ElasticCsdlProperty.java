package com.hevelian.olastic.core.api.edm.provider;

import org.apache.olingo.commons.api.edm.provider.CsdlProperty;

import com.google.common.base.Objects;

/**
 * Elasticsearch CSDL property implementation.
 * 
 * @author rdidyk
 */
public class ElasticCsdlProperty extends CsdlProperty
        implements ElasticCsdlEdmItem<ElasticCsdlProperty> {

    private String esIndex;
    private String esType;
    private String esField;

    public String getESField() {
        return esField;
    }

    /**
     * Sets elasticsearch field name.
     * 
     * @param esField
     *            field name
     * @return current instance
     */
    public ElasticCsdlProperty setESField(String esField) {
        this.esField = esField;
        return this;
    }

    @Override
    public String getESType() {
        return esType;
    }

    @Override
    public String getESIndex() {
        return esIndex;
    }

    @Override
    public ElasticCsdlProperty setESIndex(String esIndex) {
        this.esIndex = esIndex;
        return this;
    }

    @Override
    public ElasticCsdlProperty setESType(String esType) {
        this.esType = esType;
        return this;
    }

    @Override
    public CsdlProperty setName(String name) {
        // To avoid call setEField() in case names are the same.
        if (esField == null) {
            setESField(name);
        }
        return super.setName(name);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(esIndex, esType, esField, getName());
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
        ElasticCsdlProperty other = (ElasticCsdlProperty) obj;
        return Objects.equal(this.esIndex, other.esIndex) && Objects.equal(this.esType, other.esType)
                && Objects.equal(this.esField, other.esField)
                && Objects.equal(this.getName(), other.getName());
    }
}
