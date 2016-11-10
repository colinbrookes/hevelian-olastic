package com.hevelian.olastic.core.edm.provider;

import org.apache.olingo.commons.api.edm.provider.CsdlProperty;

/**
 * Elasticsearch CSDL property implementation.
 * 
 * @author rdidyk
 */
public class ElasticCsdlProperty extends CsdlProperty
        implements IElasticCsdlEdmItem<ElasticCsdlProperty> {

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

}
