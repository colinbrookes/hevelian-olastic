package com.hevelian.olastic.core.api.edm.provider;

import org.apache.olingo.commons.api.edm.provider.CsdlNavigationProperty;

/**
 * Elasticsearch Navigation Property implementation.
 * 
 * @author rdidyk
 */
public class ElasticCsdlNavigationProperty extends CsdlNavigationProperty
        implements ElasticCsdlEdmItem<ElasticCsdlNavigationProperty> {

    private String esIndex;
    private String esType;

    @Override
    public String getESType() {
        return esType;
    }

    @Override
    public String getESIndex() {
        return esIndex;
    }

    @Override
    public ElasticCsdlNavigationProperty setESIndex(String esIndex) {
        this.esIndex = esIndex;
        return this;
    }

    @Override
    public ElasticCsdlNavigationProperty setESType(String esType) {
        this.esType = esType;
        return this;
    }

}
