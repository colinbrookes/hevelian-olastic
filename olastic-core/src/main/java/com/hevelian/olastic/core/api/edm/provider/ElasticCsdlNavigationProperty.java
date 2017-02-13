package com.hevelian.olastic.core.api.edm.provider;

import org.apache.olingo.commons.api.edm.provider.CsdlNavigationProperty;

/**
 * Elasticsearch Navigation Property implementation.
 * 
 * @author rdidyk
 */
public class ElasticCsdlNavigationProperty extends CsdlNavigationProperty
        implements ElasticCsdlEdmItem<ElasticCsdlNavigationProperty> {

    private String eIndex;
    private String eType;

    @Override
    public String getEType() {
        return eType;
    }

    @Override
    public String getEIndex() {
        return eIndex;
    }

    @Override
    public ElasticCsdlNavigationProperty setEIndex(String eIndex) {
        this.eIndex = eIndex;
        return this;
    }

    @Override
    public ElasticCsdlNavigationProperty setEType(String eType) {
        this.eType = eType;
        return this;
    }

}
