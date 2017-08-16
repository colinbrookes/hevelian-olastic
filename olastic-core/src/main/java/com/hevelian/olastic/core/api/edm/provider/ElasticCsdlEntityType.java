package com.hevelian.olastic.core.api.edm.provider;

import org.apache.olingo.commons.api.edm.provider.CsdlEntityType;

/**
 * Elasticsearch Entity Type implementation.
 * 
 * @author rdidyk
 */
public class ElasticCsdlEntityType extends CsdlEntityType
        implements ElasticCsdlEdmItem<ElasticCsdlEntityType> {

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
    public ElasticCsdlEntityType setESIndex(String esIndex) {
        this.esIndex = esIndex;
        return this;
    }

    @Override
    public ElasticCsdlEntityType setESType(String esType) {
        this.esType = esType;
        return this;
    }

    @Override
    public CsdlEntityType setName(String name) {
        if (esType == null) {
            setESType(name);
        }
        return super.setName(name);
    }

}
