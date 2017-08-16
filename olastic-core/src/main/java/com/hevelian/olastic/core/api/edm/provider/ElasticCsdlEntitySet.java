package com.hevelian.olastic.core.api.edm.provider;

import org.apache.olingo.commons.api.edm.provider.CsdlEntitySet;

/**
 * Elasticsearch Entity Set implementation.
 * 
 * @author rdidyk
 */
public class ElasticCsdlEntitySet extends CsdlEntitySet
        implements ElasticCsdlEdmItem<ElasticCsdlEntitySet> {

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
    public ElasticCsdlEntitySet setESIndex(String esIndex) {
        this.esIndex = esIndex;
        return this;
    }

    @Override
    public ElasticCsdlEntitySet setESType(String esType) {
        this.esType = esType;
        return this;
    }

    @Override
    public CsdlEntitySet setName(String name) {
        if (esType == null) {
            setESType(name);
        }
        return super.setName(name);
    }
}
