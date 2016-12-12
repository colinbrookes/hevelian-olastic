package com.hevelian.olastic.core.api.edm.provider;

import org.apache.olingo.commons.api.edm.provider.CsdlEntityType;

/**
 * Elasticsearch Entity Type implementation.
 * 
 * @author rdidyk
 */
public class ElasticCsdlEntityType extends CsdlEntityType
        implements IElasticCsdlEdmItem<ElasticCsdlEntityType> {

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
    public ElasticCsdlEntityType setEIndex(String eIndex) {
        this.eIndex = eIndex;
        return this;
    }

    @Override
    public ElasticCsdlEntityType setEType(String eType) {
        this.eType = eType;
        return this;
    }

    @Override
    public CsdlEntityType setName(String name) {
        if (eType == null) {
            setEType(name);
        }
        return super.setName(name);
    }

}
