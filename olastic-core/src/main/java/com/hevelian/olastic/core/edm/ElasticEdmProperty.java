package com.hevelian.olastic.core.edm;

import org.apache.olingo.commons.api.edm.Edm;
import org.apache.olingo.commons.api.edm.EdmProperty;
import org.apache.olingo.commons.core.edm.EdmPropertyImpl;

import com.hevelian.olastic.core.api.edm.provider.ElasticCsdlProperty;

/**
 * Custom implementation of {@link EdmProperty} to provide behavior from
 * {@link ElasticCsdlProperty} object.
 * 
 * @author rdidyk
 */
public class ElasticEdmProperty extends EdmPropertyImpl {

    private ElasticCsdlProperty csdlProperty;

    public ElasticEdmProperty(Edm edm, ElasticCsdlProperty property) {
        super(edm, property);
        this.csdlProperty = property;
    }

    /**
     * Get's field name in Elasticsearch.
     * 
     * @return field name
     */
    public String getEField() {
        return csdlProperty.getEField();
    }
}
