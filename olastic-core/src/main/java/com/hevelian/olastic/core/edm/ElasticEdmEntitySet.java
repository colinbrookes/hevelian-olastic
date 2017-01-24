package com.hevelian.olastic.core.edm;

import org.apache.olingo.commons.api.edm.EdmEntityContainer;
import org.apache.olingo.commons.api.edm.EdmEntityType;
import org.apache.olingo.commons.core.edm.EdmEntitySetImpl;

import com.hevelian.olastic.core.api.edm.provider.ElasticCsdlEntitySet;

/**
 * Custom implementation of {@link EdmEntityType}.
 * 
 * @author rdidyk
 */
public class ElasticEdmEntitySet extends EdmEntitySetImpl {

    private ElasticCsdlEntitySet csdlEntitySet;
    private ElasticEdmProvider provider;

    public ElasticEdmEntitySet(ElasticEdmProvider provider, EdmEntityContainer container,
            ElasticCsdlEntitySet entitySet) {
        super(provider, container, entitySet);
        this.provider = provider;
        this.csdlEntitySet = entitySet;
    }

    /**
     * Get's index name in Elasticsearch.
     * 
     * @return index name
     */
    public String getEIndex() {
        return csdlEntitySet.getEIndex();
    }

    /**
     * Get's type name in Elasticsearch.
     * 
     * @return type name
     */
    public String getEType() {
        return csdlEntitySet.getEType();
    }

    @Override
    public ElasticEdmEntityType getEntityType() {
        return (ElasticEdmEntityType) provider.getEntityType(csdlEntitySet.getTypeFQN());
    }

    /**
     * Sets index to entity set.
     * @param eIndex
     */
    public void setEIndex(String eIndex) {
        csdlEntitySet.setEIndex(eIndex);
    }

    /**
     * Sets type to entity set.
     * @param eType
     */
    public void setEType(String eType) {
        csdlEntitySet.setEType(eType);
    }
}
