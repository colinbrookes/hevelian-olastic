package com.hevelian.olastic.core.api.edm.provider;

/**
 * Interface to provide behavior for CSDL Entity model items with Elasticsearch
 * properties.
 * 
 * @author rdidyk
 *
 * @param <T>
 *            EDM Item type
 */
public interface IElasticCsdlEdmItem<T extends IElasticCsdlEdmItem<T>> {

    /**
     * Return's Elasticsearch type name.
     * 
     * @return type name
     */
    String getEType();

    /**
     * Return's Elasticsearch index name.
     * 
     * @return index name
     */
    String getEIndex();

    /**
     * Set's Elasticsearch index name.
     * 
     * @param eIndex
     *            index name
     * @return current instance
     */
    T setEIndex(String eIndex);

    /**
     * Set's Elasticsearch type name.
     * 
     * @param eType
     *            type name
     * @return current instance
     */
    T setEType(String eType);

}
