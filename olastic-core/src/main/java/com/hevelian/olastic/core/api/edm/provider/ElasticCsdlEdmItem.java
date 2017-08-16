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
public interface ElasticCsdlEdmItem<T extends ElasticCsdlEdmItem<T>> {

    /**
     * Return's Elasticsearch type name.
     * 
     * @return type name
     */
    String getESType();

    /**
     * Return's Elasticsearch index name.
     * 
     * @return index name
     */
    String getESIndex();

    /**
     * Set's Elasticsearch index name.
     * 
     * @param esIndex
     *            index name
     * @return current instance
     */
    T setESIndex(String esIndex);

    /**
     * Set's Elasticsearch type name.
     * 
     * @param esType
     *            type name
     * @return current instance
     */
    T setESType(String esType);

}
