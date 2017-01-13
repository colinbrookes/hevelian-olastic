package com.hevelian.olastic.core.elastic.parsers;

import org.elasticsearch.action.search.SearchResponse;

import com.hevelian.olastic.core.edm.ElasticEdmEntitySet;
import com.hevelian.olastic.core.processors.data.InstanceData;

/**
 * Interface to provide behavior for Elasticsearch response parsers.
 * 
 * @author rdidyk
 *
 * @param <T>
 *            instance data type class
 * @param <V>
 *            instance data value class
 */
public interface ESResponseParser<T, V> {

    /**
     * Parses Elasticsearch {@link SearchResponse} and returns
     * {@link InstanceData} with type and value.
     * 
     * @param response
     *            response from Elasticsearch
     * @param entitySet
     *            the edm entity set
     * @return instance data with type and value
     */
    InstanceData<T, V> parse(SearchResponse response, ElasticEdmEntitySet entitySet);

}
