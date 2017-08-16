package com.hevelian.olastic.core.elastic.parsers;

import com.hevelian.olastic.core.edm.ElasticEdmEntitySet;
import com.hevelian.olastic.core.processors.data.InstanceData;
import org.apache.olingo.server.api.ODataApplicationException;
import org.elasticsearch.action.search.SearchResponse;

import java.util.List;

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
     * @throws ODataApplicationException
     *             if any error occurred during parsing response
     */
    InstanceData<T, V> parse(SearchResponse response, ElasticEdmEntitySet entitySet)
            throws ODataApplicationException;

    /**
     * Parses Elasticsearch {@link SearchResponse} and returns
     * {@link InstanceData} with type and value.
     *
     * @param response
     *            response from Elasticsearch
     * @param responseEntitySets
     *            entitySet list of the data returned from Elasticsearch
     * @param returnEntitySet
     *            entitySet that should be returned to the client
     * @return instance data with type and value
     * @throws ODataApplicationException
     *             if any error occurred during parsing response
     */
    InstanceData<T, V> parse(SearchResponse response, List<ElasticEdmEntitySet> responseEntitySets,
            ElasticEdmEntitySet returnEntitySet) throws ODataApplicationException;

}
