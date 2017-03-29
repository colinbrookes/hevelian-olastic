package com.hevelian.olastic.core.elastic.parsers;

import com.hevelian.olastic.core.edm.ElasticEdmEntitySet;
import com.hevelian.olastic.core.processors.data.InstanceData;
import org.apache.olingo.server.api.ODataApplicationException;
import org.elasticsearch.action.search.MultiSearchResponse;

import java.util.List;

/**
 * Interface to provide behavior for Elasticsearch response parsers.
 *
 * @author Taras Kohut
 *
 * @param <T>
 *            instance data type class
 * @param <V>
 *            instance data value class
 */
public interface ESMultiResponseParser<T, V> {
    /**
     * Parses Elasticsearch {@link MultiSearchResponse} and returns
     * {@link InstanceData} with type and value.
     *
     * @param response
     *            multi response from Elasticsearch
     * @param responseEntitySets
     *            entitySet list of the data returned from Elasticsearch
     * @param returnEntitySet
     *            entitySet that should be returned to the client
     * @return instance data with type and value
     * @throws ODataApplicationException
     *             if any error occurred during parsing response
     */
    InstanceData<T, V> parse(MultiSearchResponse response, List<ElasticEdmEntitySet> responseEntitySets, ElasticEdmEntitySet returnEntitySet)
            throws ODataApplicationException;
}
