package com.hevelian.olastic.core.elastic.requests;

import org.elasticsearch.action.search.SearchResponse;

import com.hevelian.olastic.core.edm.ElasticEdmEntitySet;
import com.hevelian.olastic.core.elastic.queries.Query;

/**
 * Interface to provide behavior for all requests implementations.
 * 
 * @author rdidyk
 */
public interface ESRequest {

    /**
     * Gets query.
     * 
     * @return query
     */
    Query getQuery();

    /**
     * Gets entity set.
     * 
     * @return the edm entity set
     */
    ElasticEdmEntitySet getEntitySet();

    /**
     * Executes request and returns search response.
     * 
     * @return found data
     */
    SearchResponse execute();

}