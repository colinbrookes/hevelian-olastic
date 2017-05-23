package com.hevelian.olastic.core.elastic.requests;

import com.hevelian.olastic.core.elastic.pagination.Pagination;

import org.apache.olingo.server.api.ODataApplicationException;
import org.elasticsearch.action.search.SearchResponse;

import com.hevelian.olastic.core.edm.ElasticEdmEntitySet;
import com.hevelian.olastic.core.elastic.queries.Query;

/**
 * Interface to provide behavior for all single requests implementations.
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
     * Gets pagination.
     *
     * @return pagination
     */
    Pagination getPagination();

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
     * @throws ODataApplicationException
     *             if any error appeared during executing request
     */
    SearchResponse execute() throws ODataApplicationException;

}