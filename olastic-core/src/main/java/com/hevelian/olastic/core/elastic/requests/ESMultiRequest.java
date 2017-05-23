package com.hevelian.olastic.core.elastic.requests;

import java.util.List;

import org.apache.olingo.server.api.ODataApplicationException;
import org.elasticsearch.action.search.MultiSearchResponse;

import com.hevelian.olastic.core.edm.ElasticEdmEntitySet;
import com.hevelian.olastic.core.elastic.pagination.Pagination;
import com.hevelian.olastic.core.elastic.queries.Query;

/**
 * Interface to provide behavior for all multiple request implementations.
 * 
 * @author Taras Kohut
 */
public interface ESMultiRequest<T extends Query> {
    /**
     * Gets list of queries.
     *
     * @return query
     */
    List<T> getQueries();

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
    MultiSearchResponse execute() throws ODataApplicationException;
}
