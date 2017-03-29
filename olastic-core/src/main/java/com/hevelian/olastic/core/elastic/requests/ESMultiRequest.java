package com.hevelian.olastic.core.elastic.requests;

import com.hevelian.olastic.core.edm.ElasticEdmEntitySet;
import com.hevelian.olastic.core.elastic.pagination.Pagination;
import com.hevelian.olastic.core.elastic.queries.Query;
import org.elasticsearch.action.search.MultiSearchResponse;

import java.util.List;

/**
 * Interface to provide behavior for all multiple request implementations.
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
     */
    MultiSearchResponse execute();
}
