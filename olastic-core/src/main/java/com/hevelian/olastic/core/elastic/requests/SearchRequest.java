package com.hevelian.olastic.core.elastic.requests;

import com.hevelian.olastic.core.edm.ElasticEdmEntitySet;
import com.hevelian.olastic.core.elastic.ESClient;
import com.hevelian.olastic.core.elastic.pagination.Pagination;
import com.hevelian.olastic.core.elastic.queries.SearchQuery;
import org.elasticsearch.action.search.SearchResponse;

/**
 * Search request with search query and pagination.
 * 
 * @author rdidyk
 */
public class SearchRequest extends BaseRequest {

    /**
     * Constructor to initialize values.
     * 
     * @param query
     *            search query
     * @param entitySet
     *            the edm entity set
     * @param pagination
     *            pagination
     */
    public SearchRequest(SearchQuery query, ElasticEdmEntitySet entitySet, Pagination pagination) {
        super(query, entitySet, pagination);
    }

    @Override
    public SearchResponse execute() {
        return ESClient.getInstance().executeRequest(getQuery());
    }

    @Override
    public SearchQuery getQuery() {
        return (SearchQuery) super.getQuery();
    }

}
