package com.hevelian.olastic.core.elastic.requests;

import org.elasticsearch.action.search.SearchResponse;

import com.hevelian.olastic.core.edm.ElasticEdmEntitySet;
import com.hevelian.olastic.core.elastic.ESClient;
import com.hevelian.olastic.core.elastic.pagination.Pagination;
import com.hevelian.olastic.core.elastic.queries.SearchQuery;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.experimental.FieldDefaults;

/**
 * Search request with search query and pagination.
 * 
 * @author rdidyk
 */
@Getter
@FieldDefaults(makeFinal = true, level = AccessLevel.PRIVATE)
public class SearchRequest extends BaseRequest {

    Pagination pagination;

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
        super(query, entitySet);
        this.pagination = pagination;
    }

    @Override
    public SearchResponse execute() {
        return ESClient.getInstance().executeRequest(getQuery(), getPagination());
    }

    @Override
    public SearchQuery getQuery() {
        return (SearchQuery) super.getQuery();
    }

}
