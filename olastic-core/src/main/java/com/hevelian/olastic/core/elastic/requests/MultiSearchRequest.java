package com.hevelian.olastic.core.elastic.requests;

import java.util.List;

import org.apache.olingo.server.api.ODataApplicationException;
import org.elasticsearch.action.search.MultiSearchResponse;

import com.hevelian.olastic.core.edm.ElasticEdmEntitySet;
import com.hevelian.olastic.core.elastic.ESClient;
import com.hevelian.olastic.core.elastic.pagination.Pagination;
import com.hevelian.olastic.core.elastic.queries.SearchQuery;

import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * Multi search request with search queries and pagination.
 * 
 * @author Taras Kohut
 */
@AllArgsConstructor
@Getter
public class MultiSearchRequest implements ESMultiRequest<SearchQuery> {

    private final List<SearchQuery> queries;
    private final ElasticEdmEntitySet entitySet;
    private final Pagination pagination;

    @Override
    public List<SearchQuery> getQueries() {
        return queries;
    }

    @Override
    public ElasticEdmEntitySet getEntitySet() {
        return entitySet;
    }

    @Override
    public MultiSearchResponse execute() throws ODataApplicationException {
        return ESClient.getInstance().executeRequest(getQueries());
    }
}
