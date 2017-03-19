package com.hevelian.olastic.core.elastic.requests;

import com.hevelian.olastic.core.edm.ElasticEdmEntitySet;
import com.hevelian.olastic.core.elastic.ESClient;
import com.hevelian.olastic.core.elastic.pagination.Pagination;
import com.hevelian.olastic.core.elastic.queries.SearchQuery;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.experimental.FieldDefaults;
import org.elasticsearch.action.search.MultiSearchResponse;

import java.util.List;

/**
 * Multi search request with search queries and pagination.
 * @author Taras Kohut
 */
@AllArgsConstructor
@Getter
@FieldDefaults(makeFinal = true, level = AccessLevel.PRIVATE)
public class MultiSearchRequest implements ESMultiRequest<SearchQuery> {
    List<SearchQuery> queries;
    ElasticEdmEntitySet entitySet;
    Pagination pagination;

    @Override
    public List<SearchQuery> getQueries() {
        return queries;
    }

    @Override
    public ElasticEdmEntitySet getEntitySet() {
        return entitySet;
    }

    @Override
    public MultiSearchResponse execute() {
        return ESClient.getInstance().executeRequest(getQueries());
    }
}
