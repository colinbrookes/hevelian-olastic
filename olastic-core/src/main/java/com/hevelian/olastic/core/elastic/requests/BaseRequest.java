package com.hevelian.olastic.core.elastic.requests;

import com.hevelian.olastic.core.elastic.pagination.Pagination;
import org.apache.olingo.commons.api.ex.ODataRuntimeException;
import org.elasticsearch.action.search.SearchResponse;

import com.hevelian.olastic.core.edm.ElasticEdmEntitySet;
import com.hevelian.olastic.core.elastic.queries.Query;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.experimental.FieldDefaults;

/**
 * Base request class.
 * 
 * @author rdidyk
 */
@AllArgsConstructor
@Getter
@FieldDefaults(makeFinal = true, level = AccessLevel.PRIVATE)
public class BaseRequest implements ESRequest {
    Query query;
    ElasticEdmEntitySet entitySet;
    Pagination pagination;

    @Override
    public SearchResponse execute() {
        throw new ODataRuntimeException("Execute for BaseRequest instance is not allowed.");
    }

}
