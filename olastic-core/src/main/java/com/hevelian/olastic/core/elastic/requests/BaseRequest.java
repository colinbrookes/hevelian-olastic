package com.hevelian.olastic.core.elastic.requests;

import org.apache.olingo.commons.api.ex.ODataRuntimeException;
import org.apache.olingo.server.api.ODataApplicationException;
import org.elasticsearch.action.search.SearchResponse;

import com.hevelian.olastic.core.edm.ElasticEdmEntitySet;
import com.hevelian.olastic.core.elastic.pagination.Pagination;
import com.hevelian.olastic.core.elastic.queries.Query;

import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * Base request class.
 * 
 * @author rdidyk
 */
@AllArgsConstructor
@Getter
public class BaseRequest implements ESRequest {

    private final Query query;
    private final ElasticEdmEntitySet entitySet;
    private final Pagination pagination;

    @Override
    public SearchResponse execute() throws ODataApplicationException {
        throw new ODataRuntimeException("Execute for BaseRequest instance is not allowed.");
    }
}
