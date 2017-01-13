package com.hevelian.olastic.core.elastic.requests;

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

    @Override
    public SearchResponse execute() {
        throw new ODataRuntimeException("Execure for BaseRequest instance is not allowed.");
    }

}
