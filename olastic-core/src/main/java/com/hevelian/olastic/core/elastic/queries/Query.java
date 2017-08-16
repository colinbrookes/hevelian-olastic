package com.hevelian.olastic.core.elastic.queries;

import org.elasticsearch.index.query.QueryBuilder;

import com.hevelian.olastic.core.elastic.pagination.Pagination;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;

/**
 * Query with base parameters for all queries to create request.
 * 
 * @author rdidyk
 */
@AllArgsConstructor
@Getter
@Setter
public class Query {

    @NonNull
    private String index;
    @NonNull
    private String[] types;
    @NonNull
    private QueryBuilder queryBuilder;
    private Pagination pagination;

}
