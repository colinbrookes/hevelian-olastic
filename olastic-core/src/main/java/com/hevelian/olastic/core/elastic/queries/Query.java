package com.hevelian.olastic.core.elastic.queries;

import org.elasticsearch.index.query.QueryBuilder;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.FieldDefaults;

/**
 * Query with base parameters for all queries to create request.
 * 
 * @author rdidyk
 */
@AllArgsConstructor
@Getter
@FieldDefaults(makeFinal = true, level = AccessLevel.PRIVATE)
public class Query {

    @NonNull
    String index;
    @NonNull
    String[] types;
    @NonNull
    QueryBuilder queryBuilder;

}
