package com.hevelian.olastic.core.elastic.queries;

import com.hevelian.olastic.core.elastic.pagination.Pagination;
import lombok.*;
import lombok.experimental.FieldDefaults;
import org.elasticsearch.index.query.QueryBuilder;

/**
 * Query with base parameters for all queries to create request.
 * 
 * @author rdidyk
 */
@AllArgsConstructor
@Getter
@Setter
@FieldDefaults(level = AccessLevel.PRIVATE)
public class Query {

    @NonNull
    String index;
    @NonNull
    String[] types;
    @NonNull
    QueryBuilder queryBuilder;
    Pagination pagination;

}
