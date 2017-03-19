package com.hevelian.olastic.core.elastic.queries;

import com.hevelian.olastic.core.elastic.pagination.Pagination;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.FieldDefaults;
import org.elasticsearch.index.query.QueryBuilder;

import java.util.Set;

/**
 * Search query with fields.
 * 
 * @author rdidyk
 */
@Getter
@FieldDefaults(makeFinal = true, level = AccessLevel.PRIVATE)
public class SearchQuery extends Query {

    @NonNull
    Set<String> fields;

    /**
     * Constructor to initialize parameters.
     * 
     * @param index
     *            index name
     * @param types
     *            types name
     * @param queryBuilder
     *            main query builder
     * @param fields
     *            fields to search
     * @param pagination
     *            pagination
     */
    public SearchQuery(String index, String[] types, QueryBuilder queryBuilder,
            Set<String> fields, Pagination pagination) {
        super(index, types, queryBuilder, pagination);
        this.fields = fields;
    }

}