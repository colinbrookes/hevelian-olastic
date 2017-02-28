package com.hevelian.olastic.core.elastic.requests;

import org.elasticsearch.action.search.SearchResponse;

import com.hevelian.olastic.core.edm.ElasticEdmEntitySet;
import com.hevelian.olastic.core.elastic.ESClient;
import com.hevelian.olastic.core.elastic.pagination.Pagination;
import com.hevelian.olastic.core.elastic.queries.AggregateQuery;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.experimental.FieldDefaults;

/**
 * Aggregate request with aggregate query and count alias.
 * 
 * @author rdidyk
 */
@Getter
@FieldDefaults(makeFinal = true, level = AccessLevel.PRIVATE)
public class AggregateRequest extends BaseRequest {

    Pagination pagination;
    String countAlias;

    /**
     * Constructor to initialize query and entity.
     *
     * @param query
     *            aggregate query
     * @param entitySet
     *            the edm entity set
     */
    public AggregateRequest(AggregateQuery query, ElasticEdmEntitySet entitySet) {
        this(query, entitySet, null, null);
    }

    /**
     * Constructor to initialize query and entity.
     *
     * @param query
     *            aggregate query
     * @param entitySet
     *            the edm entity set
     * @param countAlias
     *            name of count alias, or null if no count option applied
     */
    public AggregateRequest(AggregateQuery query, ElasticEdmEntitySet entitySet,
            String countAlias) {
        this(query, entitySet, null, countAlias);
    }

    /**
     * Constructor to initialize query and entity.
     *
     * @param query
     *            aggregate query
     * @param entitySet
     *            the edm entity set
     * @param pagination
     *            pagination information
     */
    public AggregateRequest(AggregateQuery query, ElasticEdmEntitySet entitySet,
            Pagination pagination) {
        this(query, entitySet, pagination, null);
    }

    /**
     * Constructor to initialize query and entity.
     * 
     * @param query
     *            aggregate query
     * @param entitySet
     *            the edm entity set
     * @param pagination
     *            pagination information
     * @param countAlias
     *            name of count alias, or null if no count option applied
     */
    public AggregateRequest(AggregateQuery query, ElasticEdmEntitySet entitySet,
            Pagination pagination, String countAlias) {
        super(query, entitySet);
        this.pagination = pagination;
        this.countAlias = countAlias;
    }

    @Override
    public SearchResponse execute() {
        return ESClient.getInstance().executeRequest(getQuery());
    }

    @Override
    public AggregateQuery getQuery() {
        return (AggregateQuery) super.getQuery();
    }

}
