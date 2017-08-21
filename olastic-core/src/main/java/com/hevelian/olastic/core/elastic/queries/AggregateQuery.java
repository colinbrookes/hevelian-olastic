package com.hevelian.olastic.core.elastic.queries;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.PipelineAggregationBuilder;

import lombok.Getter;
import lombok.NonNull;

/**
 * Aggregate query with simple and pipeline aggregations.
 * 
 * @author rdidyk
 */
@Getter
public class AggregateQuery extends Query {

    @NonNull
    private final List<AggregationBuilder> aggregations;
    private final List<PipelineAggregationBuilder> pipelineAggregations;

    /**
     * Constructor to initialize parameters.
     * 
     * @param index
     *            index name
     * @param types
     *            types name
     * @param queryBuilder
     *            main query builder
     * @param aggregations
     *            simple aggregations
     */
    public AggregateQuery(String index, String[] types, QueryBuilder queryBuilder,
            AggregationBuilder aggregations) {
        super(index, types, queryBuilder, null);
        this.aggregations = Arrays.asList(aggregations);
        this.pipelineAggregations = Collections.emptyList();
    }

    /**
     * Constructor to initialize parameters.
     * 
     * @param index
     *            index name
     * @param types
     *            types name
     * @param queryBuilder
     *            main query builder
     * @param aggregations
     *            simple aggregations
     * @param pipelineAggregations
     *            pipeline aggregations
     */
    public AggregateQuery(String index, String[] types, QueryBuilder queryBuilder,
            List<AggregationBuilder> aggregations,
            List<PipelineAggregationBuilder> pipelineAggregations) {
        super(index, types, queryBuilder, null);
        this.aggregations = aggregations;
        this.pipelineAggregations = pipelineAggregations;
    }

}