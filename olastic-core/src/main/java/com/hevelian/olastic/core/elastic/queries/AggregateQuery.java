package com.hevelian.olastic.core.elastic.queries;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.FieldDefaults;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.PipelineAggregationBuilder;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Aggregate query with simple and pipeline aggregations.
 * 
 * @author rdidyk
 */
@Getter
@FieldDefaults(makeFinal = true, level = AccessLevel.PRIVATE)
public class AggregateQuery extends Query {

    @NonNull
    List<AggregationBuilder> aggregations;
    List<PipelineAggregationBuilder> pipelineAggregations;

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
        this(index, types, queryBuilder, Arrays.asList(aggregations), Collections.emptyList());
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