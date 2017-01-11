package com.hevelian.olastic.core.elastic.queries;

import java.util.List;

import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.PipelineAggregationBuilder;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.FieldDefaults;

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
     * @param type
     *            type name
     * @param queryBuilder
     *            main query builder
     * @param aggregations
     *            simple aggregations
     * @param pipelineAggregations
     *            pipeline aggregations
     */
    public AggregateQuery(String index, String type, QueryBuilder queryBuilder,
            List<AggregationBuilder> aggregations,
            List<PipelineAggregationBuilder> pipelineAggregations) {
        super(index, type, queryBuilder);
        this.aggregations = aggregations;
        this.pipelineAggregations = pipelineAggregations;
    }

}