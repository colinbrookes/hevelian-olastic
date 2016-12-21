package com.hevelian.olastic.core.elastic.utils;

import static org.elasticsearch.search.aggregations.AggregationBuilders.avg;
import static org.elasticsearch.search.aggregations.AggregationBuilders.max;
import static org.elasticsearch.search.aggregations.AggregationBuilders.min;
import static org.elasticsearch.search.aggregations.AggregationBuilders.sum;
import static org.elasticsearch.search.aggregations.pipeline.PipelineAggregatorBuilders.avgBucket;
import static org.elasticsearch.search.aggregations.pipeline.PipelineAggregatorBuilders.maxBucket;
import static org.elasticsearch.search.aggregations.pipeline.PipelineAggregatorBuilders.minBucket;
import static org.elasticsearch.search.aggregations.pipeline.PipelineAggregatorBuilders.sumBucket;

import org.apache.commons.lang3.StringUtils;
import org.apache.olingo.commons.api.ex.ODataRuntimeException;
import org.apache.olingo.server.api.uri.queryoption.apply.AggregateExpression.StandardMethod;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.PipelineAggregationBuilder;

/**
 * Utility class with helper methods to work with aggregations.
 * 
 * @author rdidyk
 */
public final class AggregationUtils {

    /** Buckets path separator in Pipeline Aggregation. */
    public static final String BUCKETS_PATH_SEPARATOR = ">";

    private AggregationUtils() {
    }

    /**
     * Create's aggregation query based on {@link StandardMethod}.
     * 
     * @param method
     *            method
     * @param name
     *            agg name
     * @param field
     *            agg field
     * @return created aggregation
     */
    public static AggregationBuilder getAggQuery(StandardMethod method, String name, String field) {
        switch (method) {
        case SUM:
            return sum(name).field(field);
        case MAX:
            return max(name).field(field);
        case MIN:
            return min(name).field(field);
        case AVERAGE:
            return avg(name).field(field);
        default:
            throw new ODataRuntimeException(
                    String.format("Aggregate method '%s' is not supported yet.", method));
        }
    }

    /**
     * Create's pipeline aggregation query based on {@link StandardMethod}.
     * 
     * @param method
     *            method
     * @param name
     *            agg name
     * @param path
     *            buckets path
     * @return created aggregation
     */
    public static PipelineAggregationBuilder getPipelineAggQuery(StandardMethod method, String name,
            String... path) {
        String bucketsPath = StringUtils.join(path, BUCKETS_PATH_SEPARATOR);
        switch (method) {
        case SUM:
            return sumBucket(name, bucketsPath);
        case MAX:
            return maxBucket(name, bucketsPath);
        case MIN:
            return minBucket(name, bucketsPath);
        case AVERAGE:
            return avgBucket(name, bucketsPath);
        default:
            throw new ODataRuntimeException(
                    String.format("Aggregate method '%s' is not supported yet.", method));
        }
    }

}
