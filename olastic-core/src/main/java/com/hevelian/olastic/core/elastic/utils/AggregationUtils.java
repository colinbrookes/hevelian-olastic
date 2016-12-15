package com.hevelian.olastic.core.elastic.utils;

import static org.elasticsearch.search.aggregations.AggregationBuilders.avg;
import static org.elasticsearch.search.aggregations.AggregationBuilders.max;
import static org.elasticsearch.search.aggregations.AggregationBuilders.min;
import static org.elasticsearch.search.aggregations.AggregationBuilders.sum;

import java.util.ArrayList;
import java.util.List;

import org.apache.olingo.commons.api.ex.ODataRuntimeException;
import org.apache.olingo.server.api.uri.queryoption.ApplyItem;
import org.apache.olingo.server.api.uri.queryoption.ApplyOption;
import org.apache.olingo.server.api.uri.queryoption.apply.Aggregate;
import org.apache.olingo.server.api.uri.queryoption.apply.AggregateExpression.StandardMethod;
import org.apache.olingo.server.api.uri.queryoption.apply.GroupBy;
import org.elasticsearch.search.aggregations.AggregationBuilder;

/**
 * Utility class with helper methods to work with aggregations.
 * 
 * @author rdidyk
 */
public final class AggregationUtils {

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
     * Get's {@link Aggregate} list from {@link ApplyOption} option.
     * 
     * @param applyOption
     *            apply option
     * @return item list
     */
    public static List<Aggregate> getAggregations(ApplyOption applyOption) {
        List<Aggregate> aggregations = new ArrayList<>();
        if (applyOption != null) {
            for (ApplyItem item : applyOption.getApplyItems()) {
                if (item.getKind() == ApplyItem.Kind.AGGREGATE) {
                    aggregations.add((Aggregate) item);
                }
            }
        }
        return aggregations;
    }

    /**
     * Get's {@link GroupBy} list from {@link ApplyOption} option.
     * 
     * @param applyOption
     *            apply option
     * @return item list
     */
    public static List<GroupBy> getGroupByItems(ApplyOption applyOption) {
        List<GroupBy> groupByList = new ArrayList<>();
        if (applyOption != null) {
            for (ApplyItem applyItem : applyOption.getApplyItems()) {
                if (applyItem.getKind() == ApplyItem.Kind.GROUP_BY) {
                    groupByList.add((GroupBy) applyItem);
                }
            }
        }
        return groupByList;
    }

}
