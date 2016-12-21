package com.hevelian.olastic.core.utils;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;

import org.apache.olingo.server.api.uri.queryoption.ApplyItem;
import org.apache.olingo.server.api.uri.queryoption.ApplyOption;
import org.apache.olingo.server.api.uri.queryoption.apply.Aggregate;
import org.apache.olingo.server.api.uri.queryoption.apply.Filter;
import org.apache.olingo.server.api.uri.queryoption.apply.GroupBy;
import org.apache.olingo.server.api.uri.queryoption.apply.Search;

/**
 * Utility class with methods to work with {@link ApplyOption} system query.
 * 
 * @author rdidyk
 */
public final class ApplyOptionUtils {
    private ApplyOptionUtils() {
    }

    /**
     * Get's {@link Search} list from {@link ApplyOption} option.
     * 
     * @param applyOption
     *            apply option
     * @return item list
     */
    public static List<Search> getSearchItems(ApplyOption applyOption) {
        return getItems(applyOption, e -> e.getKind() == ApplyItem.Kind.SEARCH, Search.class);
    }

    /**
     * Get's {@link Filter} list from {@link ApplyOption} option.
     * 
     * @param applyOption
     *            apply option
     * @return item list
     */
    public static List<Filter> getFilters(ApplyOption applyOption) {
        return getItems(applyOption, e -> e.getKind() == ApplyItem.Kind.FILTER, Filter.class);
    }

    /**
     * Get's {@link Aggregate} list from {@link ApplyOption} option.
     * 
     * @param applyOption
     *            apply option
     * @return item list
     */
    public static List<Aggregate> getAggregations(ApplyOption applyOption) {
        return getItems(applyOption, e -> e.getKind() == ApplyItem.Kind.AGGREGATE, Aggregate.class);
    }

    /**
     * Get's {@link GroupBy} list from {@link ApplyOption} option.
     * 
     * @param applyOption
     *            apply option
     * @return item list
     */
    public static List<GroupBy> getGroupByItems(ApplyOption applyOption) {
        return getItems(applyOption, e -> e.getKind() == ApplyItem.Kind.GROUP_BY, GroupBy.class);
    }

    /**
     * Get's item list from {@link ApplyOption} by predicate.
     * 
     * @param applyOption
     *            apply option
     * @param predicate
     *            predicate for filter
     * @param clazz
     *            item class
     * @return list of items math to predicate and class
     */
    public static <T> List<T> getItems(ApplyOption applyOption, Predicate<ApplyItem> predicate,
            Class<T> clazz) {
        List<T> itemsList = new ArrayList<>();
        if (applyOption != null) {
            applyOption.getApplyItems().stream().filter(predicate).map(clazz::cast)
                    .forEach(itemsList::add);
        }
        return itemsList;
    }

}
