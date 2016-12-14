package com.hevelian.olastic.core.elastic.builders;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.lucene.search.join.ScoreMode;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.IdsQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;

/**
 * Builds elasticsearch query.
 */
public class ESQueryBuilder {
    private String type;
    private String index;
    private Set<String> fields;
    private BoolQueryBuilder query;
    private QueryBuilder parentChildQuery;

    /**
     * Initializes internal query.
     */
    public ESQueryBuilder() {
        query = QueryBuilders.boolQuery();
        fields = new HashSet<>();
    }

    public String getIndex() {
        return index;
    }

    public ESQueryBuilder setIndex(String index) {
        this.index = index;
        return this;
    }

    public String getType() {
        return type;
    }

    public ESQueryBuilder setType(String type) {
        this.type = type;
        return this;
    }

    public Set<String> getFields() {
        return fields;
    }

    public ESQueryBuilder setFields(Set<String> fields) {
        this.fields = fields;
        return this;
    }

    public ESQueryBuilder addField(String field) {
        this.fields.add(field);
        return this;
    }

    /**
     * Adds new level of parent query.
     * 
     * @param type
     *            parent type
     * @param ids
     *            list of ids of parent documents we are looking for
     * @return builder's instance
     */
    public ESQueryBuilder addParentQuery(String type, List<String> ids) {
        QueryBuilder parentQuery = ids == null || ids.isEmpty() ? QueryBuilders.matchAllQuery()
                : buildIdsQuery(ids, type);
        QueryBuilder resultQuery = getParentChildResultQuery(parentQuery);
        parentChildQuery = QueryBuilders.hasParentQuery(type, resultQuery, false);
        return this;
    }

    /**
     * Adds new level of child query.
     * 
     * @param type
     *            child type
     * @param ids
     *            list of ids of child documents we are looking for
     * @return builder's instance
     */
    public ESQueryBuilder addChildQuery(String type, List<String> ids) {
        QueryBuilder childQuery = ids == null || ids.isEmpty() ? QueryBuilders.matchAllQuery()
                : buildIdsQuery(ids, type);
        QueryBuilder resultQuery = getParentChildResultQuery(childQuery);
        parentChildQuery = QueryBuilders.hasChildQuery(type, resultQuery, ScoreMode.None);
        return this;
    }

    /**
     * Adds ids query to the current level.
     * 
     * @param type
     *            type
     * @param ids
     *            list of ids
     * @return builder's instance
     */
    public ESQueryBuilder addIdsQuery(String type, List<String> ids) {
        if (!ids.isEmpty()) {
            query.must(buildIdsQuery(ids, type));
        }
        return this;
    }

    private QueryBuilder buildIdsQuery(List<String> ids, String... type) {
        return new IdsQueryBuilder().types(type).addIds(ids.toArray(new String[1]));
    }

    /**
     * Returns raw elasticsearch query.
     *
     * @return query
     */
    public BoolQueryBuilder getQuery() {
        BoolQueryBuilder resultQuery = QueryBuilders.boolQuery();
        if (query.hasClauses()) {
            resultQuery.must(query);
        }
        if (parentChildQuery != null) {
            resultQuery.must(parentChildQuery);
        }

        return resultQuery;
    }

    /**
     * Builds must query with existing #parentChildQuery and new query, or just
     * returns new query, if $parentChildQuery is null Note: we can't initialize
     * #parentChildQuery in the beginning, because we don't know what type it
     * will be: has_parent or has_child
     * 
     * @param query
     * @return raw es query
     */
    private QueryBuilder getParentChildResultQuery(QueryBuilder query) {
        return parentChildQuery != null
                ? QueryBuilders.boolQuery().must(parentChildQuery).must(query) : query;
    }

}
