package com.hevelian.olastic.core.elastic.builders;

import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.IdsQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;

import java.util.List;

/**
 * Builds elasticsearch query.
 *
 */
public class ESQueryBuilder {
    private String esType;
    private String esIndex;
	private BoolQueryBuilder query;
	private QueryBuilder parentChildQuery;

    /**
     * Initializes internal query.
     */
    public ESQueryBuilder() {
        query = QueryBuilders.boolQuery();
    }

    public String getEsIndex() {
        return esIndex;
    }

    public void setEsIndex(String esIndex) {
        this.esIndex = esIndex;
    }

    public String getEsType() {
        return esType;
    }

    public void setEsType(String esType) {
        this.esType = esType;
    }

    /**
     * Adds new level of parent query.
     * @param type parent type
     * @param ids list of ids of parent documents we are looking for
     * @return builder's instance
     */
    public ESQueryBuilder addParentQuery(String type, List ids){
        QueryBuilder parentQuery = ids == null || ids.isEmpty() ? QueryBuilders.matchAllQuery() : buildIdsQuery(ids, type);
        QueryBuilder resultQuery = getParentChildResultQuery(parentQuery);
        parentChildQuery = QueryBuilders.hasParentQuery(type, resultQuery);
        return this;
    }
    /**
     * Adds new level of child query.
     * @param type child type
     * @param ids list of ids of child documents we are looking for
     * @return builder's instance
     */
    public ESQueryBuilder addChildQuery(String type, List ids){
        QueryBuilder childQuery = ids == null || ids.isEmpty() ? QueryBuilders.matchAllQuery() : buildIdsQuery(ids, type);
        QueryBuilder resultQuery = getParentChildResultQuery(childQuery);
        parentChildQuery = QueryBuilders.hasChildQuery(type, resultQuery);
        return this;
    }

    /**
     * Adds ids query to the current level.
     * @param type type
     * @param ids list of ids
     * @return builder's instance
     */
    public ESQueryBuilder addIdsQuery(String type, List ids){
        if (!ids.isEmpty()) {
            query.must(buildIdsQuery(ids, type));
        }
        return this;
    }

    private QueryBuilder buildIdsQuery(List ids, String... type) {
        return new IdsQueryBuilder(type).ids(ids);
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
     * Builds must query with existing #parentChildQuery and new query,
     * or just returns new query, if $parentChildQuery is null
     * Note: we can't initialize #parentChildQuery in the beginning, because we don't know
     * what type it will be: has_parent or has_child
     * @param query
     * @return raw es query
     */
    private QueryBuilder getParentChildResultQuery(QueryBuilder query){
        return parentChildQuery != null
                ? QueryBuilders.boolQuery().must(parentChildQuery).must(query)
                : query;
    }

}
