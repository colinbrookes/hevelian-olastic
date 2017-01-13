package com.hevelian.olastic.core.api.uri.queryoption.expression.member.impl;

import org.apache.lucene.search.join.ScoreMode;
import org.apache.olingo.server.api.ODataApplicationException;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;

/**
 * Wraps the data for nested query building.
 *
 * @author Taras Kohut
 */
public class NestedMember extends BaseMember {

    private String nestedType;
    private QueryBuilder query;

    public NestedMember(String nestedType, QueryBuilder query) {
        this.nestedType = nestedType;
        this.query = query;
    }

    @Override
    public ExpressionResult any() throws ODataApplicationException {
        return buildNestedQuery();
    }

    private ExpressionResult buildNestedQuery() {
        return new ExpressionResult(QueryBuilders.nestedQuery(nestedType, query, ScoreMode.None));
    }

}
