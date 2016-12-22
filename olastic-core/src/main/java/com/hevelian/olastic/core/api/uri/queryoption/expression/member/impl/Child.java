package com.hevelian.olastic.core.api.uri.queryoption.expression.member.impl;

import com.hevelian.olastic.core.api.uri.queryoption.expression.member.ExpressionMember;
import org.apache.lucene.search.join.ScoreMode;
import org.apache.olingo.server.api.ODataApplicationException;
import org.elasticsearch.index.query.QueryBuilder;

import static org.elasticsearch.index.query.QueryBuilders.hasChildQuery;

/**
 * Wraps the data for child query building.
 * @author Taras Kohut
 */
public class Child extends ExpressionMember {
    private String childType;
    private QueryBuilder query;

    public Child(String childType, QueryBuilder query) {
        this.childType = childType;
        this.query = query;
    }

    @Override
    public ExpressionResult any() throws ODataApplicationException {
        return buildChildQuery();
    }

    private ExpressionResult buildChildQuery() {
        return new ExpressionResult(hasChildQuery(childType, query, ScoreMode.None));
    }

}
