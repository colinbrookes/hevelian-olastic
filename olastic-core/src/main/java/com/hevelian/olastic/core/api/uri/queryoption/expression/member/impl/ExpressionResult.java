package com.hevelian.olastic.core.api.uri.queryoption.expression.member.impl;

import com.hevelian.olastic.core.api.uri.queryoption.expression.member.ExpressionMember;
import org.apache.olingo.server.api.ODataApplicationException;
import org.elasticsearch.index.query.QueryBuilder;

import static org.elasticsearch.index.query.QueryBuilders.boolQuery;

/**
 * Represents the result of expression.
 * @author Taras Kohut
 */
public class ExpressionResult extends ExpressionMember{
    private QueryBuilder queryBuilder;

    public ExpressionResult(QueryBuilder queryBuilder) {
        this.queryBuilder = queryBuilder;
    }

    public QueryBuilder getQueryBuilder() {
        return queryBuilder;
    }

    @Override
    public ExpressionResult and(ExpressionMember expressionMember) throws ODataApplicationException {
        return new ExpressionResult(boolQuery().must(queryBuilder).must(((ExpressionResult) expressionMember).getQueryBuilder()));
    }

    @Override
    public ExpressionResult or(ExpressionMember expressionMember) throws ODataApplicationException {
        return new ExpressionResult(boolQuery().should(queryBuilder).should(((ExpressionResult) expressionMember).getQueryBuilder()));
    }

    @Override
    public ExpressionResult not() throws ODataApplicationException {
        return new ExpressionResult(boolQuery().mustNot(queryBuilder));
    }
}
