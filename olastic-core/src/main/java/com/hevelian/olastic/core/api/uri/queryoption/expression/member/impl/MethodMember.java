package com.hevelian.olastic.core.api.uri.queryoption.expression.member.impl;

import static com.hevelian.olastic.core.elastic.utils.ElasticUtils.addKeywordIfNeeded;
import static org.elasticsearch.index.query.QueryBuilders.prefixQuery;
import static org.elasticsearch.index.query.QueryBuilders.wildcardQuery;

import com.hevelian.olastic.core.api.uri.queryoption.expression.member.ExpressionMember;
import com.hevelian.olastic.core.elastic.ElasticConstants;

/**
 * Handles method calls.
 *
 * @author Taras Kohut
 * @contributor Ruslan Didyk
 */
public class MethodMember extends BaseMember {

    @Override
    public ExpressionResult contains(ExpressionMember left, ExpressionMember right) {
        PrimitiveMember primitive = (PrimitiveMember) left;
        LiteralMember literal = (LiteralMember) right;
        return new ExpressionResult(
                wildcardQuery(addKeywordIfNeeded(primitive.getField(), primitive.getAnnotations()),
                        ElasticConstants.WILDCARD_CHAR + literal.getValue()
                                + ElasticConstants.WILDCARD_CHAR));
    }

    @Override
    public ExpressionResult startsWith(ExpressionMember left, ExpressionMember right) {
        PrimitiveMember primitive = (PrimitiveMember) left;
        LiteralMember literal = (LiteralMember) right;
        return new ExpressionResult(
                prefixQuery(addKeywordIfNeeded(primitive.getField(), primitive.getAnnotations()),
                        (String) literal.getValue()));
    }

    @Override
    public ExpressionResult endsWith(ExpressionMember left, ExpressionMember right) {
        PrimitiveMember primitive = (PrimitiveMember) left;
        LiteralMember literal = (LiteralMember) right;
        return new ExpressionResult(
                wildcardQuery(addKeywordIfNeeded(primitive.getField(), primitive.getAnnotations()),
                        ElasticConstants.WILDCARD_CHAR + literal.getValue()));
    }

    @Override
    public ExpressionMember date(ExpressionMember expressionMember) {
        // Elasticsearch doesn't distinguish between search by the date and
        // search by the timestamp, so no conversion is needed
        return expressionMember;
    }
}
