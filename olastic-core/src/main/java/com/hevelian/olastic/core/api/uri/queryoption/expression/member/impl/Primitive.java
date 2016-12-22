package com.hevelian.olastic.core.api.uri.queryoption.expression.member.impl;

import com.hevelian.olastic.core.api.uri.queryoption.expression.member.ExpressionMember;
import org.apache.olingo.commons.api.edm.EdmType;
import org.apache.olingo.server.api.ODataApplicationException;

import static com.hevelian.olastic.core.elastic.utils.ElasticUtils.addKeywordIfNeeded;
import static org.elasticsearch.index.query.QueryBuilders.boolQuery;
import static org.elasticsearch.index.query.QueryBuilders.rangeQuery;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;

/**
 * Wraps raw olingo primitive.
 * @author Taras Kohut
 */
public class Primitive extends TypedExpressionMember {

    public Primitive(String field, EdmType edmType) {
        super(field, edmType);
    }

    @Override
    public ExpressionResult eq(ExpressionMember expressionMember) throws ODataApplicationException {
        return new ExpressionResult(
                termQuery(addKeywordIfNeeded(this), ((ExpressionLiteral) expressionMember).getValue()));
    }

    @Override
    public ExpressionResult ne(ExpressionMember expressionMember) throws ODataApplicationException {
        return new ExpressionResult(
                boolQuery().mustNot(termQuery(addKeywordIfNeeded(this), ((ExpressionLiteral) expressionMember).getValue())));
    }

    @Override
    public ExpressionResult ge(ExpressionMember expressionMember) throws ODataApplicationException {
        return new ExpressionResult(
                rangeQuery(getField()).gte(((ExpressionLiteral) expressionMember).getValue()));
    }

    @Override
    public ExpressionResult gt(ExpressionMember expressionMember) throws ODataApplicationException {
        return new ExpressionResult(
                rangeQuery(getField()).gt(((ExpressionLiteral) expressionMember).getValue()));
    }

    @Override
    public ExpressionResult le(ExpressionMember expressionMember) throws ODataApplicationException {
        return new ExpressionResult(
                rangeQuery(getField()).lte(((ExpressionLiteral) expressionMember).getValue()));
    }

    @Override
    public ExpressionResult lt(ExpressionMember expressionMember) throws ODataApplicationException {
        return new ExpressionResult(
                rangeQuery(getField()).lt(((ExpressionLiteral) expressionMember).getValue()));
    }
}
