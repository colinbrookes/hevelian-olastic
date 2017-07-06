package com.hevelian.olastic.core.api.uri.queryoption.expression.member.impl;

import static org.elasticsearch.index.query.QueryBuilders.boolQuery;
import static org.elasticsearch.index.query.QueryBuilders.rangeQuery;

import java.util.List;

import org.apache.olingo.commons.api.edm.EdmAnnotation;
import org.apache.olingo.server.api.ODataApplicationException;

import com.hevelian.olastic.core.api.uri.queryoption.expression.member.ExpressionMember;

/**
 * Wraps raw olingo primitive.
 *
 * @author Taras Kohut
 */
public class PrimitiveMember extends AnnotatedMember {

    public PrimitiveMember(String field, List<EdmAnnotation> annotations) {
        super(field, annotations);
    }

    @Override
    public ExpressionResult eq(ExpressionMember expressionMember) throws ODataApplicationException {
        return new ExpressionResult(getEqQuery(expressionMember));
    }

    @Override
    public ExpressionResult ne(ExpressionMember expressionMember) throws ODataApplicationException {
        return new ExpressionResult(boolQuery().mustNot(getEqQuery(expressionMember)));
    }

    @Override
    public ExpressionResult ge(ExpressionMember expressionMember) throws ODataApplicationException {
        return new ExpressionResult(
                rangeQuery(getField()).gte(((LiteralMember) expressionMember).getValue()));
    }

    @Override
    public ExpressionResult gt(ExpressionMember expressionMember) throws ODataApplicationException {
        return new ExpressionResult(
                rangeQuery(getField()).gt(((LiteralMember) expressionMember).getValue()));
    }

    @Override
    public ExpressionResult le(ExpressionMember expressionMember) throws ODataApplicationException {
        return new ExpressionResult(
                rangeQuery(getField()).lte(((LiteralMember) expressionMember).getValue()));
    }

    @Override
    public ExpressionResult lt(ExpressionMember expressionMember) throws ODataApplicationException {
        return new ExpressionResult(
                rangeQuery(getField()).lt(((LiteralMember) expressionMember).getValue()));
    }

    @Override
    public ExpressionResult contains(ExpressionMember right) {
        LiteralMember literal = (LiteralMember) right;
        return new ExpressionResult(buildContainsQuery(this, literal.getValue()));
    }

    @Override
    public ExpressionResult startsWith(ExpressionMember right) {
        LiteralMember literal = (LiteralMember) right;
        return new ExpressionResult(buildStartsWithQuery(this, (String) literal.getValue()));
    }

    @Override
    public ExpressionResult endsWith(ExpressionMember right) {
        LiteralMember literal = (LiteralMember) right;
        return new ExpressionResult(buildEndsWithQuery(this, (String) literal.getValue()));
    }

    @Override
    public ExpressionMember date() {
        // Elasticsearch doesn't distinguish between search by the date and
        // search by the timestamp, so no conversion is needed
        return this;
    }
}
