package com.hevelian.olastic.core.api.uri.queryoption.expression.member.impl;

import com.hevelian.olastic.core.api.uri.queryoption.expression.member.ExpressionMember;
import com.hevelian.olastic.core.elastic.ElasticConstants;
import org.apache.olingo.commons.api.edm.EdmType;
import org.apache.olingo.server.api.ODataApplicationException;

import static com.hevelian.olastic.core.elastic.utils.ElasticUtils.addKeywordIfNeeded;
import static org.elasticsearch.index.query.QueryBuilders.*;

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

    @Override
    public ExpressionResult contains(ExpressionMember expressionMember) {
        return new ExpressionResult(
                matchQuery(getField(), ((ExpressionLiteral) expressionMember).getValue()) );
    }

    @Override
    public ExpressionResult startsWith(ExpressionMember expressionMember) {
        return new ExpressionResult(prefixQuery(addKeywordIfNeeded(this),
                (String)((ExpressionLiteral) expressionMember).getValue()));
    }

    @Override
    public ExpressionResult endsWith(ExpressionMember expressionMember) {
        return new ExpressionResult(wildcardQuery(addKeywordIfNeeded(this),
                ElasticConstants.WILDCARD_CHAR  + ((ExpressionLiteral) expressionMember).getValue()));
    }

    @Override
    public ExpressionMember date () {
        //Elasticsearch doesn't distinguish between search by the date and search by the timestamp, so no conversion is needed
        return this;
    }
}
