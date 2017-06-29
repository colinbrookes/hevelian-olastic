package com.hevelian.olastic.core.api.uri.queryoption.expression.member.impl;

import com.hevelian.olastic.core.api.uri.queryoption.expression.member.ExpressionMember;
import org.apache.olingo.commons.api.edm.EdmAnnotation;
import org.apache.olingo.commons.api.http.HttpStatusCode;
import org.apache.olingo.server.api.ODataApplicationException;
import org.elasticsearch.index.query.QueryBuilder;

import java.util.List;
import java.util.Locale;

import static com.hevelian.olastic.core.elastic.ElasticConstants.ID_FIELD_NAME;
import static com.hevelian.olastic.core.elastic.utils.ElasticUtils.addKeywordIfNeeded;
import static org.elasticsearch.index.query.QueryBuilders.*;

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
        return new ExpressionResult(buildStartsWithQuery(this, (String)literal.getValue()));
    }

    @Override
    public ExpressionResult endsWith(ExpressionMember right) {
        LiteralMember literal = (LiteralMember) right;
        return new ExpressionResult(buildEndsWithQuery(this, (String)literal.getValue()));
    }

    @Override
    public ExpressionMember date() {
        // Elasticsearch doesn't distinguish between search by the date and
        // search by the timestamp, so no conversion is needed
        return this;
    }

    /**
     * Gets query for equals and not equals operations.
     * 
     * @param expressionMember
     *            member with value
     * @return appropriate query
     */
    private QueryBuilder getEqQuery(ExpressionMember expressionMember) throws ODataApplicationException{
        Object value = ((LiteralMember) expressionMember).getValue();
        if (getField().equals(ID_FIELD_NAME)) {
            if (value == null) {
                throw new ODataApplicationException("Id value can not be null", HttpStatusCode.BAD_REQUEST.getStatusCode(),
                        Locale.ROOT);
            }
            return idsQuery().addIds(value.toString());
        } else {
            String fieldName = addKeywordIfNeeded(getField(), getAnnotations());
            if (value == null) {
                return boolQuery().mustNot(existsQuery(fieldName));
            } else {
                return termQuery(fieldName, value);
            }
        }
    }
}
