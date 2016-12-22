package com.hevelian.olastic.core.api.uri.queryoption.expression.member.impl;

import com.hevelian.olastic.core.api.uri.queryoption.expression.member.ExpressionMember;
import org.apache.olingo.commons.api.edm.EdmType;
import org.apache.olingo.server.api.ODataApplicationException;
import org.elasticsearch.index.query.QueryBuilder;

import java.util.Collections;
import java.util.List;

import static com.hevelian.olastic.core.elastic.utils.ElasticUtils.addKeywordIfNeeded;
import static org.elasticsearch.index.query.QueryBuilders.*;

/**
 * Wraps the data needed for building parent query.
 *
 * @author Taras Kohut
 */
public class Parent extends TypedExpressionMember {
    private List<String> parentTypes;

    public Parent(List<String> parentTypes, String field, EdmType type) {
        super(field, type);
        this.parentTypes = parentTypes;
    }

    @Override
    public ExpressionResult eq(ExpressionMember expressionMember) throws ODataApplicationException {
        QueryBuilder query = termQuery(addKeywordIfNeeded(this), ((ExpressionLiteral) expressionMember).getValue());
        return buildParentQuery(query);
    }

    @Override
    public ExpressionResult ne(ExpressionMember expressionMember) throws ODataApplicationException {
        QueryBuilder query = boolQuery().mustNot(termQuery(addKeywordIfNeeded(this), ((ExpressionLiteral) expressionMember).getValue()));
        return buildParentQuery(query);
    }

    @Override
    public ExpressionResult ge(ExpressionMember expressionMember) throws ODataApplicationException {
        QueryBuilder query = rangeQuery(getField()).gte(((ExpressionLiteral) expressionMember).getValue());
        return buildParentQuery(query);
    }

    @Override
    public ExpressionResult gt(ExpressionMember expressionMember) throws ODataApplicationException {
        QueryBuilder query = rangeQuery(getField()).gt(((ExpressionLiteral) expressionMember).getValue());
        return buildParentQuery(query);
    }

    @Override
    public ExpressionResult le(ExpressionMember expressionMember) throws ODataApplicationException {
        QueryBuilder query = rangeQuery(getField()).lte(((ExpressionLiteral) expressionMember).getValue());
        return buildParentQuery(query);
    }

    @Override
    public ExpressionResult lt(ExpressionMember expressionMember) throws ODataApplicationException {
        QueryBuilder query = rangeQuery(getField()).lt(((ExpressionLiteral) expressionMember).getValue());
        return buildParentQuery(query);
    }

    private ExpressionResult buildParentQuery(QueryBuilder query) {
        Collections.reverse(parentTypes);
        QueryBuilder resultQuery = query;
        for (String type : parentTypes) {
            resultQuery = hasParentQuery(type, resultQuery, false);
        }
        return new ExpressionResult(resultQuery);
    }
}
