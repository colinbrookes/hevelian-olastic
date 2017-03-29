package com.hevelian.olastic.core.api.uri.queryoption.expression.member.impl;

import static com.hevelian.olastic.core.elastic.utils.ElasticUtils.addKeywordIfNeeded;
import static org.elasticsearch.index.query.QueryBuilders.boolQuery;
import static org.elasticsearch.index.query.QueryBuilders.hasParentQuery;
import static org.elasticsearch.index.query.QueryBuilders.rangeQuery;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;

import java.util.List;
import java.util.ListIterator;

import org.apache.olingo.commons.api.edm.EdmType;
import org.apache.olingo.server.api.ODataApplicationException;
import org.elasticsearch.index.query.QueryBuilder;

import com.hevelian.olastic.core.api.uri.queryoption.expression.member.ExpressionMember;

/**
 * Wraps the data needed for building parent query.
 *
 * @author Taras Kohut
 */
public class ParentMember extends TypedMember {

    private List<String> parentTypes;

    public ParentMember(List<String> parentTypes, String field, EdmType type) {
        super(field, type);
        this.parentTypes = parentTypes;
    }

    @Override
    public ExpressionResult eq(ExpressionMember expressionMember) throws ODataApplicationException {
        QueryBuilder query = termQuery(addKeywordIfNeeded(getField(), getEdmType()),
                ((LiteralMember) expressionMember).getValue());
        return buildParentQuery(query);
    }

    @Override
    public ExpressionResult ne(ExpressionMember expressionMember) throws ODataApplicationException {
        QueryBuilder query = boolQuery()
                .mustNot(termQuery(addKeywordIfNeeded(getField(), getEdmType()),
                        ((LiteralMember) expressionMember).getValue()));
        return buildParentQuery(query);
    }

    @Override
    public ExpressionResult ge(ExpressionMember expressionMember) throws ODataApplicationException {
        QueryBuilder query = rangeQuery(getField())
                .gte(((LiteralMember) expressionMember).getValue());
        return buildParentQuery(query);
    }

    @Override
    public ExpressionResult gt(ExpressionMember expressionMember) throws ODataApplicationException {
        QueryBuilder query = rangeQuery(getField())
                .gt(((LiteralMember) expressionMember).getValue());
        return buildParentQuery(query);
    }

    @Override
    public ExpressionResult le(ExpressionMember expressionMember) throws ODataApplicationException {
        QueryBuilder query = rangeQuery(getField())
                .lte(((LiteralMember) expressionMember).getValue());
        return buildParentQuery(query);
    }

    @Override
    public ExpressionResult lt(ExpressionMember expressionMember) throws ODataApplicationException {
        QueryBuilder query = rangeQuery(getField())
                .lt(((LiteralMember) expressionMember).getValue());
        return buildParentQuery(query);
    }

    private ExpressionResult buildParentQuery(QueryBuilder query) {
        ListIterator<String> iterator = parentTypes.listIterator(parentTypes.size());
        QueryBuilder resultQuery = query;
        while (iterator.hasPrevious()) {
            resultQuery = hasParentQuery(iterator.previous(), resultQuery, false);
        }
        return new ExpressionResult(resultQuery);
    }
}
