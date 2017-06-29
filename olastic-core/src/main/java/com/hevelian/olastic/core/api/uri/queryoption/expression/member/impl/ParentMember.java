package com.hevelian.olastic.core.api.uri.queryoption.expression.member.impl;

import com.hevelian.olastic.core.api.uri.queryoption.expression.member.ExpressionMember;
import org.apache.olingo.commons.api.edm.EdmAnnotation;
import org.apache.olingo.server.api.ODataApplicationException;
import org.elasticsearch.index.query.QueryBuilder;

import java.util.List;
import java.util.ListIterator;

import static com.hevelian.olastic.core.elastic.utils.ElasticUtils.addKeywordIfNeeded;
import static org.elasticsearch.index.query.QueryBuilders.*;

/**
 * Wraps the data needed for building parent query.
 *
 * @author Taras Kohut
 */
public class ParentMember extends AnnotatedMember {

    private List<String> parentTypes;

    public ParentMember(List<String> parentTypes, String field,  List<EdmAnnotation> annotations) {
        super(field, annotations);
        this.parentTypes = parentTypes;
    }

    @Override
    public ExpressionResult eq(ExpressionMember expressionMember) throws ODataApplicationException {
        QueryBuilder query = termQuery(addKeywordIfNeeded(getField(), getAnnotations()),
                ((LiteralMember) expressionMember).getValue());
        return buildParentQuery(query);
    }

    @Override
    public ExpressionResult ne(ExpressionMember expressionMember) throws ODataApplicationException {
        QueryBuilder query = boolQuery()
                .mustNot(termQuery(addKeywordIfNeeded(getField(), getAnnotations()),
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

    @Override
    public ExpressionResult contains(ExpressionMember right) {
        LiteralMember literal = (LiteralMember) right;
        QueryBuilder query = buildContainsQuery(this, literal.getValue());
        return buildParentQuery(query);
    }

    @Override
    public ExpressionResult startsWith(ExpressionMember right) {
        LiteralMember literal = (LiteralMember) right;
        QueryBuilder query =  buildStartsWithQuery(this, (String)literal.getValue());
        return buildParentQuery(query);
    }

    @Override
    public ExpressionResult endsWith(ExpressionMember right) {
        LiteralMember literal = (LiteralMember) right;
        QueryBuilder query = buildEndsWithQuery(this, (String)literal.getValue());
        return buildParentQuery(query);
    }

    @Override
    public ExpressionMember date() {
        // Elasticsearch doesn't distinguish between search by the date and
        // search by the timestamp, so no conversion is needed
        return this;
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
