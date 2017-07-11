package com.hevelian.olastic.core.api.uri.queryoption.expression.member.impl;

import static com.hevelian.olastic.core.elastic.ElasticConstants.WILDCARD_CHAR;
import static com.hevelian.olastic.core.elastic.utils.ElasticUtils.addKeywordIfNeeded;
import static com.hevelian.olastic.core.utils.ProcessorUtils.throwNotImplemented;
import static org.elasticsearch.index.query.QueryBuilders.prefixQuery;
import static org.elasticsearch.index.query.QueryBuilders.wildcardQuery;

import org.apache.olingo.server.api.ODataApplicationException;
import org.elasticsearch.index.query.QueryBuilder;

import com.hevelian.olastic.core.api.uri.queryoption.expression.member.ExpressionMember;

/**
 * Base common class for any expression member.
 *
 * @author Taras Kohut
 */
public abstract class BaseMember implements ExpressionMember {

    @Override
    public ExpressionMember any() throws ODataApplicationException {
        return throwNotImplemented();
    }

    @Override
    public ExpressionMember all() throws ODataApplicationException {
        return throwNotImplemented();
    }

    @Override
    public ExpressionMember and(ExpressionMember expressionMember)
            throws ODataApplicationException {
        return throwNotImplemented();
    }

    @Override
    public ExpressionMember or(ExpressionMember expressionMember) throws ODataApplicationException {
        return throwNotImplemented();
    }

    @Override
    public ExpressionMember not() throws ODataApplicationException {
        return throwNotImplemented();
    }

    @Override
    public ExpressionMember eq(ExpressionMember expressionMember) throws ODataApplicationException {
        return throwNotImplemented();
    }

    @Override
    public ExpressionMember ne(ExpressionMember expressionMember) throws ODataApplicationException {
        return throwNotImplemented();
    }

    @Override
    public ExpressionMember ge(ExpressionMember expressionMember) throws ODataApplicationException {
        return throwNotImplemented();
    }

    @Override
    public ExpressionMember gt(ExpressionMember expressionMember) throws ODataApplicationException {
        return throwNotImplemented();
    }

    @Override
    public ExpressionMember le(ExpressionMember expressionMember) throws ODataApplicationException {
        return throwNotImplemented();
    }

    @Override
    public ExpressionMember lt(ExpressionMember expressionMember) throws ODataApplicationException {
        return throwNotImplemented();
    }

    @Override
    public ExpressionMember contains(ExpressionMember expressionMember)
            throws ODataApplicationException {
        return throwNotImplemented();
    }

    @Override
    public ExpressionMember startsWith(ExpressionMember expressionMember)
            throws ODataApplicationException {
        return throwNotImplemented();
    }

    @Override
    public ExpressionMember endsWith(ExpressionMember expressionMember)
            throws ODataApplicationException {
        return throwNotImplemented();
    }

    @Override
    public ExpressionMember date() throws ODataApplicationException {
        return throwNotImplemented();
    }

    protected QueryBuilder buildContainsQuery(AnnotatedMember member, Object value) {
        return wildcardQuery(addKeywordIfNeeded(member.getField(), member.getAnnotations()),
                WILDCARD_CHAR + value + WILDCARD_CHAR);
    }

    protected QueryBuilder buildStartsWithQuery(AnnotatedMember member, String value) {
        return prefixQuery(addKeywordIfNeeded(member.getField(), member.getAnnotations()), value);
    }

    protected QueryBuilder buildEndsWithQuery(AnnotatedMember member, String value) {
        return wildcardQuery(addKeywordIfNeeded(member.getField(), member.getAnnotations()),
                WILDCARD_CHAR + value);
    }

}
