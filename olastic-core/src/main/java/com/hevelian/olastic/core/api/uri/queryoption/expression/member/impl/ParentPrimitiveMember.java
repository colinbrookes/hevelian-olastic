package com.hevelian.olastic.core.api.uri.queryoption.expression.member.impl;

import java.util.List;

import org.apache.olingo.server.api.ODataApplicationException;

import com.hevelian.olastic.core.api.uri.queryoption.expression.member.ExpressionMember;

/**
 * Wraps the data needed for building parent query.
 *
 * @author Taras Kohut
 */
public class ParentPrimitiveMember extends ParentMember {

    private PrimitiveMember primitiveMember;

    public ParentPrimitiveMember(List<String> parentTypes, PrimitiveMember primitiveMember) {
        super(parentTypes);
        this.primitiveMember = primitiveMember;
    }

    @Override
    public ExpressionResult eq(ExpressionMember expressionMember) throws ODataApplicationException {
        ExpressionResult expressionResult = primitiveMember.eq(expressionMember);
        return buildParentQuery(expressionResult.getQueryBuilder());
    }

    @Override
    public ExpressionResult ne(ExpressionMember expressionMember) throws ODataApplicationException {
        ExpressionResult expressionResult = primitiveMember.ne(expressionMember);
        return buildParentQuery(expressionResult.getQueryBuilder());
    }

    @Override
    public ExpressionResult ge(ExpressionMember expressionMember) throws ODataApplicationException {
        ExpressionResult expressionResult = primitiveMember.ge(expressionMember);
        return buildParentQuery(expressionResult.getQueryBuilder());
    }

    @Override
    public ExpressionResult gt(ExpressionMember expressionMember) throws ODataApplicationException {
        ExpressionResult expressionResult = primitiveMember.gt(expressionMember);
        return buildParentQuery(expressionResult.getQueryBuilder());
    }

    @Override
    public ExpressionResult le(ExpressionMember expressionMember) throws ODataApplicationException {
        ExpressionResult expressionResult = primitiveMember.le(expressionMember);
        return buildParentQuery(expressionResult.getQueryBuilder());
    }

    @Override
    public ExpressionResult lt(ExpressionMember expressionMember) throws ODataApplicationException {
        ExpressionResult expressionResult = primitiveMember.lt(expressionMember);
        return buildParentQuery(expressionResult.getQueryBuilder());
    }

    @Override
    public ExpressionResult contains(ExpressionMember right) {
        ExpressionResult expressionResult = primitiveMember.contains(right);
        return buildParentQuery(expressionResult.getQueryBuilder());
    }

    @Override
    public ExpressionResult startsWith(ExpressionMember right) {
        ExpressionResult expressionResult = primitiveMember.startsWith(right);
        return buildParentQuery(expressionResult.getQueryBuilder());
    }

    @Override
    public ExpressionResult endsWith(ExpressionMember right) {
        ExpressionResult expressionResult = primitiveMember.endsWith(right);
        return buildParentQuery(expressionResult.getQueryBuilder());
    }

    @Override
    public ExpressionMember date() {
        // Elasticsearch doesn't distinguish between search by the date and
        // search by the timestamp, so no conversion is needed
        return this;
    }

    public PrimitiveMember getPrimitiveMember() {
        return primitiveMember;
    }

}
