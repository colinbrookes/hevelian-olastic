package com.hevelian.olastic.core.api.uri.queryoption.expression.member;

import org.apache.olingo.server.api.ODataApplicationException;


import static com.hevelian.olastic.core.utils.ProcessorUtils.throwNotImplemented;
//TODO think about separation - this class violates interface segregation principle
/**
 * Olastic wrapper for expression member.
 */
public abstract class ExpressionMember{
    /**
     * Applies other expression member using and operation
     * @param expressionMember member to apply
     * @return result of the expression
     * @throws ODataApplicationException
     */
    public ExpressionMember and(ExpressionMember expressionMember) throws ODataApplicationException {
        return throwNotImplemented("Not implemented");
    }

    /**
     * Applies other expression member using or operation
     * @param expressionMember member to apply
     * @return result of the expression
     * @throws ODataApplicationException
     */
    public ExpressionMember or(ExpressionMember expressionMember) throws ODataApplicationException {
        return throwNotImplemented("Not implemented");
    }

    /**
     * Applies other expression member using equals operation
     * @param expressionMember member to apply
     * @return result of the expression
     * @throws ODataApplicationException
     */
    public ExpressionMember eq(ExpressionMember expressionMember) throws ODataApplicationException {
        return throwNotImplemented("Not implemented");
    }

    /**
     * Applies other expression member using not equals operation
     * @param expressionMember member to apply
     * @return result of the expression
     * @throws ODataApplicationException
     */
    public ExpressionMember ne(ExpressionMember expressionMember) throws ODataApplicationException {
        return throwNotImplemented("Not implemented");
    }

    /**
     * Applies other expression member using greater than or equals operation
     * @param expressionMember member to apply
     * @return result of the expression
     * @throws ODataApplicationException
     */
    public ExpressionMember ge(ExpressionMember expressionMember) throws ODataApplicationException {
        return throwNotImplemented("Not implemented");
    }

    /**
     * Applies other expression member using greater than operation
     * @param expressionMember member to apply
     * @return result of the expression
     * @throws ODataApplicationException
     */
    public ExpressionMember gt(ExpressionMember expressionMember) throws ODataApplicationException {
        return throwNotImplemented("Not implemented");
    }

    /**
     * Applies other expression member using less than or equals operation
     * @param expressionMember member to apply
     * @return result of the expression
     * @throws ODataApplicationException
     */
    public ExpressionMember le(ExpressionMember expressionMember) throws ODataApplicationException {
        return throwNotImplemented("Not implemented");
    }

    /**
     * Applies other expression member using less than operation
     * @param expressionMember member to apply
     * @return result of the expression
     * @throws ODataApplicationException
     */
    public ExpressionMember lt(ExpressionMember expressionMember) throws ODataApplicationException {
        return throwNotImplemented("Not implemented");
    }

    /**
     * Performs logical not operation on the expression member
     * @return result of the expression
     * @throws ODataApplicationException
     */
    public ExpressionMember not() throws ODataApplicationException {
        return throwNotImplemented("Not implemented");
    }

    /**
     * Performs any lambda operation on the expression member
     * @return result of the expression
     * @throws ODataApplicationException
     */
    public ExpressionMember any() throws ODataApplicationException {
        return throwNotImplemented("Not implemented");
    }

    /**
     * Performs all lambda operation on the expression member
     * @throws ODataApplicationException
     */
    public ExpressionMember all() throws ODataApplicationException {
        return throwNotImplemented("Not implemented");
    }

    /**
     * Checks if current member contains other
     * @throws ODataApplicationException
     */
    public ExpressionMember contains(ExpressionMember expressionMember) throws ODataApplicationException {
        return throwNotImplemented("Not implemented");
    }

    /**
     * Checks if current member starts with other
     * @throws ODataApplicationException
     */
    public ExpressionMember startsWith(ExpressionMember expressionMember) throws ODataApplicationException {
        return throwNotImplemented("Not implemented");
    }

    /**
     * Checks if current member ends with other
     * @throws ODataApplicationException
     */
    public ExpressionMember endsWith(ExpressionMember expressionMember) throws ODataApplicationException {
        return throwNotImplemented("Not implemented");
    }

    /**
     * Converts date time offset to date
     * @throws ODataApplicationException
     */
    public ExpressionMember date() throws ODataApplicationException {
        return throwNotImplemented("Not implemented");
    }
}
