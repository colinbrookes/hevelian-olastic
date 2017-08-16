package com.hevelian.olastic.core.api.uri.queryoption.expression.member;

import org.apache.olingo.server.api.ODataApplicationException;

/**
 * Interface for logical expressions.
 * 
 * @author Taras Kohut
 */
public interface LogicalExpression {
    /**
     * Applies other expression member using and operation.
     * 
     * @param expressionMember
     *            member to apply
     * @return result of the expression
     * @throws ODataApplicationException
     *             odata app exception
     */
    ExpressionMember and(ExpressionMember expressionMember) throws ODataApplicationException;

    /**
     * Applies other expression member using or operation.
     * 
     * @param expressionMember
     *            member to apply
     * @return result of the expression
     * @throws ODataApplicationException
     *             odata app exception
     */
    ExpressionMember or(ExpressionMember expressionMember) throws ODataApplicationException;

    /**
     * Performs logical not operation on the expression member.
     * 
     * @return result of the expression
     * @throws ODataApplicationException
     *             odata app exception
     */
    ExpressionMember not() throws ODataApplicationException;

    /**
     * Applies other expression member using equals operation.
     * 
     * @param expressionMember
     *            member to apply
     * @return result of the expression
     * @throws ODataApplicationException
     *             odata app exception
     */
    ExpressionMember eq(ExpressionMember expressionMember) throws ODataApplicationException;

    /**
     * Applies other expression member using not equals operation.
     * 
     * @param expressionMember
     *            member to apply
     * @return result of the expression
     * @throws ODataApplicationException
     *             odata app exception
     */
    ExpressionMember ne(ExpressionMember expressionMember) throws ODataApplicationException;

    /**
     * Applies other expression member using greater than or equals operation.
     * 
     * @param expressionMember
     *            member to apply
     * @return result of the expression
     * @throws ODataApplicationException
     *             odata app exception
     */
    ExpressionMember ge(ExpressionMember expressionMember) throws ODataApplicationException;

    /**
     * Applies other expression member using greater than operation.
     * 
     * @param expressionMember
     *            member to apply
     * @return result of the expression
     * @throws ODataApplicationException
     *             odata app exception
     */
    ExpressionMember gt(ExpressionMember expressionMember) throws ODataApplicationException;

    /**
     * Applies other expression member using less than or equals operation.
     * 
     * @param expressionMember
     *            member to apply
     * @return result of the expression
     * @throws ODataApplicationException
     *             odata app exception
     */
    ExpressionMember le(ExpressionMember expressionMember) throws ODataApplicationException;

    /**
     * Applies other expression member using less than operation.
     * 
     * @param expressionMember
     *            member to apply
     * @return result of the expression
     * @throws ODataApplicationException
     *             odata app exception
     */
    ExpressionMember lt(ExpressionMember expressionMember) throws ODataApplicationException;

}
