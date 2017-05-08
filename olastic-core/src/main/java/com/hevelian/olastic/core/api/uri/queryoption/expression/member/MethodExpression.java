package com.hevelian.olastic.core.api.uri.queryoption.expression.member;

import org.apache.olingo.server.api.ODataApplicationException;

/**
 * Interface for expression methods.
 * @author Taras Kohut
 */
public interface MethodExpression {

    /**
     * Checks if current member contains other one.
     *
     * @param left  method  parameter - column
     * @param right method parameter - literal
     * @return result of the expression
     * @throws ODataApplicationException odata app exception
     */
    ExpressionMember contains(ExpressionMember left, ExpressionMember right) throws ODataApplicationException;

    /**
     * Checks if current member starts with other one.
     *
     * @param left  method  parameter - column
     * @param right method parameter - literal
     * @return result of the expression
     * @throws ODataApplicationException odata app exception
     */
    ExpressionMember startsWith(ExpressionMember left, ExpressionMember right) throws ODataApplicationException;
    /**
     * Checks if current member ends with other one.
     *
     * @param left  method  parameter - column
     * @param right method parameter - literal
     * @return result of the expression
     * @throws ODataApplicationException odata app exception
     */
    ExpressionMember endsWith(ExpressionMember left, ExpressionMember right) throws ODataApplicationException;

    /**
     * Converts date time offset to date.
     *
     * @param expressionMember date column
     * @return result of the expression
     * @throws ODataApplicationException odata app exception
     */
    ExpressionMember date(ExpressionMember expressionMember) throws ODataApplicationException;
}
