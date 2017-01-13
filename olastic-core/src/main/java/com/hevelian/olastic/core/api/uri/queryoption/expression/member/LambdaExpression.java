package com.hevelian.olastic.core.api.uri.queryoption.expression.member;

import org.apache.olingo.server.api.ODataApplicationException;


/**
 * Interface for lambda expressions.
 * @author Taras Kohut
 */
public interface LambdaExpression {

    /**
     * Performs any lambda operation on the expression member
     * @return result of the expression
     * @throws ODataApplicationException
     */
    ExpressionMember any() throws ODataApplicationException;

    /**
     * Performs all lambda operation on the expression member
     * @throws ODataApplicationException
     */
    ExpressionMember all() throws ODataApplicationException;
}
