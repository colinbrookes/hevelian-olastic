package com.hevelian.olastic.core.api.uri.queryoption.expression.member;

/**
 * Common interface for all expression members. All expression member classes
 * should have common root interface. This root interface is used in
 * ElasticSearchExpressionVisitor class as a generic type.
 */
public interface ExpressionMember extends LogicalExpression, LambdaExpression, MethodExpression {

}
