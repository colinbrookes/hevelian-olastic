package com.hevelian.olastic.core.api.uri.queryoption.expression;

import java.util.List;

import com.hevelian.olastic.core.api.uri.queryoption.expression.member.ExpressionMember;
import com.hevelian.olastic.core.api.uri.queryoption.expression.member.impl.ExpressionLiteral;
import org.apache.olingo.commons.api.edm.EdmEnumType;
import org.apache.olingo.commons.api.edm.EdmType;
import org.apache.olingo.server.api.ODataApplicationException;
import org.apache.olingo.server.api.uri.queryoption.expression.BinaryOperatorKind;
import org.apache.olingo.server.api.uri.queryoption.expression.Expression;
import org.apache.olingo.server.api.uri.queryoption.expression.ExpressionVisitException;
import org.apache.olingo.server.api.uri.queryoption.expression.ExpressionVisitor;
import org.apache.olingo.server.api.uri.queryoption.expression.Literal;
import org.apache.olingo.server.api.uri.queryoption.expression.Member;
import org.apache.olingo.server.api.uri.queryoption.expression.MethodKind;
import org.apache.olingo.server.api.uri.queryoption.expression.UnaryOperatorKind;

import static com.hevelian.olastic.core.utils.ProcessorUtils.throwNotImplemented;

/**
 * Implementation of expression visitor for building elasticsearch queries from filter expression.
 */
public class ElasticSearchExpressionVisitor implements ExpressionVisitor<ExpressionMember> {

    @Override
    public ExpressionMember visitBinaryOperator(BinaryOperatorKind operator, ExpressionMember left, ExpressionMember right)
            throws ExpressionVisitException, ODataApplicationException {
        switch (operator) {
            case AND:
                return left.and(right);
            case OR:
                return left.or(right);
            case EQ:
                return left.eq(right);
            case NE:
                return left.ne(right);
            case GE:
                return left.ge(right);
            case GT:
                return left.gt(right);
            case LE:
                return left.le(right);
            case LT:
                return left.lt(right);
            default:
                return throwNotImplemented("Unsupported binary operator");
        }
    }

    @Override
    public ExpressionMember visitUnaryOperator(UnaryOperatorKind operator, ExpressionMember operand)
            throws ExpressionVisitException, ODataApplicationException {
        return operand.not();
    }

    @Override
    public ExpressionMember visitMethodCall(MethodKind methodCall, List<ExpressionMember> parameters)
            throws ExpressionVisitException, ODataApplicationException {
        switch (methodCall) {
            case CONTAINS:
                return parameters.get(0).contains(parameters.get(1));
            case STARTSWITH:
                return parameters.get(0).startsWith(parameters.get(1));
            case ENDSWITH:
                return parameters.get(0).endsWith(parameters.get(1));
            case DATE:
                return parameters.get(0).date();
            default:
               return throwNotImplemented(String.format("Method call %s is not implemented", methodCall));
        }
    }

    @Override
    public ExpressionMember visitLambdaExpression(String lambdaFunction, String lambdaVariable,
                                                  Expression expression) throws ExpressionVisitException, ODataApplicationException {
        //this method isn't used, because lambdas are handled by visitMember method.
        return null;
    }

    @Override
    public ExpressionMember visitLiteral(Literal literal)
            throws ExpressionVisitException, ODataApplicationException {
        String literalAsString = literal.getText();
        EdmType type = literal.getType();
        return new ExpressionLiteral(literalAsString, type);
    }

    @Override
    public ExpressionMember visitMember(Member member)
            throws ExpressionVisitException, ODataApplicationException {
        MemberHandler handler = new MemberHandler(member);
        return handler.handle();
    }

    @Override
    public ExpressionMember visitAlias(String aliasName)
            throws ExpressionVisitException, ODataApplicationException {
        return throwNotImplemented("Aliases are not implemented");
    }

    @Override
    public ExpressionMember visitTypeLiteral(EdmType type)
            throws ExpressionVisitException, ODataApplicationException {
        return throwNotImplemented("Type literals are not implemented");
    }

    @Override
    public ExpressionMember visitLambdaReference(String variableName)
            throws ExpressionVisitException, ODataApplicationException {
        return throwNotImplemented("Lambda references are not implemented");
    }

    @Override
    public ExpressionMember visitEnum(EdmEnumType type, List<String> enumValues)
            throws ExpressionVisitException, ODataApplicationException {
        return throwNotImplemented("Enums are not implemented");
    }
}
