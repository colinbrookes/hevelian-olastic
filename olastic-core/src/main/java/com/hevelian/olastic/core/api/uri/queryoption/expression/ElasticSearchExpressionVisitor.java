package com.hevelian.olastic.core.api.uri.queryoption.expression;

import java.util.ArrayList;
import java.util.List;
import static org.elasticsearch.index.query.QueryBuilders.boolQuery;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.elasticsearch.index.query.QueryBuilders.rangeQuery;
import org.apache.olingo.commons.api.edm.EdmEnumType;
import org.apache.olingo.commons.api.edm.EdmType;
import org.apache.olingo.commons.core.edm.primitivetype.EdmString;
import org.apache.olingo.server.api.ODataApplicationException;
import org.apache.olingo.server.api.uri.UriInfoResource;
import org.apache.olingo.server.api.uri.UriResource;
import org.apache.olingo.server.api.uri.UriResourcePrimitiveProperty;
import org.apache.olingo.server.api.uri.UriResourceProperty;
import org.apache.olingo.server.api.uri.queryoption.expression.BinaryOperatorKind;
import org.apache.olingo.server.api.uri.queryoption.expression.Expression;
import org.apache.olingo.server.api.uri.queryoption.expression.ExpressionVisitException;
import org.apache.olingo.server.api.uri.queryoption.expression.ExpressionVisitor;
import org.apache.olingo.server.api.uri.queryoption.expression.Literal;
import org.apache.olingo.server.api.uri.queryoption.expression.Member;
import org.apache.olingo.server.api.uri.queryoption.expression.MethodKind;
import org.apache.olingo.server.api.uri.queryoption.expression.UnaryOperatorKind;
import org.elasticsearch.index.query.*;

import static com.hevelian.olastic.core.utils.ElasticUtils.getKeywordField;

public class ElasticSearchExpressionVisitor implements ExpressionVisitor<Object> {

    @Override
    public Object visitBinaryOperator(BinaryOperatorKind operator, Object left, Object right)
            throws ExpressionVisitException, ODataApplicationException {
        switch (operator) {
            case AND:
                return boolQuery().must((QueryBuilder) left).must((QueryBuilder) right);
            case OR:
                return boolQuery().should((QueryBuilder) left).should((QueryBuilder) right);
            case EQ:
                return termQuery(getKeywordField((String) left), right);
            case NE:
                return boolQuery().mustNot(termQuery(getKeywordField((String)left), right));
            case GE:
                return rangeQuery((String) left).gte(right);
            case GT:
                return rangeQuery((String) left).gt(right);
            case LE:
                return rangeQuery((String) left).lte(right);
            case LT:
                return rangeQuery((String) left).lt(right);
            default: throw new ExpressionVisitException("unsupported operator");
        }
    }

    @Override
    public Object visitUnaryOperator(UnaryOperatorKind operator, Object operand)
            throws ExpressionVisitException, ODataApplicationException {
        return boolQuery().mustNot((QueryBuilder) operand);
    }

    @Override
    public Object visitMethodCall(MethodKind methodCall, List<Object> parameters)
            throws ExpressionVisitException, ODataApplicationException {
        return null;
    }

    @Override
    public Object visitLambdaExpression(String lambdaFunction, String lambdaVariable,
            Expression expression) throws ExpressionVisitException, ODataApplicationException {
        return null;
    }

    @Override
    public Object visitLiteral(Literal literal)
            throws ExpressionVisitException, ODataApplicationException {
        // To keep this tutorial simple, our filter expression visitor supports
        // only Edm.Int32 and Edm.String
        // In real world scenarios it can be difficult to guess the type of an
        // literal.
        // We can be sure, that the literal is a valid OData literal because the
        // URI Parser checks
        // the lexicographical structure

        // String literals start and end with an single quotation mark
        String literalAsString = literal.getText();
        if (literal.getType() instanceof EdmString) {
            String stringLiteral = "";
            if (literal.getText().length() > 2) {
                stringLiteral = literalAsString.substring(1, literalAsString.length() - 1);
            }

            return stringLiteral;
        } else {
            return literalAsString;
        }
    }

    @Override
    public Object visitMember(Member member)
            throws ExpressionVisitException, ODataApplicationException {
        UriInfoResource resource = member.getResourcePath();
        if (resource.getUriResourceParts().size() == 1) {
            UriResourcePrimitiveProperty property = (UriResourcePrimitiveProperty) resource
                    .getUriResourceParts().get(0);
            return property.getProperty().getName();
        } else {
            List<String> propertyNames = new ArrayList<>();
            for (UriResource property : resource.getUriResourceParts()) {
                UriResourceProperty primitiveProperty = (UriResourceProperty) property;
                propertyNames.add(primitiveProperty.getProperty().getName());
            }
            return propertyNames;
        }
    }

    @Override
    public Object visitAlias(String aliasName)
            throws ExpressionVisitException, ODataApplicationException {
        return null;
    }

    @Override
    public Object visitTypeLiteral(EdmType type)
            throws ExpressionVisitException, ODataApplicationException {
        return null;
    }

    @Override
    public Object visitLambdaReference(String variableName)
            throws ExpressionVisitException, ODataApplicationException {
        return null;
    }

    @Override
    public Object visitEnum(EdmEnumType type, List<String> enumValues)
            throws ExpressionVisitException, ODataApplicationException {
        return null;
    }
}
