package com.hevelian.olastic.core.api.uri.queryoption.expression.member;

import static com.hevelian.olastic.core.utils.ProcessorUtils.throwNotImplemented;

import java.util.List;
import java.util.stream.Collectors;

import org.apache.olingo.commons.api.edm.EdmProperty;
import org.apache.olingo.commons.api.edm.constants.EdmTypeKind;
import org.apache.olingo.server.api.ODataApplicationException;
import org.apache.olingo.server.api.uri.UriInfoResource;
import org.apache.olingo.server.api.uri.UriResource;
import org.apache.olingo.server.api.uri.UriResourceComplexProperty;
import org.apache.olingo.server.api.uri.UriResourceLambdaAll;
import org.apache.olingo.server.api.uri.UriResourceLambdaAny;
import org.apache.olingo.server.api.uri.UriResourceLambdaVariable;
import org.apache.olingo.server.api.uri.UriResourceNavigation;
import org.apache.olingo.server.api.uri.UriResourcePartTyped;
import org.apache.olingo.server.api.uri.UriResourcePrimitiveProperty;
import org.apache.olingo.server.api.uri.UriResourceProperty;
import org.apache.olingo.server.api.uri.queryoption.expression.ExpressionVisitException;
import org.apache.olingo.server.api.uri.queryoption.expression.Member;

import com.hevelian.olastic.core.api.uri.queryoption.expression.ElasticSearchExpressionVisitor;
import com.hevelian.olastic.core.api.uri.queryoption.expression.member.impl.ChildMember;
import com.hevelian.olastic.core.api.uri.queryoption.expression.member.impl.ExpressionResult;
import com.hevelian.olastic.core.api.uri.queryoption.expression.member.impl.NestedMember;
import com.hevelian.olastic.core.api.uri.queryoption.expression.member.impl.ParentMember;
import com.hevelian.olastic.core.api.uri.queryoption.expression.member.impl.PrimitiveMember;
import com.hevelian.olastic.core.edm.ElasticEdmComplexType;
import com.hevelian.olastic.core.edm.ElasticEdmProperty;
import com.hevelian.olastic.core.elastic.ElasticConstants;

/**
 * Processes raw olingo expression member data.
 * 
 * @author Taras Kohut
 * @contributor rdidyk
 */
public class MemberHandler {

    private UriResource firstPart;
    private UriResource lastPart;
    private List<UriResource> resourceParts;

    /**
     * Initializes member handler using raw olingo expression member.
     *
     * @param member
     *            raw olingo expression member
     */
    public MemberHandler(Member member) {
        UriInfoResource resource = member.getResourcePath();
        resourceParts = resource.getUriResourceParts();
        firstPart = resourceParts.get(0);
        lastPart = resourceParts.get(resourceParts.size() - 1);
    }

    /**
     * Processes raw olingo expression member.
     *
     * @return expression member
     * @throws ODataApplicationException
     * @throws ExpressionVisitException
     */
    public ExpressionMember handle() throws ODataApplicationException, ExpressionVisitException {
        if (lastPart instanceof UriResourceLambdaAll) {
            return throwNotImplemented("All lambda is not implemented");
        }
        // filter by child's property
        // Authors?$filter=Books/any(b:b/Name eq 'God delusion')
        else if (lastPart instanceof UriResourceLambdaAny) {
            return handleLambdaAny();
        } else if (lastPart instanceof UriResourcePrimitiveProperty) {
            return handlePrimitive();
        } else {
            return throwNotImplemented();
        }
    }

    /**
     * Analyzes uri parts and creates a member. Lambda has expression that
     * should be executed to get the inner query.
     *
     * @return nested or child expression member
     */
    private ExpressionMember handleLambdaAny()
            throws ODataApplicationException, ExpressionVisitException {
        UriResourceLambdaAny lambda = (UriResourceLambdaAny) lastPart;
        if (firstPart instanceof UriResourceNavigation) {
            UriResourceNavigation navigationResource = (UriResourceNavigation) firstPart;
            ExpressionResult lambdaResult = (ExpressionResult) lambda.getExpression()
                    .accept(new ElasticSearchExpressionVisitor());
            return new ChildMember(navigationResource.getProperty().getType().getName(),
                    lambdaResult.getQueryBuilder()).any();
        }
        // complex type collection
        else {
            ElasticEdmComplexType complex = (ElasticEdmComplexType) ((UriResourceComplexProperty) firstPart)
                    .getProperty().getType();
            ExpressionResult lambdaResult = (ExpressionResult) lambda.getExpression()
                    .accept(new ElasticSearchExpressionVisitor());
            return new NestedMember(complex.getENestedType(), lambdaResult.getQueryBuilder()).any();
        }
    }

    /**
     * Analyzes uri parts and creates primitive or parent expression member.
     * Also handles primitive expressions inside lambda's expression.
     * 
     * @return primitive or parent expression member
     */
    private ExpressionMember handlePrimitive() {
        EdmProperty lastProperty = ((UriResourceProperty) lastPart).getProperty();
        // filter by parent's property
        // Books?$filter=Author/Name eq 'Dawkins'
        if (firstPart instanceof UriResourceNavigation) {
            return new ParentMember(collectNavigationTypes(resourceParts),
                    ((ElasticEdmProperty) lastProperty).getEField(), lastProperty.getType());
        }
        // filtering by complex type collection
        // Books?$filter=nested/any(n:n/state eq true)
        else if (firstPart instanceof UriResourceLambdaVariable
                && ((UriResourcePartTyped) firstPart).getType().getKind() == EdmTypeKind.COMPLEX) {
            ElasticEdmComplexType complexType = (ElasticEdmComplexType) ((UriResourceLambdaVariable) firstPart)
                    .getType();
            String nestedPath = complexType.getENestedType()
                    + ElasticConstants.NESTED_PATH_SEPARATOR + lastProperty.getName();
            return new PrimitiveMember(nestedPath, lastProperty.getType());
        }
        // simple primitive expression or expression inside lambda
        else {
            return new PrimitiveMember(((ElasticEdmProperty) lastProperty).getEField(),
                    lastProperty.getType());
        }
    }

    private List<String> collectNavigationTypes(List<UriResource> resourceParts) {
        return resourceParts.stream().filter(UriResourceNavigation.class::isInstance)
                .map(part -> ((UriResourceNavigation) part).getProperty().getType().getName())
                .collect(Collectors.toList());
    }

}
