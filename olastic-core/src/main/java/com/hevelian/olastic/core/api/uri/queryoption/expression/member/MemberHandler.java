package com.hevelian.olastic.core.api.uri.queryoption.expression.member;

import com.hevelian.olastic.core.api.uri.queryoption.expression.ElasticSearchExpressionVisitor;
import com.hevelian.olastic.core.api.uri.queryoption.expression.member.impl.*;
import com.hevelian.olastic.core.edm.ElasticEdmEntityType;
import com.hevelian.olastic.core.edm.ElasticEdmProperty;
import com.hevelian.olastic.core.elastic.ElasticConstants;
import org.apache.olingo.commons.api.edm.EdmProperty;
import org.apache.olingo.commons.api.edm.constants.EdmTypeKind;
import org.apache.olingo.server.api.ODataApplicationException;
import org.apache.olingo.server.api.uri.*;
import org.apache.olingo.server.api.uri.queryoption.expression.Binary;
import org.apache.olingo.server.api.uri.queryoption.expression.Expression;
import org.apache.olingo.server.api.uri.queryoption.expression.ExpressionVisitException;
import org.apache.olingo.server.api.uri.queryoption.expression.Member;
import org.apache.olingo.server.core.uri.UriInfoImpl;

import java.util.List;
import java.util.stream.Collectors;

import static com.hevelian.olastic.core.utils.ProcessorUtils.throwNotImplemented;

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
    private String parentPath;

    /**
     * Initializes member handler using raw olingo expression member.
     *
     * @param member
     *            raw olingo expression member
     */
    public MemberHandler(Member member) {
        UriInfoImpl resource = (UriInfoImpl)member.getResourcePath();
        resourceParts = resource.getUriResourceParts();
        firstPart = resourceParts.get(0);
        lastPart = resourceParts.get(resourceParts.size() - 1);
        String fragment = resource.getFragment();
        /** represents the path to current member.
         * Is useful in lambdas, because member contains no information about its parent members*/
        parentPath = collectPathToMember(fragment);
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
        Expression expression = lambda.getExpression();
        if (firstPart instanceof UriResourceNavigation) {
            UriResourceNavigation navigationResource = (UriResourceNavigation) firstPart;
            ExpressionResult lambdaResult = (ExpressionResult) lambda.getExpression()
                    .accept(new ElasticSearchExpressionVisitor());
            ElasticEdmEntityType entityType = (ElasticEdmEntityType) navigationResource
                    .getProperty().getType();
            return new ChildMember(entityType.getEType(), lambdaResult.getQueryBuilder()).any();
        }
        // complex type collection
        else {
            setPath(expression);
            ExpressionResult lambdaResult = (ExpressionResult) expression
                    .accept(new ElasticSearchExpressionVisitor());
            return new NestedMember(parentPath, lambdaResult.getQueryBuilder()).any();
        }
    }


    private void setPath(Expression expression) {
        if (expression instanceof Member) {
            setPath((Member)expression);
        } else if (expression instanceof Binary) {
            Binary binaryExpression = (Binary)expression;
            setPath(binaryExpression.getLeftOperand());
            setPath(binaryExpression.getRightOperand());
        }
    }


    private void setPath(Member member) {
        UriInfoImpl memberUriInfo = (UriInfoImpl)member.getResourcePath();
        memberUriInfo.setFragment(parentPath);
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
            return new ParentMember(collectNavigationTypes(),
                    ((ElasticEdmProperty) lastProperty).getEField(), lastProperty.getAnnotations());
        }
        // filtering by complex type collection
        // Books?$filter=nested/any(n:n/state eq true)
        else if (firstPart instanceof UriResourceLambdaVariable
                && ((UriResourcePartTyped) firstPart).getType().getKind() == EdmTypeKind.COMPLEX) {
            String parentPathPrefix = parentPath != null ? parentPath + ElasticConstants.NESTED_PATH_SEPARATOR : "";
            String nestedPath = parentPathPrefix + lastProperty.getName();
            return new PrimitiveMember(nestedPath, lastProperty.getAnnotations());
        }
        // simple primitive expression or expression inside lambda
        else {
            return new PrimitiveMember(((ElasticEdmProperty) lastProperty).getEField(),
                    lastProperty.getAnnotations());
        }
    }

    private List<String> collectNavigationTypes() {
        return resourceParts.stream().filter(UriResourceNavigation.class::isInstance)
                .map(part -> ((ElasticEdmEntityType) ((UriResourceNavigation) part).getProperty()
                        .getType()).getEType())
                .collect(Collectors.toList());
    }

    /**
     * Collects path to member.
     * This path is helpful for complex lambdas, like this one:
     * $filter=_omni/attributes/any(a:a/profile/any(p:p/name eq 'pattern_WN' and p/value eq 'W W'))
     *
     * @param parentPath path to parent member
     * @return path to current member
     */
    private String collectPathToMember(String parentPath) {
        String parentPathPrefix = parentPath != null ? parentPath : "";
        List resourceNames = null;

        if (resourceParts.size() > 1) {
            //we need only parts that shows path to property
            //the last part is either lambda or name of the property we want to filter by, so we ignore it
            resourceNames = resourceParts.subList(0, resourceParts.size() - 1)
                    .stream().filter(resource -> resource instanceof UriResourceComplexProperty || resource instanceof UriResourcePrimitiveProperty)
                    .map(part -> ((UriResourceProperty) part).getProperty()
                            .getName())
                    .collect(Collectors.toList());
        }
        boolean namesListIsNotEmpty = resourceNames!= null && !resourceNames.isEmpty();
        if (namesListIsNotEmpty && !parentPathPrefix.isEmpty()) {
            parentPathPrefix += ElasticConstants.NESTED_PATH_SEPARATOR;
        }

        return namesListIsNotEmpty ? parentPathPrefix + String.join(ElasticConstants.NESTED_PATH_SEPARATOR, resourceNames): parentPath;
    }

}
