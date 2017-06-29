package com.hevelian.olastic.core.api.uri.queryoption.expression.member;

import com.hevelian.olastic.core.api.uri.queryoption.expression.member.impl.*;
import com.hevelian.olastic.core.edm.ElasticEdmEntityType;
import com.hevelian.olastic.core.edm.ElasticEdmProperty;
import org.apache.commons.lang3.StringUtils;
import org.apache.olingo.commons.api.edm.EdmAnnotation;
import org.apache.olingo.commons.api.edm.EdmEntityType;
import org.apache.olingo.commons.api.edm.EdmProperty;
import org.apache.olingo.commons.api.edm.constants.EdmTypeKind;
import org.apache.olingo.server.api.ODataApplicationException;
import org.apache.olingo.server.api.uri.*;
import org.apache.olingo.server.api.uri.queryoption.expression.*;
import org.apache.olingo.server.core.uri.UriInfoImpl;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.hevelian.olastic.core.elastic.ElasticConstants.NESTED_PATH_SEPARATOR;
import static com.hevelian.olastic.core.utils.ProcessorUtils.throwNotImplemented;

/**
 * Processes raw olingo expression member data.
 * 
 * @author Taras Kohut
 * @author rdidyk
 */
public class MemberHandler {

    private UriResource firstPart;
    private UriResource lastPart;
    private List<UriResource> resourceParts;
    /**
     * represents the path to current member. Is useful in lambdas, because
     * member contains no information about its parent members
     */
    private String pathToMember;
    private Map<String, UriResource> collectionResourceCache;
    ExpressionVisitor<?> visitor;

    public String getPath() {
        return pathToMember;
    }

    /**
     * Initializes member handler using raw olingo expression member.
     *
     * @param member
     *            raw olingo expression member
     * @param visitor
     *            visitor instance
     */
    public MemberHandler(Member member, ExpressionVisitor<?> visitor) {
        this.visitor = visitor;
        UriInfoImpl resource = (UriInfoImpl) member.getResourcePath();
        resourceParts = resource.getUriResourceParts();
        firstPart = resourceParts.get(0);
        lastPart = resourceParts.get(resourceParts.size() - 1);
        String parentPath = resource.getFragment();
        pathToMember = collectPathToMember(parentPath);
    }

    /**
     * Processes raw olingo expression member.
     * 
     * @param collectionResourceCache
     *            cache with parent members collection resources. Used for
     *            nested lambdas
     * @return expression member
     * @throws ODataApplicationException OData app exception
     * @throws ExpressionVisitException expression visitor exception
     */
    public ExpressionMember handle(Map<String, UriResource> collectionResourceCache)
            throws ODataApplicationException, ExpressionVisitException {
        this.collectionResourceCache = collectionResourceCache;
        if (lastPart instanceof UriResourceLambdaAll) {
            return throwNotImplemented("All lambda is not implemented");
        }
        // filter by child's property
        // Authors?$filter=Books/any(b:b/Name eq 'God delusion')
        else if (lastPart instanceof UriResourceLambdaAny) {
            return handleLambdaAny();
        } else if (lastPart instanceof UriResourcePrimitiveProperty
                || lastPart instanceof UriResourceLambdaVariable) {
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
        boolean isNavigationLambdaVar = firstPart instanceof UriResourcePartTyped && ((UriResourcePartTyped)firstPart).getType() instanceof EdmEntityType;
        //navigation property collection
        // /author?$filter=book/any(b:b/character/any(c:c/name eq 'Oliver Twist'))
        if (firstPart instanceof UriResourceNavigation || isNavigationLambdaVar) {
            //pre-last resource - the one before lambda; it's always a collection type
            UriResourceNavigation preLastNavResource = (UriResourceNavigation)resourceParts.get(resourceParts.size() - 2);
            ExpressionResult lambdaResult = (ExpressionResult) lambda.getExpression()
                    .accept(visitor);
            ElasticEdmEntityType entityType = (ElasticEdmEntityType) preLastNavResource
                    .getProperty().getType();
            return new ChildMember(entityType.getEType(), lambdaResult.getQueryBuilder()).any();
        }
        // complex or primitive type collection
        else {
            setPath(expression);
            // if any lambda uses primitive property
            // (//Books?$filter=property/any(p:p eq 'value'))
            // than parent path already contains path and property name
            // that's why we need to retrieve only nested path
            String nestedPath = isPreLastResourcePrimitive()
                    ? StringUtils.substringBeforeLast(pathToMember, NESTED_PATH_SEPARATOR)
                    : pathToMember;

            ExpressionResult expressionResult = (ExpressionResult) expression.accept(visitor);
            return isPreLastResourcePrimitive() ? expressionResult
                    : new NestedMember(nestedPath, expressionResult.getQueryBuilder()).any();
        }
    }

    private boolean isPreLastResourcePrimitive() {
        UriResource preLastResource = resourceParts.get(resourceParts.size() - 2);
        return preLastResource.getKind() == UriResourceKind.primitiveProperty;
    }

    private void setPath(Expression expression) {
        if (expression instanceof Member) {
            setPath((Member) expression);
        } else if (expression instanceof Binary) {
            Binary binaryExpression = (Binary) expression;
            setPath(binaryExpression.getLeftOperand());
            setPath(binaryExpression.getRightOperand());
        } else if (expression instanceof Method) {
            Method method = (Method) expression;
            method.getParameters().forEach(param->setPath(param));
        }
    }

    private void setPath(Member member) {
        UriInfoImpl memberUriInfo = (UriInfoImpl) member.getResourcePath();
        memberUriInfo.setFragment(pathToMember);
    }

    /**
     * Analyzes uri parts and creates primitive or parent expression member.
     * Also handles primitive expressions inside lambda's expression.
     * 
     * @return primitive or parent expression member
     */
    private ExpressionMember handlePrimitive() {
        // filter by parent's property
        // Books?$filter=Author/Name eq 'Dawkins'
        if (firstPart instanceof UriResourceNavigation) {
            EdmProperty lastProperty = ((UriResourceProperty) lastPart).getProperty();
            return new ParentMember(collectNavigationTypes(),
                    ((ElasticEdmProperty) lastProperty).getEField(), lastProperty.getAnnotations());
        }
        // filtering by complex type collection
        // Books?$filter=nested/any(n:n/state eq true)
        else if (firstPart instanceof UriResourceLambdaVariable
                && ((UriResourcePartTyped) firstPart).getType().getKind() == EdmTypeKind.COMPLEX) {
            EdmProperty lastProperty = ((UriResourceProperty) lastPart).getProperty();
            String parentPathPrefix = pathToMember != null ? pathToMember + NESTED_PATH_SEPARATOR
                    : "";
            String nestedPath = parentPathPrefix + lastProperty.getName();
            return new PrimitiveMember(nestedPath, lastProperty.getAnnotations());
        }
        // filtering by primitive type collection
        // Books?$filter=nested/property/any(p:p/tags/any(t:t eq 'Tag'))
        else if (firstPart instanceof UriResourceLambdaVariable
                && ((UriResourcePartTyped) firstPart).getType()
                        .getKind() == EdmTypeKind.PRIMITIVE) {
            String nestedPath = pathToMember != null ? pathToMember : "";
            UriResource parentResource = collectionResourceCache.get(pathToMember);

            return new PrimitiveMember(nestedPath, getAnnotations(parentResource));
        }
        // simple primitive expression or expression inside lambda for
        // retrieving children
        else {
            EdmProperty lastProperty = ((UriResourceProperty) lastPart).getProperty();
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

    private List<EdmAnnotation> getAnnotations(UriResource uriResource) {
        if (uriResource instanceof UriResourceNavigation) {
            return ((UriResourceNavigation) uriResource).getProperty().getAnnotations();
        } else if (uriResource instanceof UriResourceProperty) {
            return ((UriResourceProperty) uriResource).getProperty().getAnnotations();
        } else {
            return Collections.emptyList();
        }
    }

    /**
     * Collects path to member. This path is helpful for complex lambdas, like
     * this one: $filter=info/pages/any(p:p/words/any(w:w eq 'word')) We need to
     * store this path manually because Member inside lambda doesn't contain
     * full path to itself
     *
     * @param parentPath
     *            path to parent member
     * @return path to current member
     */
    private String collectPathToMember(String parentPath) {
        String parentPathPrefix = parentPath != null ? parentPath : "";
        List<String> resourceNames = null;

        if (resourceParts.size() > 1) {
            // we need only parts that shows path to property
            // the last part is either lambda or name of the property we want to
            // filter by, so we ignore it
            resourceNames = resourceParts.subList(0, resourceParts.size() - 1).stream()
                    .filter(resource -> resource instanceof UriResourceComplexProperty
                            || resource instanceof UriResourcePrimitiveProperty)
                    .map(part -> ((UriResourceProperty) part).getProperty().getName())
                    .collect(Collectors.toList());
        }
        boolean namesListIsNotEmpty = resourceNames != null && !resourceNames.isEmpty();
        if (namesListIsNotEmpty && !parentPathPrefix.isEmpty()) {
            parentPathPrefix += NESTED_PATH_SEPARATOR;
        }

        return namesListIsNotEmpty
                ? parentPathPrefix + String.join(NESTED_PATH_SEPARATOR, resourceNames) : parentPath;
    }

    /**
     * Returns collection URI resource
     * 
     * @return collection URI resource
     */
    public UriResource getCollectionResource() {
        UriResource collectionResource = null;
        if (lastPart.getKind() == UriResourceKind.lambdaAll
                || lastPart.getKind() == UriResourceKind.lambdaAny) {
            collectionResource = resourceParts.get(resourceParts.size() - 2);
        }
        return collectionResource;
    }

}
