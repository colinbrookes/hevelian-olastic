package com.hevelian.olastic.core.api.uri.queryoption.expression.member;

import static com.hevelian.olastic.core.elastic.ElasticConstants.NESTED_PATH_SEPARATOR;
import static com.hevelian.olastic.core.utils.ProcessorUtils.throwNotImplemented;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.apache.olingo.commons.api.edm.EdmAnnotation;
import org.apache.olingo.commons.api.edm.EdmEntityType;
import org.apache.olingo.commons.api.edm.EdmProperty;
import org.apache.olingo.commons.api.edm.constants.EdmTypeKind;
import org.apache.olingo.server.api.ODataApplicationException;
import org.apache.olingo.server.api.uri.UriResource;
import org.apache.olingo.server.api.uri.UriResourceComplexProperty;
import org.apache.olingo.server.api.uri.UriResourceKind;
import org.apache.olingo.server.api.uri.UriResourceLambdaAll;
import org.apache.olingo.server.api.uri.UriResourceLambdaAny;
import org.apache.olingo.server.api.uri.UriResourceLambdaVariable;
import org.apache.olingo.server.api.uri.UriResourceNavigation;
import org.apache.olingo.server.api.uri.UriResourcePartTyped;
import org.apache.olingo.server.api.uri.UriResourcePrimitiveProperty;
import org.apache.olingo.server.api.uri.UriResourceProperty;
import org.apache.olingo.server.api.uri.queryoption.expression.Binary;
import org.apache.olingo.server.api.uri.queryoption.expression.Expression;
import org.apache.olingo.server.api.uri.queryoption.expression.ExpressionVisitException;
import org.apache.olingo.server.api.uri.queryoption.expression.ExpressionVisitor;
import org.apache.olingo.server.api.uri.queryoption.expression.Member;
import org.apache.olingo.server.api.uri.queryoption.expression.Method;
import org.apache.olingo.server.core.uri.UriInfoImpl;

import com.hevelian.olastic.core.api.uri.queryoption.expression.member.impl.ChildMember;
import com.hevelian.olastic.core.api.uri.queryoption.expression.member.impl.ExpressionResult;
import com.hevelian.olastic.core.api.uri.queryoption.expression.member.impl.NestedMember;
import com.hevelian.olastic.core.api.uri.queryoption.expression.member.impl.ParentWrapperMember;
import com.hevelian.olastic.core.api.uri.queryoption.expression.member.impl.ParentPrimitiveMember;
import com.hevelian.olastic.core.api.uri.queryoption.expression.member.impl.PrimitiveMember;
import com.hevelian.olastic.core.edm.ElasticEdmEntityType;
import com.hevelian.olastic.core.edm.ElasticEdmProperty;

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
    private ExpressionVisitor<?> visitor;

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
     * Collects path to member. This path is helpful for complex lambdas, like
     * this one: $filter=info/pages/any(p:p/words/any(w:w eq 'word')) We need to
     * store this path manually because Member inside lambda doesn't contain
     * full path to itself.
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
     * Processes raw olingo expression member.
     * 
     * @param collectionResourceCache
     *            cache with parent members collection resources. Used for
     *            nested lambdas
     * @return expression member
     * @throws ODataApplicationException
     *             OData app exception
     * @throws ExpressionVisitException
     *             expression visitor exception
     */
    public ExpressionMember handle(Map<String, UriResource> collectionResourceCache)
            throws ODataApplicationException, ExpressionVisitException {
        this.collectionResourceCache = collectionResourceCache;
        if (lastPart instanceof UriResourceLambdaAll) {
            return throwNotImplemented("All lambda is not implemented");
        } else if (lastPart instanceof UriResourceLambdaAny) {
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

        boolean isNavigationLambdaVar = firstPart instanceof UriResourcePartTyped
                && ((UriResourcePartTyped) firstPart).getType() instanceof EdmEntityType;
        if (firstPart instanceof UriResourceNavigation || isNavigationLambdaVar) {
            boolean isParentNestedLambdaVar = resourceParts.stream()
                    .anyMatch(part -> part instanceof UriResourceComplexProperty);
            List<String> navigationTypes = collectNavigationTypes();
            if (isParentNestedLambdaVar) {
                // navigation parent nested collection
                // book?$filter=author/_dimension/any(d:d/name eq 'Validity')
                ExpressionResult lambdaResult = handleLambdaAny(expression);
                return new ParentWrapperMember(navigationTypes, lambdaResult.getQueryBuilder())
                        .any();
            } else {
                if (resourceParts.size() > 2) {
                    // navigation parent to another child
                    // book?$filter=author/address/any(a:a/city eq 'New York'))
                    List<String> parentTypes = navigationTypes.subList(0,
                            navigationTypes.size() - 1);
                    return new ParentWrapperMember(parentTypes,
                            handleChildLambda(lambda).getQueryBuilder()).any();
                } else {
                    // navigation child property collection
                    // author?$filter=book/any(b:b/character/any(c:c/name eq
                    // 'Oliver'))
                    return handleChildLambda(lambda);
                }
            }
        } else {
            // complex or primitive type collection
            return handleLambdaAny(expression);
        }
    }

    private ExpressionResult handleLambdaAny(Expression lambdaExpression)
            throws ODataApplicationException, ExpressionVisitException {
        setPath(lambdaExpression);
        // if any lambda uses primitive property
        // Books?$filter=property/any(p:p eq 'value')
        // than parent path already contains path and property name
        // that's why we need to retrieve only nested path
        String nestedPath = isPreLastResourcePrimitive()
                ? StringUtils.substringBeforeLast(pathToMember, NESTED_PATH_SEPARATOR)
                : pathToMember;

        ExpressionResult expressionResult = (ExpressionResult) lambdaExpression.accept(visitor);
        return isPreLastResourcePrimitive() ? expressionResult
                : new NestedMember(nestedPath, expressionResult.getQueryBuilder()).any();
    }

    private ExpressionResult handleChildLambda(UriResourceLambdaAny lambda)
            throws ExpressionVisitException, ODataApplicationException {
        ExpressionResult lambdaResult = (ExpressionResult) lambda.getExpression().accept(visitor);
        // pre-last resource - before lambda; it's always a collection type
        UriResourceNavigation preLastNavResource = (UriResourceNavigation) resourceParts
                .get(resourceParts.size() - 2);
        ElasticEdmEntityType entityType = (ElasticEdmEntityType) preLastNavResource.getProperty()
                .getType();
        return new ChildMember(entityType.getESType(), lambdaResult.getQueryBuilder()).any();
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
            method.getParameters().forEach(this::setPath);
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
            PrimitiveMember primitiveMember = new PrimitiveMember(
                    ((ElasticEdmProperty) lastProperty).getEField(), lastProperty.getAnnotations());
            return new ParentPrimitiveMember(collectNavigationTypes(), primitiveMember);
        } else if (firstPart instanceof UriResourceLambdaVariable
                && ((UriResourcePartTyped) firstPart).getType().getKind() == EdmTypeKind.COMPLEX) {
            // filtering by complex type collection
            // Books?$filter=nested/any(n:n/state eq true)
            EdmProperty lastProperty = ((UriResourceProperty) lastPart).getProperty();
            String parentPathPrefix = pathToMember != null ? pathToMember + NESTED_PATH_SEPARATOR
                    : "";
            String nestedPath = parentPathPrefix + lastProperty.getName();
            return new PrimitiveMember(nestedPath, lastProperty.getAnnotations());
        } else if (firstPart instanceof UriResourceLambdaVariable
                && ((UriResourcePartTyped) firstPart).getType()
                        .getKind() == EdmTypeKind.PRIMITIVE) {
            // filtering by primitive type collection
            // Books?$filter=nested/property/any(p:p/tags/any(t:t eq 'Tag'))
            String nestedPath = pathToMember != null ? pathToMember : "";
            UriResource parentResource = collectionResourceCache.get(pathToMember);

            return new PrimitiveMember(nestedPath, getAnnotations(parentResource));
        } else {
            // simple primitive expression or expression inside lambda for
            // retrieving children
            EdmProperty lastProperty = ((UriResourceProperty) lastPart).getProperty();
            return new PrimitiveMember(((ElasticEdmProperty) lastProperty).getEField(),
                    lastProperty.getAnnotations());
        }
    }

    private List<String> collectNavigationTypes() {
        return resourceParts.stream().filter(UriResourceNavigation.class::isInstance)
                .map(part -> ((ElasticEdmEntityType) ((UriResourceNavigation) part).getProperty()
                        .getType()).getESType())
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
     * Returns collection URI resource.
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

    public String getPath() {
        return pathToMember;
    }

}
