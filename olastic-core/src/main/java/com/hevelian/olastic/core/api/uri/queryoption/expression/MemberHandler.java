package com.hevelian.olastic.core.api.uri.queryoption.expression;

import com.hevelian.olastic.core.api.uri.queryoption.expression.member.ExpressionMember;
import com.hevelian.olastic.core.api.uri.queryoption.expression.member.impl.*;
import com.hevelian.olastic.core.elastic.ElasticConstants;
import org.apache.olingo.commons.api.edm.EdmProperty;
import org.apache.olingo.commons.api.edm.constants.EdmTypeKind;
import org.apache.olingo.commons.api.http.HttpStatusCode;
import org.apache.olingo.server.api.ODataApplicationException;
import org.apache.olingo.server.api.uri.*;
import org.apache.olingo.server.api.uri.queryoption.expression.ExpressionVisitException;
import org.apache.olingo.server.api.uri.queryoption.expression.Member;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import static com.hevelian.olastic.core.utils.ProcessorUtils.throwNotImplemented;

/**
 * Processes raw olingo expression member data.
 * @author Taras Kohut
 */
public class MemberHandler {
    private int resourcePartsCount;
    private UriResource firstPart;
    private UriResource lastPart;
    private List<UriResource> resourceParts;

    /**
     * Initializes member handler using raw olingo expression member.
     *
     * @param member raw olingo expression member
     */
    public MemberHandler(Member member) {
        UriInfoResource resource = member.getResourcePath();
        resourceParts = resource.getUriResourceParts();
        resourcePartsCount = resourceParts.size();
        firstPart = resourceParts.get(0);
        lastPart = resourceParts.get(resourcePartsCount - 1);
    }

    /**
     * Processes raw olingo expression member
     *
     * @return olastic expression member
     * @throws ODataApplicationException
     * @throws ExpressionVisitException
     */
    public ExpressionMember handle() throws ODataApplicationException, ExpressionVisitException {
        if (lastPart instanceof UriResourceLambdaAll) {
            return throwNotImplemented("All lambda is not implemented");
        }
        //filter by child's property
        // Authors?$filter=Books/any(b:b/Name eq 'God delusion')
        else if (lastPart instanceof UriResourceLambdaAny) {
            return handleLambdaAny();
        } else if (lastPart instanceof UriResourcePrimitiveProperty) {
            return handlePrimitive();
        } else {
            throw new ODataApplicationException("Not implemented",
                    HttpStatusCode.NOT_IMPLEMENTED.getStatusCode(), Locale.ROOT);
        }
    }
    private List<String> collectNavigationTypes(List<UriResource> resourceParts) {
        List<String> navigationTypes = new ArrayList<>();
        int idx = 0;
        while (resourceParts.get(idx) instanceof UriResourceNavigation) {
            navigationTypes.add(((UriResourceNavigation) resourceParts.get(idx)).getProperty().getName());
            idx++;
        }
        return navigationTypes;
    }

    /**
     * Analyzes uri parts and creates a member.
     * Lambda has expression that should be executed to get the inner query.
     *
     * @return nested or child expression member
     */
    private ExpressionMember handleLambdaAny() throws ODataApplicationException, ExpressionVisitException {
        UriResourceLambdaAny lambda = (UriResourceLambdaAny) lastPart;
        if (firstPart instanceof UriResourceNavigation) {
            UriResourceNavigation navigationResource = (UriResourceNavigation) firstPart;
            ExpressionResult lambdaResult = (ExpressionResult) lambda
                    .getExpression()
                    .accept(new ElasticSearchExpressionVisitor());
            return new Child(navigationResource.getProperty().getName(), lambdaResult.getQueryBuilder()).any();
        }
        //complex type collection
        else {
            UriResourceComplexProperty complex = (UriResourceComplexProperty)firstPart;
            ExpressionResult lambdaResult = (ExpressionResult) lambda
                    .getExpression()
                    .accept(new ElasticSearchExpressionVisitor());
            return new Nested(complex.getProperty().getName(), lambdaResult.getQueryBuilder()).any();
        }

    }

    /**
     * Analyzes uri parts and creates primitive or parent expression member.
     * Also handles primitive expressions inside lambda's expression.
     * @return primitive or parent expression member
     */
    private ExpressionMember handlePrimitive() {
        EdmProperty lastProperty = ((UriResourceProperty) lastPart).getProperty();
        //filter by parent's property
        // Books?$filter=Author/Name eq 'Dawkins'
        if (firstPart instanceof UriResourceNavigation) {
            return new Parent(collectNavigationTypes(resourceParts), lastProperty.getName(), lastProperty.getType());
        }
        //filtering by complex type collection
        //Books?$filter=nested/any(n:n/state eq true)
        else if (firstPart instanceof UriResourceLambdaVariable &&
                ((UriResourcePartTyped) firstPart).getType().getKind() == EdmTypeKind.COMPLEX) {
            String nestedFieldName = ((UriResourceLambdaVariable) firstPart).getType().getName();
            String nestedPath = nestedFieldName + ElasticConstants.NESTED_PATH_SEPARATOR + lastProperty.getName();
            return new Primitive(nestedPath, lastProperty.getType());
        }
        //simple primitive expression or expression inside lambda
        else {
            return new Primitive(lastProperty.getName(), lastProperty.getType());
        }
    }
}
