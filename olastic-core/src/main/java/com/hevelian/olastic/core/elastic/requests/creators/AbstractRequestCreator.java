package com.hevelian.olastic.core.elastic.requests.creators;

import static com.hevelian.olastic.core.elastic.utils.ElasticUtils.addKeywordIfNeeded;
import static com.hevelian.olastic.core.utils.ProcessorUtils.throwNotImplemented;
import static org.elasticsearch.index.query.QueryBuilders.matchQuery;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.olingo.commons.api.edm.EdmBindingTarget;
import org.apache.olingo.commons.api.edm.EdmEntitySet;
import org.apache.olingo.commons.api.ex.ODataRuntimeException;
import org.apache.olingo.server.api.ODataApplicationException;
import org.apache.olingo.server.api.uri.UriInfo;
import org.apache.olingo.server.api.uri.UriInfoResource;
import org.apache.olingo.server.api.uri.UriResource;
import org.apache.olingo.server.api.uri.UriResourceEntitySet;
import org.apache.olingo.server.api.uri.UriResourceKind;
import org.apache.olingo.server.api.uri.UriResourceNavigation;
import org.apache.olingo.server.api.uri.UriResourcePrimitiveProperty;
import org.apache.olingo.server.api.uri.queryoption.ApplyOption;
import org.apache.olingo.server.api.uri.queryoption.FilterOption;
import org.apache.olingo.server.api.uri.queryoption.OrderByItem;
import org.apache.olingo.server.api.uri.queryoption.OrderByOption;
import org.apache.olingo.server.api.uri.queryoption.SearchOption;
import org.apache.olingo.server.api.uri.queryoption.SkipOption;
import org.apache.olingo.server.api.uri.queryoption.TopOption;
import org.apache.olingo.server.api.uri.queryoption.expression.Expression;
import org.apache.olingo.server.api.uri.queryoption.expression.ExpressionVisitException;
import org.apache.olingo.server.api.uri.queryoption.expression.Member;
import org.apache.olingo.server.api.uri.queryoption.search.SearchBinary;
import org.apache.olingo.server.api.uri.queryoption.search.SearchBinaryOperatorKind;
import org.apache.olingo.server.api.uri.queryoption.search.SearchExpression;
import org.apache.olingo.server.api.uri.queryoption.search.SearchUnary;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;

import com.hevelian.olastic.core.api.uri.queryoption.expression.ElasticSearchExpressionVisitor;
import com.hevelian.olastic.core.api.uri.queryoption.expression.member.impl.ExpressionResult;
import com.hevelian.olastic.core.edm.ElasticEdmEntitySet;
import com.hevelian.olastic.core.edm.ElasticEdmProperty;
import com.hevelian.olastic.core.elastic.ElasticConstants;
import com.hevelian.olastic.core.elastic.builders.ESQueryBuilder;
import com.hevelian.olastic.core.elastic.pagination.Pagination;
import com.hevelian.olastic.core.elastic.pagination.Sort;
import com.hevelian.olastic.core.elastic.queries.Query;
import com.hevelian.olastic.core.elastic.requests.BaseRequest;
import com.hevelian.olastic.core.utils.ApplyOptionUtils;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.experimental.FieldDefaults;

/**
 * Base request creator with common logic to create Elasticsearch query.
 * 
 * @author rdidyk
 */
@AllArgsConstructor
@FieldDefaults(makeFinal = true, level = AccessLevel.PRIVATE)
@Getter
public abstract class AbstractRequestCreator implements ESRequestCreator {

    ESQueryBuilder<?> queryBuilder;

    /**
     * Constructor to initialize default ES query builder.
     */
    public AbstractRequestCreator() {
        this(new ESQueryBuilder<>());
    }

    /**
     * Gets base request info that is need for all requests. It goes through all
     * URI resource parts and build query for each segment, the last entity set
     * from resource segment is metadata entity of type to search. It returns
     * {@link Query} with index, type, and query builder for search, and last
     * entity set from resource parts.
     * 
     * @param uriInfo
     *            URI info
     * @return base request
     * @throws ODataApplicationException
     */
    public BaseRequest getBaseRequestInfo(UriInfo uriInfo) throws ODataApplicationException {
        List<UriResource> resourceParts = uriInfo.getUriResourceParts();
        ElasticEdmEntitySet responseEntitySet = (ElasticEdmEntitySet) getFirstResourceEntitySet(
                uriInfo);
        Iterator<UriResource> iterator = resourceParts.iterator();
        while (iterator.hasNext()) {
            UriResource segment = iterator.next();
            if (segment.getKind() == UriResourceKind.primitiveProperty) {
                break;
            }
            if (segment.getKind() == UriResourceKind.navigationProperty) {
                responseEntitySet = getNavigationTargetEntitySet(responseEntitySet,
                        (UriResourceNavigation) segment);
            } else if (segment.getKind() != UriResourceKind.entitySet) {
                throwNotImplemented();
            }
            if (iterator.hasNext()) {
                int nextIndex = resourceParts.indexOf(segment) + 1;
                queryBuilder.addSegmentQuery(segment, resourceParts.get(nextIndex));
            } else {
                queryBuilder.addSegmentQuery(segment, null);
            }
        }
        queryBuilder.addFilter(getFilterQuery(uriInfo)).addFilter(getSearchQuery(uriInfo));
        return new BaseRequest(new Query(responseEntitySet.getEIndex(),
                responseEntitySet.getEType(), queryBuilder.build()), responseEntitySet);
    }

    /**
     * Gets first resource entity set from URI info.
     * 
     * @param uriInfo
     *            URI info
     * @return first entity set
     * @throws ODataApplicationException
     */
    protected EdmEntitySet getFirstResourceEntitySet(UriInfo uriInfo)
            throws ODataApplicationException {
        List<UriResource> resourceParts = uriInfo.getUriResourceParts();
        UriResource uriResource = resourceParts.get(0);
        if (!(uriResource instanceof UriResourceEntitySet)) {
            throwNotImplemented("Only EntitySet is supported.");
        }
        return ((UriResourceEntitySet) uriResource).getEntitySet();
    }

    /**
     * Gets related binding target by resource navigation property name.
     * 
     * @param entitySet
     *            parent entity set
     * @param resourceNavigation
     *            resource navigation property
     * @return found binding target
     * @throws ODataApplicationException
     *             if any error occurred
     */
    protected ElasticEdmEntitySet getNavigationTargetEntitySet(ElasticEdmEntitySet entitySet,
            UriResourceNavigation resourceNavigation) throws ODataApplicationException {
        ElasticEdmEntitySet navigationTargetEntitySet = null;
        EdmBindingTarget edmBindingTarget = entitySet
                .getRelatedBindingTarget(resourceNavigation.getProperty().getName());
        if (edmBindingTarget == null) {
            throwNotImplemented();
        }
        if (edmBindingTarget instanceof ElasticEdmEntitySet) {
            navigationTargetEntitySet = (ElasticEdmEntitySet) edmBindingTarget;
        } else {
            throwNotImplemented();
        }
        return navigationTargetEntitySet;
    }

    /**
     * Method get's filter query from URL.
     * 
     * @param uriInfo
     *            URI info
     * @return filter query
     * @throws ODataApplicationException
     *             if any error occurred
     */
    protected BoolQueryBuilder getFilterQuery(UriInfo uriInfo) throws ODataApplicationException {
        FilterOption filterOption = uriInfo.getFilterOption();
        ApplyOption applyOption = uriInfo.getApplyOption();
        BoolQueryBuilder filterQuery = new BoolQueryBuilder();
        try {
            if (filterOption != null) {
                Expression expression = filterOption.getExpression();
                ExpressionResult expressionResult = (ExpressionResult) expression
                        .accept(new ElasticSearchExpressionVisitor());
                filterQuery.filter(expressionResult.getQueryBuilder());
            } else if (applyOption != null) {
                List<Expression> expressions = ApplyOptionUtils.getFilters(applyOption).stream()
                        .map(e -> e.getFilterOption().getExpression()).collect(Collectors.toList());
                for (Expression expression : expressions) {
                    ExpressionResult expressionResult = (ExpressionResult) expression
                            .accept(new ElasticSearchExpressionVisitor());
                    filterQuery.filter(expressionResult.getQueryBuilder());
                }
            }
        } catch (ExpressionVisitException e) {
            throw new ODataRuntimeException(e);
        }
        return filterQuery;
    }

    /**
     * Method creates search query from $search system query option from URL.
     * 
     * @param uriInfo
     *            URI info
     * @return search query
     * @throws ODataApplicationException
     *             if any error occurred
     */
    protected BoolQueryBuilder getSearchQuery(UriInfo uriInfo) throws ODataApplicationException {
        SearchOption searchOption = uriInfo.getSearchOption();
        ApplyOption applyOption = uriInfo.getApplyOption();
        BoolQueryBuilder searchQuery = new BoolQueryBuilder();
        if (searchOption != null) {
            buildSearchQuery(searchQuery, searchOption.getSearchExpression());
        } else if (applyOption != null) {
            List<SearchExpression> expressions = ApplyOptionUtils.getSearchItems(applyOption)
                    .stream().map(e -> e.getSearchOption().getSearchExpression())
                    .collect(Collectors.toList());
            for (SearchExpression expression : expressions) {
                buildSearchQuery(searchQuery, expression);
            }
        }
        return searchQuery;
    }

    /**
     * Method create's query for $search expression.
     * 
     * @param parentQuery
     *            parent bool query
     * @param expression
     *            search expression from query option
     */
    private void buildSearchQuery(BoolQueryBuilder parentQuery, SearchExpression expression) {
        if (expression.isSearchBinary()) {
            SearchBinary binary = expression.asSearchBinary();
            BoolQueryBuilder leftBool = QueryBuilders.boolQuery();
            BoolQueryBuilder rightBool = QueryBuilders.boolQuery();
            SearchBinaryOperatorKind operatorKind = binary.getOperator();
            if (operatorKind == SearchBinaryOperatorKind.AND) {
                parentQuery.must(leftBool);
                parentQuery.must(rightBool);
            } else if (operatorKind == SearchBinaryOperatorKind.OR) {
                parentQuery.should(leftBool);
                parentQuery.should(rightBool);
            }
            buildSearchQuery(leftBool, binary.getLeftOperand());
            buildSearchQuery(rightBool, binary.getRightOperand());
        } else if (expression.isSearchUnary()) {
            SearchUnary unary = expression.asSearchUnary();
            parentQuery.mustNot(
                    matchQuery(ElasticConstants.ALL_FIELD, unary.getOperand().getSearchTerm()));
        } else {
            parentQuery.must(matchQuery(ElasticConstants.ALL_FIELD,
                    expression.asSearchTerm().getSearchTerm()));
        }
    }

    /**
     * Returns pagination data.
     *
     * @return pagination
     */
    protected Pagination getPagination(UriInfo uriInfo) {
        SkipOption skipOption = uriInfo.getSkipOption();
        int skipNumber = skipOption != null ? skipOption.getValue() : Pagination.SKIP_DEFAULT;

        TopOption topOption = uriInfo.getTopOption();
        int topNumber = topOption != null ? topOption.getValue() : Pagination.TOP_DEFAULT;

        OrderByOption orderByOption = uriInfo.getOrderByOption();
        List<Sort> orderBy = new ArrayList<>();
        if (orderByOption != null) {
            List<OrderByItem> orderItemList = orderByOption.getOrders();
            for (OrderByItem orderByItem : orderItemList) {
                Expression expression = orderByItem.getExpression();
                if (expression instanceof Member) {
                    UriInfoResource resourcePath = ((Member) expression).getResourcePath();
                    UriResource oUriResource = resourcePath.getUriResourceParts().get(0);
                    if (oUriResource instanceof UriResourcePrimitiveProperty) {
                        ElasticEdmProperty edmProperty = (ElasticEdmProperty) ((UriResourcePrimitiveProperty) oUriResource)
                                .getProperty();
                        String property = addKeywordIfNeeded(edmProperty.getEField(),
                                edmProperty.getType());
                        orderBy.add(new Sort(property, orderByItem.isDescending()
                                ? Sort.Direction.DESC : Sort.Direction.ASC));
                    }
                }
            }
        }
        return new Pagination(topNumber, skipNumber, orderBy);
    }

}
