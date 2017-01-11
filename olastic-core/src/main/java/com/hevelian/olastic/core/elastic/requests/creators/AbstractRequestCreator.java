package com.hevelian.olastic.core.elastic.requests.creators;

import static com.hevelian.olastic.core.utils.ProcessorUtils.getFirstResourceEntitySet;
import static com.hevelian.olastic.core.utils.ProcessorUtils.throwNotImplemented;
import static org.elasticsearch.index.query.QueryBuilders.matchQuery;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.lucene.search.join.ScoreMode;
import org.apache.olingo.commons.api.edm.EdmBindingTarget;
import org.apache.olingo.commons.api.edm.EdmNavigationProperty;
import org.apache.olingo.commons.api.edm.EdmProperty;
import org.apache.olingo.commons.api.ex.ODataRuntimeException;
import org.apache.olingo.server.api.ODataApplicationException;
import org.apache.olingo.server.api.uri.UriInfo;
import org.apache.olingo.server.api.uri.UriInfoResource;
import org.apache.olingo.server.api.uri.UriParameter;
import org.apache.olingo.server.api.uri.UriResource;
import org.apache.olingo.server.api.uri.UriResourceEntitySet;
import org.apache.olingo.server.api.uri.UriResourceKind;
import org.apache.olingo.server.api.uri.UriResourceNavigation;
import org.apache.olingo.server.api.uri.UriResourcePartTyped;
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
import org.apache.olingo.server.core.uri.UriResourceNavigationPropertyImpl;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.IdsQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;

import com.hevelian.olastic.core.api.uri.queryoption.expression.ElasticSearchExpressionVisitor;
import com.hevelian.olastic.core.api.uri.queryoption.expression.member.impl.ExpressionResult;
import com.hevelian.olastic.core.edm.ElasticEdmEntitySet;
import com.hevelian.olastic.core.edm.ElasticEdmEntityType;
import com.hevelian.olastic.core.elastic.ElasticConstants;
import com.hevelian.olastic.core.elastic.pagination.Pagination;
import com.hevelian.olastic.core.elastic.pagination.Sort;
import com.hevelian.olastic.core.elastic.queries.Query;
import com.hevelian.olastic.core.elastic.requests.BaseRequest;
import com.hevelian.olastic.core.utils.ApplyOptionUtils;

/**
 * Base request creator with common logic to create Elasticsearch query.
 * 
 * @author rdidyk
 */
public abstract class AbstractRequestCreator implements ESRequestCreator {

    private BoolQueryBuilder query;
    private QueryBuilder parentChildQuery;

    /**
     * Default constructor.
     */
    public AbstractRequestCreator() {
        this.query = QueryBuilders.boolQuery();
    }

    @Override
    public BaseRequest create(UriInfo uriInfo) throws ODataApplicationException {
        List<UriResource> resourceParts = uriInfo.getUriResourceParts();
        ElasticEdmEntitySet responseEntitySet = (ElasticEdmEntitySet) getFirstResourceEntitySet(
                uriInfo).getEntitySet();

        Iterator<UriResource> iterator = resourceParts.iterator();
        while (iterator.hasNext()) {
            UriResource segment = iterator.next();
            if (segment.getKind() == UriResourceKind.primitiveProperty) {
                break;
            }
            if (segment.getKind() == UriResourceKind.navigationProperty) {
                UriResourceNavigation uriResourceNavigation = (UriResourceNavigation) segment;
                EdmNavigationProperty navigationProperty = uriResourceNavigation.getProperty();
                responseEntitySet = getNavigationTargetEntitySet(responseEntitySet,
                        navigationProperty);
            } else if (segment.getKind() != UriResourceKind.entitySet) {
                throwNotImplemented();
            }
            // TODO Possibly extract method.
            String type = ((UriResourcePartTyped) segment).getType().getName();
            List<String> ids = collectIds(segment);
            if (iterator.hasNext()) {
                int nextIndex = resourceParts.indexOf(segment) + 1;
                UriResource nextSegment = resourceParts.get(nextIndex);
                if (nextSegment.getKind() == UriResourceKind.primitiveProperty) {
                    addIdsQuery(type, ids);
                } else {
                    if (((UriResourceNavigationPropertyImpl) nextSegment).getProperty()
                            .isCollection()) {
                        addParentQuery(type, ids);
                    } else {
                        addChildQuery(type, ids);
                    }
                }
            } else {
                addIdsQuery(type, ids);
            }
        }
        ElasticEdmEntityType entityType = responseEntitySet.getEntityType();
        return new BaseRequest(
                new Query(entityType.getEIndex(), entityType.getEType(), getQueryBuilder(uriInfo)),
                responseEntitySet);
    }

    /**
     * Example: For the following navigation:
     * DemoService.svc/Categories(1)/Products we need the EdmEntitySet for the
     * navigation property "Products"
     *
     * This is defined as follows in the metadata: <code>
     * 
     * <EntitySet Name="Categories" EntityType="OData.Demo.Category">
     * <NavigationPropertyBinding Path="Products" Target="Products"/>
     * </EntitySet>
     * </code> The "Target" attribute specifies the target EntitySet Therefore
     * we need the startEntitySet "Categories" in order to retrieve the target
     * EntitySet "Products"
     */
    protected ElasticEdmEntitySet getNavigationTargetEntitySet(ElasticEdmEntitySet entitySet,
            EdmNavigationProperty navProperty) throws ODataApplicationException {
        ElasticEdmEntitySet navigationTargetEntitySet = null;
        EdmBindingTarget edmBindingTarget = entitySet
                .getRelatedBindingTarget(navProperty.getName());
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
     * Retrieves ids from uri resource part.
     *
     * @param segment
     *            uri resource part
     * @return ids list
     */
    private List<String> collectIds(UriResource segment) throws ODataApplicationException {
        List<UriParameter> keyPredicates;
        if (segment instanceof UriResourceNavigation) {
            keyPredicates = ((UriResourceNavigation) segment).getKeyPredicates();
        } else {
            keyPredicates = ((UriResourceEntitySet) segment).getKeyPredicates();
        }
        if (keyPredicates.size() > 1) {
            throwNotImplemented("Composite Keys are not supported");
        }
        return keyPredicates.stream().map(param -> param.getText().replaceAll("\'", ""))
                .collect(Collectors.toList());
    }

    /**
     * Adds new level of parent query.
     * 
     * @param type
     *            parent type
     * @param ids
     *            list of ids of parent documents we are looking for
     */
    private void addParentQuery(String type, List<String> ids) {
        QueryBuilder parentQuery = ids == null || ids.isEmpty() ? QueryBuilders.matchAllQuery()
                : buildIdsQuery(ids, type);
        QueryBuilder resultQuery = getParentChildResultQuery(parentQuery);
        parentChildQuery = QueryBuilders.hasParentQuery(type, resultQuery, false);
    }

    /**
     * Adds new level of child query.
     * 
     * @param type
     *            child type
     * @param ids
     *            list of ids of child documents we are looking for
     */
    private void addChildQuery(String type, List<String> ids) {
        QueryBuilder childQuery = ids == null || ids.isEmpty() ? QueryBuilders.matchAllQuery()
                : buildIdsQuery(ids, type);
        QueryBuilder resultQuery = getParentChildResultQuery(childQuery);
        parentChildQuery = QueryBuilders.hasChildQuery(type, resultQuery, ScoreMode.None);
    }

    /**
     * Adds ids query to the current level.
     * 
     * @param type
     *            type
     * @param ids
     *            list of ids
     */
    private void addIdsQuery(String type, List<String> ids) {
        if (!ids.isEmpty()) {
            query.must(buildIdsQuery(ids, type));
        }
    }

    private QueryBuilder buildIdsQuery(List<String> ids, String... type) {
        return new IdsQueryBuilder().types(type).addIds(ids.toArray(new String[1]));
    }

    /**
     * Returns raw Elasticsearch query.
     * 
     * @param uriInfo
     *            URI info
     * @return query builder
     * @throws ODataApplicationException
     *             if any error occurred
     */
    protected QueryBuilder getQueryBuilder(UriInfo uriInfo) throws ODataApplicationException {
        BoolQueryBuilder resultQuery = QueryBuilders.boolQuery();
        if (query.hasClauses()) {
            resultQuery.must(query);
        }
        if (parentChildQuery != null) {
            resultQuery.must(parentChildQuery);
        }
        BoolQueryBuilder filterQuery = getFilterQuery(uriInfo);
        if (filterQuery.hasClauses()) {
            resultQuery.filter(filterQuery);
        }
        BoolQueryBuilder searchQuery = getSearchQuery(uriInfo);
        if (searchQuery.hasClauses()) {
            resultQuery.filter(searchQuery);
        }
        return resultQuery;
    }

    /**
     * Builds must query with existing #parentChildQuery and new query, or just
     * returns new query, if $parentChildQuery is null Note: we can't initialize
     * #parentChildQuery in the beginning, because we don't know what type it
     * will be: has_parent or has_child
     * 
     * @param query
     * @return raw es query
     */
    private QueryBuilder getParentChildResultQuery(QueryBuilder query) {
        return parentChildQuery != null
                ? QueryBuilders.boolQuery().must(parentChildQuery).must(query) : query;
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
                // TODO Bug in expression visitor when mappings are enable
                QueryBuilder query = ((ExpressionResult) expression
                        .accept(new ElasticSearchExpressionVisitor())).getQueryBuilder();
                filterQuery.filter(query);
            } else if (applyOption != null) {
                List<Expression> expressions = ApplyOptionUtils.getFilters(applyOption).stream()
                        .map(e -> e.getFilterOption().getExpression()).collect(Collectors.toList());
                for (Expression expression : expressions) {
                    filterQuery.filter(
                            (QueryBuilder) expression.accept(new ElasticSearchExpressionVisitor()));
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
                        EdmProperty edmProperty = ((UriResourcePrimitiveProperty) oUriResource)
                                .getProperty();
                        // TODO Fix bug here when mappings are applied.
                        orderBy.add(new Sort(edmProperty.getName(), orderByItem.isDescending()
                                ? Sort.Direction.DESC : Sort.Direction.ASC));
                    }
                }
            }
        }
        return new Pagination(topNumber, skipNumber, orderBy);
    }

}
