package com.hevelian.olastic.core.elastic.requests.creators;

import static com.hevelian.olastic.core.elastic.utils.ElasticUtils.addKeywordIfNeeded;
import static com.hevelian.olastic.core.utils.ApplyOptionUtils.getAggregations;
import static com.hevelian.olastic.core.utils.ApplyOptionUtils.getGroupByItems;
import static com.hevelian.olastic.core.utils.ProcessorUtils.throwNotImplemented;
import static java.util.Collections.reverse;
import static java.util.stream.Collectors.toMap;
import static org.elasticsearch.search.aggregations.AggregationBuilders.terms;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.apache.olingo.commons.api.http.HttpStatusCode;
import org.apache.olingo.server.api.ODataApplicationException;
import org.apache.olingo.server.api.uri.UriInfo;
import org.apache.olingo.server.api.uri.UriResource;
import org.apache.olingo.server.api.uri.UriResourceKind;
import org.apache.olingo.server.api.uri.queryoption.apply.GroupBy;
import org.apache.olingo.server.api.uri.queryoption.apply.GroupByItem;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;

import com.hevelian.olastic.core.edm.ElasticEdmEntitySet;
import com.hevelian.olastic.core.edm.ElasticEdmEntityType;
import com.hevelian.olastic.core.edm.ElasticEdmProperty;
import com.hevelian.olastic.core.elastic.builders.ESQueryBuilder;
import com.hevelian.olastic.core.elastic.pagination.Pagination;
import com.hevelian.olastic.core.elastic.pagination.Sort;
import com.hevelian.olastic.core.elastic.pagination.Sort.Direction;
import com.hevelian.olastic.core.elastic.queries.AggregateQuery;
import com.hevelian.olastic.core.elastic.queries.Query;
import com.hevelian.olastic.core.elastic.requests.AggregateRequest;
import com.hevelian.olastic.core.elastic.requests.ESRequest;

/**
 * Class responsible for creating {@link AggregateRequest} instance for buckets
 * aggregations with metrics.
 * 
 * @author rdidyk
 */
public class BucketsAggregationsRequestCreator extends AbstractAggregationsRequestCreator {

    /**
     * Default constructor.
     */
    public BucketsAggregationsRequestCreator() {
        super();
    }

    /**
     * Constructor to initialize ES query builder.
     * 
     * @param queryBuilder
     *            ES query builder
     */
    public BucketsAggregationsRequestCreator(ESQueryBuilder<?> queryBuilder) {
        super(queryBuilder);
    }

    @Override
    public AggregateRequest create(UriInfo uriInfo) throws ODataApplicationException {
        ESRequest baseRequestInfo = getBaseRequestInfo(uriInfo);
        Query baseQuery = baseRequestInfo.getQuery();
        ElasticEdmEntitySet entitySet = baseRequestInfo.getEntitySet();
        ElasticEdmEntityType entityType = entitySet.getEntityType();

        List<GroupBy> groupByItems = getGroupByItems(uriInfo.getApplyOption());
        if (groupByItems.size() > 1) {
            throwNotImplemented("Combining Transformations per Group is not supported.");
        }
        Pagination pagination = getPagination(uriInfo);
        List<AggregationBuilder> bucketsQueries = getBucketsQueries(groupByItems.get(0), entityType,
                pagination);

        AggregateQuery aggregateQuery = new AggregateQuery(baseQuery.getIndex(),
                baseQuery.getTypes(), baseQuery.getQueryBuilder(), bucketsQueries,
                Collections.emptyList());
        return new AggregateRequest(aggregateQuery, entitySet, pagination, getCountAlias());
    }

    /**
     * Get's buckets queries from {@link GroupBy} item in URL.
     *
     * @param groupBy
     *            groupBy instance
     * @param entityType
     *            entity type
     * @param pagination
     *            pagination information
     * @return list of fields
     * @throws ODataApplicationException
     *             if any error occurred
     */
    protected List<AggregationBuilder> getBucketsQueries(GroupBy groupBy,
            ElasticEdmEntityType entityType, Pagination pagination)
            throws ODataApplicationException {
        int size = pagination.getSkip() + pagination.getTop();
        Map<String, Boolean> orders = pagination.getOrderBy().stream()
                .collect(toMap(Sort::getProperty, order -> order.getDirection() == Direction.ASC));
        List<String> properties = getProperties(groupBy);
        reverse(properties);

        // Last because of reverse
        String lastProperty = properties.remove(0);
        String queryField = getQueryField(lastProperty, entityType);
        TermsAggregationBuilder groupByQuery = terms(lastProperty).field(queryField).size(size)
                .shardSize(getShardSize(size));
        getMetricsAggQueries(getAggregations(groupBy.getApplyOption()))
                .forEach(groupByQuery::subAggregation);
        List<BucketOrder> queryOrders = getQueryOrders(queryField, orders);
        if (!queryOrders.isEmpty()) {
            groupByQuery.order(queryOrders);
        }

        for (String property : properties) {
            queryField = getQueryField(property, entityType);
            groupByQuery = terms(property).field(queryField).size(size)
                    .subAggregation(groupByQuery);
            Boolean termOrder = orders.remove(queryField);
            if (termOrder != null) {
                groupByQuery.order(BucketOrder.key(termOrder));
            }
        }

        // Fields in $orderby are not same as $groupby fields!
        if (!orders.isEmpty()) {
            throw new ODataApplicationException(
                    "Ordering only by fields in $groupby or $aggregation options is allowed.",
                    HttpStatusCode.BAD_REQUEST.getStatusCode(), Locale.ROOT);
        }
        // For now only one 'groupby' is supported and returned in the list
        return Arrays.asList(groupByQuery);
    }

    /**
     * Get's shard_size for terms aggregation.
     * 
     * @param size
     *            actual requested size
     * @return shard_size
     */
    protected int getShardSize(int size) {
        // The default shard_size will be size if the search request needs to go
        // to a single shard - by Elasticsearch documentation
        return size;
    }

    /**
     * Get's properties from groupBy for aggregation query.
     * 
     * @param groupBy
     *            groupBy instance
     * @return list of properties
     * @throws ODataApplicationException
     *             if any error occurred
     */
    private static List<String> getProperties(GroupBy groupBy) throws ODataApplicationException {
        List<String> groupByProperties = new ArrayList<>();
        for (GroupByItem item : groupBy.getGroupByItems()) {
            List<UriResource> path = item.getPath();
            if (path.size() > 1) {
                throwNotImplemented("Grouping by navigation property is not supported yet.");
            }
            UriResource resource = path.get(0);
            if (resource.getKind() == UriResourceKind.primitiveProperty) {
                groupByProperties.add(resource.getSegmentValue());
            } else {
                throwNotImplemented("Grouping by complex type is not supported yet.");
            }
        }
        return groupByProperties;
    }

    /**
     * Get's list of orders for first query, because it has metrics aggregations
     * and also can have count and term order.
     * 
     * @param field
     *            query field
     * @param ordersMap
     *            orders map from URI
     * @return list of orders
     */
    private List<BucketOrder> getQueryOrders(String field, Map<String, Boolean> ordersMap) {
        List<BucketOrder> orders = new ArrayList<>();
        Boolean termOrder = ordersMap.remove(field);
        if (termOrder != null) {
            orders.add(BucketOrder.key(termOrder));
        }
        Boolean countOrder = ordersMap.remove(getCountAlias());
        if (countOrder != null) {
            orders.add(BucketOrder.count(countOrder));
        }
        for (String alias : getMetricAliases()) {
            Boolean aliasOrder = ordersMap.remove(alias);
            if (aliasOrder != null) {
                orders.add(BucketOrder.aggregation(alias, aliasOrder));
            }
        }
        return orders;
    }

    /**
     * Gets field for 'term' aggregation query by property from entity type.
     * 
     * @param propertyName
     *            property name
     * @param entityType
     *            entity type
     * @return field for query
     */
    private static String getQueryField(String propertyName, ElasticEdmEntityType entityType) {
        ElasticEdmProperty property = entityType.getESProperties().get(propertyName);
        return addKeywordIfNeeded(property.getEField(), property.getAnnotations());
    }

}
