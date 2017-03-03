package com.hevelian.olastic.core.elastic.requests.creators;

import static com.hevelian.olastic.core.elastic.utils.ElasticUtils.addKeywordIfNeeded;
import static com.hevelian.olastic.core.utils.ApplyOptionUtils.getAggregations;
import static com.hevelian.olastic.core.utils.ApplyOptionUtils.getGroupByItems;
import static com.hevelian.olastic.core.utils.ProcessorUtils.throwNotImplemented;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.olingo.server.api.ODataApplicationException;
import org.apache.olingo.server.api.uri.UriInfo;
import org.apache.olingo.server.api.uri.UriResource;
import org.apache.olingo.server.api.uri.UriResourceKind;
import org.apache.olingo.server.api.uri.queryoption.apply.GroupBy;
import org.apache.olingo.server.api.uri.queryoption.apply.GroupByItem;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;

import com.hevelian.olastic.core.edm.ElasticEdmEntitySet;
import com.hevelian.olastic.core.edm.ElasticEdmEntityType;
import com.hevelian.olastic.core.elastic.builders.ESQueryBuilder;
import com.hevelian.olastic.core.elastic.pagination.Pagination;
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
        List<String> fields = getFields(groupBy);
        Collections.reverse(fields);
        // Last because of reverse
        String lastField = fields.remove(0);
        // TODO Implement orderby for aggregations.
        TermsAggregationBuilder groupByQuery = AggregationBuilders.terms(lastField)
                .field(addKeywordIfNeeded(lastField, entityType))
                .size(pagination.getSkip() + pagination.getTop());
        getMetricsAggQueries(getAggregations(groupBy.getApplyOption()))
                .forEach(groupByQuery::subAggregation);
        for (String field : fields) {
            groupByQuery = AggregationBuilders.terms(field)
                    .field(addKeywordIfNeeded(field, entityType)).subAggregation(groupByQuery);
        }
        // For now only one 'groupby' is supported and returned in the list
        return Arrays.asList(groupByQuery);
    }

    /**
     * Get's fields from {@link #groupBy} for aggregation query.
     * 
     * @param groupBy
     *            groupBy instance
     * @return list of fields
     * @throws ODataApplicationException
     */
    private List<String> getFields(GroupBy groupBy) throws ODataApplicationException {
        List<String> groupByFields = new ArrayList<>();
        for (GroupByItem item : groupBy.getGroupByItems()) {
            List<UriResource> path = item.getPath();
            if (path.size() > 1) {
                throwNotImplemented("Grouping by navigation property is not supported yet.");
            }
            UriResource resource = path.get(0);
            if (resource.getKind() == UriResourceKind.primitiveProperty) {
                groupByFields.add(resource.getSegmentValue());
            } else {
                throwNotImplemented("Grouping by complex type is not supported yet.");
            }
        }
        return groupByFields;
    }

}
