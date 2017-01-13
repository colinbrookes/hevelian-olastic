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
import com.hevelian.olastic.core.elastic.pagination.Pagination;
import com.hevelian.olastic.core.elastic.queries.AggregateQuery;
import com.hevelian.olastic.core.elastic.queries.Query;
import com.hevelian.olastic.core.elastic.requests.AggregateRequest;
import com.hevelian.olastic.core.elastic.requests.BaseRequest;

/**
 * Class responsible for creating {@link AggregateRequest} instance for buckets
 * aggregations with metrics.
 * 
 * @author rdidyk
 */
public class BucketsAggregationsRequestCreator extends AbstractAggregationsRequestCreator {

    @Override
    public AggregateRequest create(UriInfo uriInfo) throws ODataApplicationException {
        BaseRequest baseRequest = super.create(uriInfo);
        ElasticEdmEntitySet entitySet = baseRequest.getEntitySet();
        ElasticEdmEntityType entityType = entitySet.getEntityType();

        List<GroupBy> groupByItems = getGroupByItems(uriInfo.getApplyOption());
        if (groupByItems.size() > 1) {
            throwNotImplemented("Combining Transformations per Group is not supported.");
        }
        List<AggregationBuilder> bucketsQueries = getGroupByQueries(groupByItems.get(0), entityType,
                getPagination(uriInfo));

        Query baseQuery = baseRequest.getQuery();
        AggregateQuery aggregateQuery = new AggregateQuery(baseQuery.getIndex(),
                baseQuery.getType(), baseQuery.getQueryBuilder(), bucketsQueries,
                Collections.emptyList());
        return new AggregateRequest(aggregateQuery, entitySet, getCountAlias());
    }

    /**
     * Get's fields from {@link #groupBy} for aggregation query.
     *
     * @param groupBy
     *            groupBy instance
     * @param entityType
     *            entity type
     * @param pagination
     *            pagination
     * @return list of fields
     * @throws ODataApplicationException
     */
    protected List<AggregationBuilder> getGroupByQueries(GroupBy groupBy,
            ElasticEdmEntityType entityType, Pagination pagination)
            throws ODataApplicationException {
        List<String> fields = getFields(groupBy);
        Collections.reverse(fields);
        // Last because of reverse
        String lastField = fields.remove(0);
        TermsAggregationBuilder groupByQuery = AggregationBuilders.terms(lastField)
                .field(addKeywordIfNeeded(lastField, entityType));
        groupByQuery.size(pagination.getTop() + pagination.getSkip());
        getMetricsAggQueries(getAggregations(groupBy.getApplyOption()), entityType)
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
    protected List<String> getFields(GroupBy groupBy) throws ODataApplicationException {
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
