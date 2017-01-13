package com.hevelian.olastic.core.elastic.requests.creators;

import static com.hevelian.olastic.core.utils.ApplyOptionUtils.getAggregations;

import java.util.Collections;
import java.util.List;

import org.apache.olingo.server.api.ODataApplicationException;
import org.apache.olingo.server.api.uri.UriInfo;
import org.apache.olingo.server.api.uri.queryoption.apply.Aggregate;
import org.elasticsearch.search.aggregations.AggregationBuilder;

import com.hevelian.olastic.core.edm.ElasticEdmEntitySet;
import com.hevelian.olastic.core.edm.ElasticEdmEntityType;
import com.hevelian.olastic.core.elastic.queries.AggregateQuery;
import com.hevelian.olastic.core.elastic.queries.Query;
import com.hevelian.olastic.core.elastic.requests.AggregateRequest;
import com.hevelian.olastic.core.elastic.requests.BaseRequest;

/**
 * Class responsible for creating {@link AggregateRequest} instance for metrics
 * aggregations.
 * 
 * @author rdidyk
 */
public class MetricsAggregationsRequestCreator extends AbstractAggregationsRequestCreator {

    @Override
    public AggregateRequest create(UriInfo uriInfo) throws ODataApplicationException {
        BaseRequest baseRequest = super.create(uriInfo);
        ElasticEdmEntitySet entitySet = baseRequest.getEntitySet();
        ElasticEdmEntityType entityType = entitySet.getEntityType();

        List<Aggregate> aggregations = getAggregations(uriInfo.getApplyOption());
        List<AggregationBuilder> metricsQueries = getMetricsAggQueries(aggregations, entityType);

        Query baseQuery = baseRequest.getQuery();
        AggregateQuery aggregateQuery = new AggregateQuery(baseQuery.getIndex(),
                baseQuery.getType(), baseQuery.getQueryBuilder(), metricsQueries,
                Collections.emptyList());
        return new AggregateRequest(aggregateQuery, entitySet, getCountAlias());
    }

}
