package com.hevelian.olastic.core.elastic.requests.creators;

import com.hevelian.olastic.core.edm.ElasticEdmEntitySet;
import com.hevelian.olastic.core.elastic.builders.ESQueryBuilder;
import com.hevelian.olastic.core.elastic.queries.AggregateQuery;
import com.hevelian.olastic.core.elastic.queries.Query;
import com.hevelian.olastic.core.elastic.requests.AggregateRequest;
import com.hevelian.olastic.core.elastic.requests.ESRequest;
import org.apache.olingo.server.api.ODataApplicationException;
import org.apache.olingo.server.api.uri.UriInfo;
import org.apache.olingo.server.api.uri.queryoption.apply.Aggregate;
import org.elasticsearch.search.aggregations.AggregationBuilder;

import java.util.Collections;
import java.util.List;

import static com.hevelian.olastic.core.utils.ApplyOptionUtils.getAggregations;

/**
 * Class responsible for creating {@link AggregateRequest} instance for metrics
 * aggregations.
 * 
 * @author rdidyk
 */
public class MetricsAggregationsRequestCreator extends AbstractAggregationsRequestCreator {

    /**
     * Default constructor.
     */
    public MetricsAggregationsRequestCreator() {
        super();
    }

    /**
     * Constructor to initialize ES query builder.
     * 
     * @param queryBuilder
     *            ES query builder
     */
    public MetricsAggregationsRequestCreator(ESQueryBuilder<?> queryBuilder) {
        super(queryBuilder);
    }

    @Override
    public AggregateRequest create(UriInfo uriInfo) throws ODataApplicationException {
        ESRequest baseRequestInfo = getBaseRequestInfo(uriInfo);
        Query baseQuery = baseRequestInfo.getQuery();
        ElasticEdmEntitySet entitySet = baseRequestInfo.getEntitySet();

        List<Aggregate> aggregations = getAggregations(uriInfo.getApplyOption());
        List<AggregationBuilder> metricsQueries = getMetricsAggQueries(aggregations);

        AggregateQuery aggregateQuery = new AggregateQuery(baseQuery.getIndex(),
                baseQuery.getTypes(), baseQuery.getQueryBuilder(), metricsQueries,
                Collections.emptyList());
        return new AggregateRequest(aggregateQuery, entitySet, getCountAlias());
    }

}
