package com.hevelian.olastic.core.elastic.requests.creators;

import static com.hevelian.olastic.core.elastic.utils.AggregationUtils.getAggQuery;
import static com.hevelian.olastic.core.utils.ProcessorUtils.throwNotImplemented;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.olingo.commons.api.ex.ODataRuntimeException;
import org.apache.olingo.server.api.ODataApplicationException;
import org.apache.olingo.server.api.uri.UriResource;
import org.apache.olingo.server.api.uri.UriResourceKind;
import org.apache.olingo.server.api.uri.queryoption.apply.Aggregate;
import org.apache.olingo.server.api.uri.queryoption.apply.AggregateExpression;
import org.apache.olingo.server.api.uri.queryoption.expression.Expression;
import org.apache.olingo.server.api.uri.queryoption.expression.ExpressionVisitException;
import org.elasticsearch.search.aggregations.AggregationBuilder;

import com.hevelian.olastic.core.api.uri.queryoption.expression.ElasticSearchExpressionVisitor;
import com.hevelian.olastic.core.api.uri.queryoption.expression.member.impl.PrimitiveMember;
import com.hevelian.olastic.core.edm.ElasticEdmEntityType;
import com.hevelian.olastic.core.elastic.builders.ESQueryBuilder;

/**
 * Class with common logic for all request creators with aggregations queries.
 * 
 * @author rdidyk
 */
public abstract class AbstractAggregationsRequestCreator extends AbstractRequestCreator {

    /** Name of count property. */
    private String countAlias;

    /**
     * Default constructor.
     */
    public AbstractAggregationsRequestCreator() {
        super();
    }

    /**
     * Constructor to initialize ES query builder.
     * 
     * @param queryBuilder
     *            ES query builder
     */
    public AbstractAggregationsRequestCreator(ESQueryBuilder<?> queryBuilder) {
        super(queryBuilder);
    }

    /**
     * Get's and creates metrics aggregation queries from {@link Aggregate} in
     * URL.
     *
     * @param entityType
     *            entity type
     * @return list of queries
     * @throws ODataApplicationException
     *             if any error occurred
     */
    protected List<AggregationBuilder> getMetricsAggQueries(List<Aggregate> aggregations,
            ElasticEdmEntityType entityType) throws ODataApplicationException {
        List<AggregateExpression> expressions = aggregations.stream()
                .flatMap(agg -> agg.getExpressions().stream()).collect(Collectors.toList());
        List<AggregationBuilder> aggs = new ArrayList<>();
        for (AggregateExpression aggExpression : expressions) {
            try {
                if (aggExpression.getInlineAggregateExpression() != null) {
                    throwNotImplemented(
                            "Aggregate for navigation or complex type fields is not supported.");
                }
                String alias = aggExpression.getAlias();
                Expression expr = aggExpression.getExpression();
                if (expr != null) {
                    String field = ((PrimitiveMember) expr
                            .accept(new ElasticSearchExpressionVisitor())).getField();
                    String fieldName = entityType.getEProperties().get(field).getEField();
                    aggs.add(getAggQuery(aggExpression.getStandardMethod(), alias, fieldName));
                } else {
                    List<UriResource> path = aggExpression.getPath();
                    if (path.size() > 1) {
                        throwNotImplemented(
                                "Aggregate for navigation or complex type fields is not supported.");
                    }
                    UriResource resource = path.get(0);
                    if (resource.getKind() == UriResourceKind.count) {
                        countAlias = alias;
                    }
                }
            } catch (ExpressionVisitException e) {
                throw new ODataRuntimeException(e);
            }
        }
        return aggs;
    }

    public String getCountAlias() {
        return countAlias;
    }

}
