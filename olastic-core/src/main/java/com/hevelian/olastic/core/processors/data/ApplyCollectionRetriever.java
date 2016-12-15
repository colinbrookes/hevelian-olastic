package com.hevelian.olastic.core.processors.data;

import static com.hevelian.olastic.core.elastic.utils.AggregationUtils.getAggQuery;
import static com.hevelian.olastic.core.elastic.utils.AggregationUtils.getAggregations;
import static com.hevelian.olastic.core.elastic.utils.AggregationUtils.getGroupByItems;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.olingo.commons.api.data.Entity;
import org.apache.olingo.commons.api.data.EntityCollection;
import org.apache.olingo.commons.api.data.Property;
import org.apache.olingo.commons.api.data.ValueType;
import org.apache.olingo.commons.api.format.ContentType;
import org.apache.olingo.commons.api.http.HttpStatusCode;
import org.apache.olingo.server.api.ODataApplicationException;
import org.apache.olingo.server.api.serializer.SerializerResult;
import org.apache.olingo.server.api.uri.UriInfo;
import org.apache.olingo.server.api.uri.UriResource;
import org.apache.olingo.server.api.uri.UriResourceKind;
import org.apache.olingo.server.api.uri.queryoption.ApplyOption;
import org.apache.olingo.server.api.uri.queryoption.apply.Aggregate;
import org.apache.olingo.server.api.uri.queryoption.apply.AggregateExpression;
import org.apache.olingo.server.api.uri.queryoption.apply.GroupBy;
import org.apache.olingo.server.api.uri.queryoption.apply.GroupByItem;
import org.apache.olingo.server.api.uri.queryoption.expression.Expression;
import org.apache.olingo.server.api.uri.queryoption.expression.ExpressionVisitException;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.aggregations.AbstractAggregationBuilder;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms.Bucket;
import org.elasticsearch.search.aggregations.bucket.terms.TermsBuilder;
import org.elasticsearch.search.aggregations.metrics.NumericMetricsAggregation.SingleValue;

import com.hevelian.olastic.core.ElasticOData;
import com.hevelian.olastic.core.ElasticServiceMetadata;
import com.hevelian.olastic.core.api.uri.queryoption.expression.ElasticSearchExpressionVisitor;
import com.hevelian.olastic.core.edm.ElasticEdmEntitySet;
import com.hevelian.olastic.core.edm.ElasticEdmEntityType;
import com.hevelian.olastic.core.elastic.builders.ESQueryBuilder;

import lombok.extern.log4j.Log4j2;

/**
 * Provides high-level methods for retrieving and converting the data for
 * collection of entities with groupby and aggregate query.
 * 
 * @author rdidyk
 */
@Log4j2
public class ApplyCollectionRetriever extends EntityCollectionRetriever {

    private GroupBy groupBy;
    private String countAlias;
    private Aggregate aggregate;

    /**
     * Fully initializes {@link ApplyCollectionRetriever}.
     *
     * @param uriInfo
     *            uriInfo object
     * @param odata
     *            odata instance
     * @param client
     *            ES raw client
     * @param rawBaseUri
     *            war base uri
     * @param serviceMetadata
     *            service metadata
     * @param responseFormat
     *            response format
     * @param groupBy
     *            group by instance
     * @throws ODataApplicationException
     */
    public ApplyCollectionRetriever(UriInfo uriInfo, ElasticOData odata, Client client,
            String rawBaseUri, ElasticServiceMetadata serviceMetadata, ContentType responseFormat,
            ApplyOption applyOption) throws ODataApplicationException {
        super(uriInfo, odata, client, rawBaseUri, serviceMetadata, responseFormat);
        List<Aggregate> aggregations = getAggregations(applyOption);
        List<GroupBy> groupByItems = getGroupByItems(applyOption);
        if (!groupByItems.isEmpty()) {
            if (groupByItems.size() > 1) {
                throw new ODataApplicationException("Only one 'groupBy' is supported.",
                        HttpStatusCode.NOT_IMPLEMENTED.getStatusCode(), Locale.ROOT);
            }
            groupBy = groupByItems.get(0);
        } else if (!aggregations.isEmpty()) {
            aggregate = aggregations.get(0);
        }
    }

    @Override
    public SerializerResult getSerializedData() throws ODataApplicationException {
        if (isCount()) {
            throw new ODataApplicationException("Count option for groupby is not implemented.",
                    HttpStatusCode.NOT_IMPLEMENTED.getStatusCode(), Locale.ROOT);
        }
        QueryWithEntity queryWithEntity = getQueryWithEntity();
        ElasticEdmEntitySet entitySet = queryWithEntity.getEntitySet();
        ESQueryBuilder queryBuilder = queryWithEntity.getQuery();

        ElasticEdmEntityType entityType = entitySet.getEntityType();
        QueryBuilder filter = getFilterQuery();
        EntityCollection entities = new EntityCollection();
        SearchResponse searchResponse;
        if (isAggregateOnly()) {
            searchResponse = retrieveData(queryBuilder, filter,
                    getSimpleAggQuery(aggregate, entityType));
            entities.getEntities().add(getAggregatedEntity(searchResponse, entityType));
        } else if (isGroupByOnly()) {
            searchResponse = retrieveData(queryBuilder, filter,
                    Arrays.asList(getGroupByQuery(groupBy, entitySet.getEntityType())));
            entities.getEntities().addAll(getAggregatedEntities(
                    searchResponse.getAggregations().asMap(), null, entityType));

        } else {
            // TODO Implement
        }
        return serializeEntities(entities, entitySet);

    }

    /**
     * Get's and creates aggregation queries from {@link Aggregate} in URL.1
     * 
     * @param aggregate
     *            aggregate from URL
     * @param entityType
     *            entity type
     * @return list of queries
     * @throws ODataApplicationException
     *             if any error occurred
     */
    protected List<AbstractAggregationBuilder> getSimpleAggQuery(Aggregate aggregate,
            ElasticEdmEntityType entityType) throws ODataApplicationException {
        List<AbstractAggregationBuilder> aggs = new ArrayList<>();
        for (AggregateExpression aggExpression : aggregate.getExpressions()) {
            try {
                String alias = aggExpression.getAlias();
                Expression expr = aggExpression.getExpression();
                if (expr != null) {
                    Object field = expr.accept(new ElasticSearchExpressionVisitor());
                    String fieldName = entityType.getEProperties().get(field).getEField();
                    aggs.add(getAggQuery(aggExpression.getStandardMethod(), alias, fieldName));
                } else {
                    List<UriResource> path = aggExpression.getPath();
                    if (path.size() > 1) {
                        throw new ODataApplicationException(
                                "Aggregate for navigation or complex type fields is not supported yet.",
                                HttpStatusCode.NOT_IMPLEMENTED.getStatusCode(), Locale.ROOT);
                    }
                    UriResource resource = path.get(0);
                    if (resource.getKind() == UriResourceKind.count) {
                        countAlias = alias;
                        continue;
                    }
                }
            } catch (ExpressionVisitException e) {
                log.debug(e);
            }
        }
        return aggs;
    }

    /**
     * Created {@link Entity} from Elasticsearch search response.
     * 
     * @param response
     *            search response
     * @param entityType
     *            entity type
     * @return created entity
     */
    private Entity getAggregatedEntity(SearchResponse response, ElasticEdmEntityType entityType) {
        Entity entity = new Entity();
        Aggregations aggregations = response.getAggregations();
        if (aggregations != null) {
            for (Entry<String, Aggregation> entry : aggregations.asMap().entrySet()) {
                Aggregation value = entry.getValue();
                if (value instanceof SingleValue) {
                    addProperty(entity, value.getName(), ((SingleValue) value).value(), entityType);
                }
            }
        }
        addCountIfNeeded(entity, response.getHits().getTotalHits());
        return entity;
    }

    /**
     * Method recursively goes through aggregations, creates entities and adds
     * fields to them. When entity has all fields from aggregations it adds to
     * entities list. If {@link #groupBy} has aggregation
     * {@link UriResourceKind}.count then property with {@link #countAlias} name
     * will be added to entity with doc count from response aggregations.
     * 
     * @param aggs
     *            response aggregations
     * @param parent
     *            parent entity
     * @param entityType
     *            entity type
     * @return list of entities
     */
    private List<Entity> getAggregatedEntities(Map<String, Aggregation> aggs, Entity parent,
            ElasticEdmEntityType entityType) {
        List<Entity> entities = new ArrayList<>();
        for (Entry<String, Terms> entry : collectTerms(aggs).entrySet()) {
            for (Bucket bucket : entry.getValue().getBuckets()) {
                Entity entity = new Entity();
                if (parent != null) {
                    entity.getProperties().addAll(parent.getProperties());
                }
                addProperty(entity, entityType.findPropertyByEField(entry.getKey()).getName(),
                        bucket.getKey(), entityType);
                Map<String, Aggregation> subAggs = bucket.getAggregations().asMap();
                if (subAggs.isEmpty()) {
                    addAggsAndCountIfNeeded(aggs, bucket.getDocCount(), entity, entityType);
                    entities.add(entity);
                } else {
                    List<Entity> subEntities = getAggregatedEntities(subAggs, entity, entityType);
                    if (subEntities.isEmpty()) {
                        addAggsAndCountIfNeeded(subAggs, bucket.getDocCount(), entity, entityType);
                        entities.add(entity);
                    } else {
                        entities.addAll(subEntities);
                    }
                }
            }
        }
        return entities;
    }

    private static Map<String, Terms> collectTerms(Map<String, Aggregation> aggregations) {
        Map<String, Terms> terms = new HashMap<>();
        for (Entry<String, Aggregation> entry : aggregations.entrySet()) {
            if (entry.getValue() instanceof Terms) {
                terms.put(entry.getKey(), (Terms) entry.getValue());
            }
        }
        return terms;
    }

    private void addAggsAndCountIfNeeded(Map<String, Aggregation> aggregations, long count,
            Entity entity, ElasticEdmEntityType entityType) {
        for (Entry<String, Aggregation> entry : aggregations.entrySet()) {
            Aggregation value = entry.getValue();
            if (value instanceof SingleValue) {
                addProperty(entity, entry.getKey(), ((SingleValue) value).value(), entityType);
            }
        }
        addCountIfNeeded(entity, count);
    }

    /**
     * Creates {@link Property} with {@link #countAlias} name if $count was in
     * URL.
     * 
     * @param entity
     *            parent entity
     * @param count
     *            count value
     */
    private void addCountIfNeeded(Entity entity, long count) {
        if (countAlias != null) {
            entity.addProperty(new Property(null, countAlias, ValueType.PRIMITIVE, count));
        }
    }

    /**
     * Get's fields from {@link #groupBy} for aggregation query.
     * 
     * @param groupBy
     * 
     * @param entityType
     *            entity type
     * @return list of fields
     * @throws ODataApplicationException
     */
    private AbstractAggregationBuilder getGroupByQuery(GroupBy groupBy,
            ElasticEdmEntityType entityType) throws ODataApplicationException {
        List<String> fields = getFields(entityType);
        Collections.reverse(fields);
        // Last because of reverse
        String lastField = fields.remove(0);
        AggregationBuilder<TermsBuilder> groupByQuery = AggregationBuilders.terms(lastField)
                .field(lastField);
        addGroupByAggs(groupBy.getApplyOption(), groupByQuery, entityType);
        for (String field : fields) {
            groupByQuery = AggregationBuilders.terms(field).field(field)
                    .subAggregation(groupByQuery);
        }
        return groupByQuery;
    }

    /**
     * Get's fields from {@link #groupBy} for aggregation query.
     * 
     * @param entityType
     *            entity type
     * @return list of fields
     * @throws ODataApplicationException
     */
    private List<String> getFields(ElasticEdmEntityType entityType)
            throws ODataApplicationException {
        List<String> groupByFields = new ArrayList<>();
        for (GroupByItem item : groupBy.getGroupByItems()) {
            List<UriResource> path = item.getPath();
            if (path.size() > 1) {
                throw new ODataApplicationException(
                        "Groupby navigation or complex type fields is not supported yet.",
                        HttpStatusCode.NOT_IMPLEMENTED.getStatusCode(), Locale.ROOT);
            }
            UriResource resource = path.get(0);
            if (resource.getKind() == UriResourceKind.primitiveProperty) {
                groupByFields.add(
                        entityType.getEProperties().get(resource.getSegmentValue()).getEField());
            }
        }
        return groupByFields;
    }

    /**
     * Add's aggregations to groupby query.
     * 
     * @param apply
     *            groupby apply option
     * @param groupByQuery
     *            groupby query
     * @param entityType
     *            entity type
     * @throws ODataApplicationException
     *             if any error occurred
     */
    private void addGroupByAggs(ApplyOption apply, AggregationBuilder<TermsBuilder> groupByQuery,
            ElasticEdmEntityType entityType) throws ODataApplicationException {
        for (Aggregate agg : getAggregations(apply)) {
            for (AbstractAggregationBuilder aggQuery : getSimpleAggQuery(agg, entityType)) {
                groupByQuery.subAggregation(aggQuery);
            }
        }
    }

    private boolean isAggregateOnly() {
        return groupBy == null && aggregate != null;
    }

    private boolean isGroupByOnly() {
        return groupBy != null && aggregate == null;
    }

}
