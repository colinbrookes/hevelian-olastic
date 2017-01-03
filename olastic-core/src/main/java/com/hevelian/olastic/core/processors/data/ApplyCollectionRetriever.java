package com.hevelian.olastic.core.processors.data;

import static com.hevelian.olastic.core.elastic.utils.AggregationUtils.getAggQuery;
import static com.hevelian.olastic.core.elastic.utils.ElasticUtils.addKeywordIfNeeded;
import static com.hevelian.olastic.core.utils.ApplyOptionUtils.getAggregations;
import static com.hevelian.olastic.core.utils.ApplyOptionUtils.getGroupByItems;
import static com.hevelian.olastic.core.utils.ProcessorUtils.throwNotImplemented;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;

import org.apache.olingo.commons.api.data.Entity;
import org.apache.olingo.commons.api.data.EntityCollection;
import org.apache.olingo.commons.api.data.Property;
import org.apache.olingo.commons.api.data.ValueType;
import org.apache.olingo.commons.api.format.ContentType;
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
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.PipelineAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms.Bucket;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.NumericMetricsAggregation.SingleValue;

import com.hevelian.olastic.core.ElasticOData;
import com.hevelian.olastic.core.ElasticServiceMetadata;
import com.hevelian.olastic.core.api.uri.queryoption.expression.ElasticSearchExpressionVisitor;
import com.hevelian.olastic.core.api.uri.queryoption.expression.member.impl.PrimitiveMember;
import com.hevelian.olastic.core.edm.ElasticEdmEntitySet;
import com.hevelian.olastic.core.edm.ElasticEdmEntityType;
import com.hevelian.olastic.core.elastic.ESClient;
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

    private String countAlias;
    private GroupBy groupBy;
    private List<Aggregate> aggregations;

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
     * @param applyOption
     *            apply option
     * @throws ODataApplicationException
     */
    public ApplyCollectionRetriever(UriInfo uriInfo, ElasticOData odata, Client client,
            String rawBaseUri, ElasticServiceMetadata serviceMetadata, ContentType responseFormat,
            ApplyOption applyOption) throws ODataApplicationException {
        super(uriInfo, odata, client, rawBaseUri, serviceMetadata, responseFormat);
        initializeValues(applyOption);
    }

    private void initializeValues(ApplyOption applyOption) throws ODataApplicationException {
        List<GroupBy> groupByItems = getGroupByItems(applyOption);
        if (!groupByItems.isEmpty()) {
            if (groupByItems.size() > 1) {
                throwNotImplemented("Combining Transformations per Group is not supported.");
            }
            this.groupBy = groupByItems.get(0);
        }
        this.aggregations = getAggregations(applyOption);
    }

    @Override
    public SerializerResult getSerializedData() throws ODataApplicationException {
        if (isCount()) {
            throwNotImplemented("Count option for groupby is not implemented.");
        }
        QueryWithEntity queryWithEntity = getQueryWithEntity();
        ElasticEdmEntitySet entitySet = queryWithEntity.getEntitySet();
        ESQueryBuilder queryBuilder = queryWithEntity.getQuery();

        ElasticEdmEntityType entityType = entitySet.getEntityType();
        EntityCollection entities = new EntityCollection();
        SearchResponse searchResponse;
        if (isAggregateOnly()) {
            searchResponse = retrieveData(queryBuilder,
                    getSimpleAggQueries(aggregations, entityType));
            entities.getEntities().add(getAggregatedEntity(searchResponse, entityType));
        } else if (isGroupByOnly()) {
            searchResponse = retrieveData(queryBuilder,
                    getGroupByQueries(groupBy, entitySet.getEntityType()));
            entities.getEntities().addAll(getAggregatedEntities(
                    searchResponse.getAggregations().asMap(), null, entityType));
        } else if (isGroupByAndAggregate()) {
            searchResponse = retrieveData(queryBuilder,
                    getGroupByQueries(groupBy, entitySet.getEntityType()),
                    getPipelineAggQuery(aggregations, entityType));
            entities.getEntities().add(getAggregatedEntity(searchResponse, entityType));
        } else {
            // TODO Implement support of other apply system query options
            return super.getSerializedData();
        }
        return serializeEntities(entities, entitySet);
    }

    /**
     * Gets the data from ES.
     *
     * @param query
     *            query builder
     * @param aggs
     *            aggregations queries list
     * @return ES response
     * @throws ODataApplicationException
     *             if any error occurred
     */
    protected SearchResponse retrieveData(ESQueryBuilder query, List<AggregationBuilder> aggs)
            throws ODataApplicationException {
        return retrieveData(query, aggs, Collections.emptyList());
    }

    /**
     * Gets the data from ES.
     *
     * @param query
     *            query builder
     * @param aggs
     *            aggregations queries list
     * @param pipelineAggs
     *            pipeline aggregation queries
     * @return ES response
     * @throws ODataApplicationException
     *             if any error occurred
     */
    protected SearchResponse retrieveData(ESQueryBuilder query, List<AggregationBuilder> aggs,
            List<PipelineAggregationBuilder> pipelineAggs) throws ODataApplicationException {
        return ESClient.executeRequest(
                query.getIndex(), query.getType(), getClient(), new BoolQueryBuilder()
                        .filter(query.getQuery()).filter(getFilterQuery()).filter(getSearchQuery()),
                aggs, pipelineAggs);
    }

    /**
     * Get's and creates aggregation queries from {@link Aggregate} in URL.
     * 
     * @param entityType
     *            entity type
     * @return list of queries
     * @throws ODataApplicationException
     *             if any error occurred
     */
    protected List<AggregationBuilder> getSimpleAggQueries(List<Aggregate> aggregations,
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
    protected Entity getAggregatedEntity(SearchResponse response, ElasticEdmEntityType entityType) {
        Entity entity = new Entity();
        Aggregations aggs = response.getAggregations();
        if (aggs != null) {
            aggs.asList().stream().filter(SingleValue.class::isInstance)
                    .map(SingleValue.class::cast)
                    .forEach(aggr -> addProperty(entity, aggr.getName(), aggr.value(), entityType));
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
    protected List<Entity> getAggregatedEntities(Map<String, Aggregation> aggs, Entity parent,
            ElasticEdmEntityType entityType) {
        List<Entity> entities = new ArrayList<>();
        for (Entry<String, Terms> entry : collectTerms(aggs).entrySet()) {
            for (Bucket bucket : entry.getValue().getBuckets()) {
                Entity entity = new Entity();
                if (parent != null) {
                    entity.getProperties().addAll(parent.getProperties());
                }
                addProperty(entity, entry.getKey(), bucket.getKey(), entityType);
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
        return aggregations.entrySet().stream().filter(e -> e.getValue() instanceof Terms)
                .collect(Collectors.toMap(e -> e.getKey(), e -> (Terms) e.getValue()));
    }

    private void addAggsAndCountIfNeeded(Map<String, Aggregation> aggregations, long count,
            Entity entity, ElasticEdmEntityType entityType) {
        aggregations.entrySet().stream().filter(e -> e.getValue() instanceof SingleValue)
                .forEach(e -> addProperty(entity, e.getKey(), ((SingleValue) e.getValue()).value(),
                        entityType));
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
    protected List<AggregationBuilder> getGroupByQueries(GroupBy groupBy,
            ElasticEdmEntityType entityType) throws ODataApplicationException {
        List<String> fields = getFields();
        Collections.reverse(fields);
        // Last because of reverse
        String lastField = fields.remove(0);
        TermsAggregationBuilder groupByQuery = AggregationBuilders.terms(lastField)
                .field(addKeywordIfNeeded(lastField, entityType));
        groupByQuery.size(getPagination().getTop() + getPagination().getSkip());
        getSimpleAggQueries(getAggregations(groupBy.getApplyOption()), entityType)
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
     * @return list of fields
     * @throws ODataApplicationException
     */
    private List<String> getFields() throws ODataApplicationException {
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

    /**
     * Get's and creates Pipeline Aggregation queries from {@link Aggregate} in
     * URL.
     * 
     * @param aggregations
     *            aggregations from URL
     * @param entityType
     *            entity type
     * @return list of queries
     * @throws ODataApplicationException
     *             if any error occurred
     */
    protected List<PipelineAggregationBuilder> getPipelineAggQuery(List<Aggregate> aggregations,
            ElasticEdmEntityType entityType) throws ODataApplicationException {
        throwNotImplemented("Aggregation for grouped and aggregated data is not implemented.");
        return new ArrayList<>();
    }

    private boolean isAggregateOnly() {
        return groupBy == null && !aggregations.isEmpty();
    }

    private boolean isGroupByOnly() {
        return groupBy != null && aggregations.isEmpty();
    }

    private boolean isGroupByAndAggregate() {
        return groupBy != null && !aggregations.isEmpty();
    }
}
