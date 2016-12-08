package com.hevelian.olastic.core.processors.data;

import java.util.ArrayList;
import java.util.Collections;
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
import org.apache.olingo.server.api.uri.queryoption.ApplyItem;
import org.apache.olingo.server.api.uri.queryoption.ApplyOption;
import org.apache.olingo.server.api.uri.queryoption.apply.Aggregate;
import org.apache.olingo.server.api.uri.queryoption.apply.AggregateExpression;
import org.apache.olingo.server.api.uri.queryoption.apply.GroupBy;
import org.apache.olingo.server.api.uri.queryoption.apply.GroupByItem;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.aggregations.AbstractAggregationBuilder;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms.Bucket;

import com.hevelian.olastic.core.ElasticOData;
import com.hevelian.olastic.core.ElasticServiceMetadata;
import com.hevelian.olastic.core.edm.ElasticEdmEntitySet;
import com.hevelian.olastic.core.edm.ElasticEdmEntityType;
import com.hevelian.olastic.core.elastic.ESClient;
import com.hevelian.olastic.core.elastic.builders.ESQueryBuilder;

/**
 * Provides high-level methods for retrieving and converting the data for
 * collection of entities with group by query.
 * 
 * @author rdidyk
 */
public class GroupByCollectionRetriever extends EntityCollectionRetriever {

    private GroupBy groupBy;
    private String countAlias;

    /**
     * Fully initializes {@link GroupByCollectionRetriever}.
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
     */
    public GroupByCollectionRetriever(UriInfo uriInfo, ElasticOData odata, Client client,
            String rawBaseUri, ElasticServiceMetadata serviceMetadata, ContentType responseFormat,
            GroupBy groupBy) {
        super(uriInfo, odata, client, rawBaseUri, serviceMetadata, responseFormat);
        this.groupBy = groupBy;
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
        List<String> fields = getFields(entitySet.getEntityType());
        AbstractAggregationBuilder aggrQuery = getQuery(fields);

        SearchResponse searchResponse = retrieveData(queryBuilder, filter, aggrQuery);
        EntityCollection entities = new EntityCollection();
        entities.getEntities().addAll(
                getAggregatedEntities(searchResponse.getAggregations().asMap(), null, entityType));
        return serializeEntities(entities, entitySet);
    }

    /**
     * Builds aggregation query based on fields in URL.
     * 
     * @param fields
     *            fields for group by
     * @return created query
     */
    protected AbstractAggregationBuilder getQuery(List<String> fields) {
        AbstractAggregationBuilder aggregationQuery = null;
        Collections.reverse(fields);
        for (String field : fields) {
            if (aggregationQuery == null) {
                aggregationQuery = AggregationBuilders.terms(field).field(field);
            } else {
                aggregationQuery = AggregationBuilders.terms(field).field(field)
                        .subAggregation(aggregationQuery);
            }
        }
        return aggregationQuery;
    }

    /**
     * Gets the data from ES.
     *
     * @param query
     *            query builder
     * @param filter
     *            raw ES query with filter
     * @return ES response
     * @throws ODataApplicationException
     *             if any error occurred
     */
    protected SearchResponse retrieveData(ESQueryBuilder query, QueryBuilder filter,
            AbstractAggregationBuilder aggregation) throws ODataApplicationException {
        return ESClient.executeRequest(query.getIndex(), query.getType(), getClient(),
                new BoolQueryBuilder().filter(query.getQuery()).filter(filter), aggregation);
    }

    /**
     * Method recursively goes through aggregations, creates entities and adds
     * fields to them. When entity has all fields from aggregations it adds to
     * entities list. If {@link #groupBy} has aggregation
     * {@link UriResourceKind}.count then property with {@link #countAlias} name
     * will be added to entity with doc count from response aggregations.
     * 
     * @param aggregations
     *            response aggregations
     * @param parent
     *            parent entity
     * @param entityType
     *            entity type
     * @return list of entities
     */
    protected List<Entity> getAggregatedEntities(Map<String, Aggregation> aggregations,
            Entity parent, ElasticEdmEntityType entityType) {
        List<Entity> entities = new ArrayList<>();
        for (Entry<String, Aggregation> entry : aggregations.entrySet()) {
            for (Bucket bucket : ((Terms) entry.getValue()).getBuckets()) {
                Entity child = new Entity();
                if (parent != null) {
                    child.getProperties().addAll(parent.getProperties());
                }
                addProperty(child, entityType.findPropertyByEField(entry.getKey()).getName(),
                        bucket.getKey(), entityType);
                Map<String, Aggregation> subAggregations = bucket.getAggregations().asMap();
                if (subAggregations.isEmpty()) {
                    if (countAlias != null) {
                        child.addProperty(getCountProperty(bucket.getDocCount()));
                    }
                    entities.add(child);
                } else {
                    entities.addAll(getAggregatedEntities(subAggregations, child, entityType));
                }
            }
        }
        return entities;
    }

    /**
     * Get's fields from {@link #groupBy} for aggregation query.
     * 
     * @param entityType
     *            entity type
     * @return list of fields
     */
    private List<String> getFields(ElasticEdmEntityType entityType) {
        List<String> groupByFields = new ArrayList<>();
        for (GroupByItem item : groupBy.getGroupByItems()) {
            for (UriResource resource : item.getPath()) {
                groupByFields.add(
                        entityType.getEProperties().get(resource.getSegmentValue()).getEField());
            }
        }
        for (Aggregate aggregate : getAggregations(groupBy.getApplyOption())) {
            handleAggregation(aggregate);
        }
        return groupByFields;
    }

    private List<Aggregate> getAggregations(ApplyOption applyOption) {
        List<Aggregate> aggregations = new ArrayList<>();
        if (applyOption != null) {
            for (ApplyItem item : applyOption.getApplyItems()) {
                if (item.getKind() == ApplyItem.Kind.AGGREGATE) {
                    aggregations.add((Aggregate) item);
                }
            }
        }
        return aggregations;
    }

    private void handleAggregation(Aggregate aggregate) {
        for (AggregateExpression expression : aggregate.getExpressions()) {
            for (UriResource resource : expression.getPath()) {
                if (resource.getKind() == UriResourceKind.count) {
                    countAlias = expression.getAlias();
                }
            }
        }
    }

    /**
     * Creates {@link Property} with {@link #countAlias} name.
     * 
     * @param value
     *            count value
     * @return property
     */
    private Property getCountProperty(long value) {
        return new Property(null, countAlias, ValueType.PRIMITIVE, value);
    }

}
