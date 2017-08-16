package com.hevelian.olastic.core.elastic.parsers;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;

import org.apache.olingo.commons.api.data.AbstractEntityCollection;
import org.apache.olingo.commons.api.data.Entity;
import org.apache.olingo.commons.api.data.EntityCollection;
import org.apache.olingo.commons.api.data.Property;
import org.apache.olingo.commons.api.data.ValueType;
import org.apache.olingo.commons.api.edm.EdmEntityType;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms.Bucket;
import org.elasticsearch.search.aggregations.metrics.NumericMetricsAggregation.SingleValue;

import com.hevelian.olastic.core.edm.ElasticEdmEntitySet;
import com.hevelian.olastic.core.edm.ElasticEdmEntityType;
import com.hevelian.olastic.core.elastic.pagination.Pagination;
import com.hevelian.olastic.core.processors.data.InstanceData;

/**
 * Class to parse buckets aggregations from Elasticsearch response.
 * 
 * @author rdidyk
 */
public class BucketsAggregationsParser
        extends SingleResponseParser<EdmEntityType, AbstractEntityCollection> {

    private String countAlias;
    private Pagination pagination;

    /**
     * Constructor.
     * 
     * @param pagination
     *            pagination information
     * @param countAlias
     *            name of count alias, or null if no count option applied
     */
    public BucketsAggregationsParser(Pagination pagination, String countAlias) {
        this.pagination = pagination;
        this.countAlias = countAlias;
    }

    @Override
    public InstanceData<EdmEntityType, AbstractEntityCollection> parse(SearchResponse response,
            ElasticEdmEntitySet entitySet) {
        ElasticEdmEntityType entityType = entitySet.getEntityType();
        EntityCollection entities = new EntityCollection();
        List<Entity> entityList = getAggregatedEntities(response.getAggregations().asMap(), null,
                entityType);
        entities.getEntities().addAll(subList(entityList));
        return new InstanceData<>(entityType, entities);
    }

    /**
     * Method recursively goes through aggregations, creates entities and adds
     * fields to them. When entity has all fields from aggregations it adds to
     * entities list. If groupBy has aggregation $count then property with
     * {@link #countAlias} name will be added to entity with doc count from
     * response aggregations.
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
                Property property = createProperty(entry.getKey(), bucket.getKey(), entityType);
                entity.addProperty(property);
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

    /**
     * Collects terms.
     * 
     * @param aggregations
     *            aggregations map
     * @return collected terms
     */
    private static Map<String, Terms> collectTerms(Map<String, Aggregation> aggregations) {
        return aggregations.entrySet().stream().filter(e -> e.getValue() instanceof Terms)
                .collect(Collectors.toMap(e -> e.getKey(), e -> (Terms) e.getValue()));
    }

    /**
     * Adds aggregation and count if needed.
     * 
     * @param aggregations
     *            metrics aggregations
     * @param count
     *            count value
     * @param entity
     *            entity to which add
     * @param entityType
     *            the edm entity type
     */
    private void addAggsAndCountIfNeeded(Map<String, Aggregation> aggregations, long count,
            Entity entity, ElasticEdmEntityType entityType) {
        aggregations.entrySet().stream().filter(e -> e.getValue() instanceof SingleValue)
                .map(agg -> createProperty(agg.getKey(), ((SingleValue) agg.getValue()).value(),
                        entityType))
                .forEach(entity::addProperty);
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
     * This is custom pagination, because Elasticsearch doesn't support this.
     * Method to get sublist from parent list with top and skip parameter. We
     * need to check whether top and skip are in parent list size or not, and
     * then calculate from and to indexes.
     * 
     * @param entities
     *            entities to paginate
     * @return new sublist
     */
    private List<Entity> subList(List<Entity> entities) {
        if (entities.isEmpty()) {
            return entities;
        }
        int top = pagination.getTop();
        int skip = pagination.getSkip();
        int fromIndex;
        int toIndex;
        int max = skip + top;
        int actualSize = entities.size();
        if (max <= actualSize) {
            fromIndex = skip;
            toIndex = top + skip;
        } else {
            fromIndex = skip > actualSize ? actualSize : skip;
            toIndex = actualSize;
        }
        return entities.subList(fromIndex, toIndex);
    }
}
