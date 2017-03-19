package com.hevelian.olastic.core.elastic.parsers;

import org.apache.olingo.commons.api.data.AbstractEntityCollection;
import org.apache.olingo.commons.api.data.Entity;
import org.apache.olingo.commons.api.data.EntityCollection;
import org.apache.olingo.commons.api.data.Property;
import org.apache.olingo.commons.api.data.ValueType;
import org.apache.olingo.commons.api.edm.EdmEntityType;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.metrics.NumericMetricsAggregation.SingleValue;

import com.hevelian.olastic.core.edm.ElasticEdmEntitySet;
import com.hevelian.olastic.core.edm.ElasticEdmEntityType;
import com.hevelian.olastic.core.processors.data.InstanceData;

/**
 * Class to parse metrics aggregations from Elasticsearch response.
 * 
 * @author rdidyk
 */
public class MetricsAggregationsParser
        extends SingleResponseParser<EdmEntityType, AbstractEntityCollection> {

    private String countAlias;

    /**
     * Constructor.
     * 
     * @param countAlias
     *            name of count alias, or null if no count option applied
     */
    public MetricsAggregationsParser(String countAlias) {
        this.countAlias = countAlias;
    }

    @Override
    public InstanceData<EdmEntityType, AbstractEntityCollection> parse(SearchResponse response,
            ElasticEdmEntitySet entitySet) {
        ElasticEdmEntityType entityType = entitySet.getEntityType();
        Entity entity = new Entity();
        Aggregations aggs = response.getAggregations();
        if (aggs != null) {
            aggs.asList().stream().filter(SingleValue.class::isInstance)
                    .map(SingleValue.class::cast)
                    .map(aggr -> createProperty(aggr.getName(), aggr.value(), entityType))
                    .forEach(entity::addProperty);
        }
        addCountIfNeeded(entity, response.getHits().getTotalHits());
        EntityCollection entities = new EntityCollection();
        entities.getEntities().add(entity);
        return new InstanceData<>(entityType, entities);
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
    protected void addCountIfNeeded(Entity entity, long count) {
        if (countAlias != null) {
            entity.addProperty(new Property(null, countAlias, ValueType.PRIMITIVE, count));
        }
    }

}
