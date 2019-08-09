package com.hevelian.olastic.core.elastic.parsers;

import com.hevelian.olastic.core.edm.ElasticEdmEntitySet;
import com.hevelian.olastic.core.edm.ElasticEdmEntityType;
import com.hevelian.olastic.core.edm.ElasticEdmProperty;
import com.hevelian.olastic.core.elastic.ElasticConstants;
import com.hevelian.olastic.core.processors.data.InstanceData;
import com.hevelian.olastic.core.utils.ProcessorUtils;
import org.apache.olingo.commons.api.data.AbstractEntityCollection;
import org.apache.olingo.commons.api.data.Entity;
import org.apache.olingo.commons.api.data.EntityCollection;
import org.apache.olingo.commons.api.edm.EdmEntityType;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.SearchHit;

import java.util.Map;

/**
 * Parser class for collection of entities.
 * 
 * @author rdidyk
 */
public class EntityCollectionParser
        extends SingleResponseParser<EdmEntityType, AbstractEntityCollection> {

    private boolean count;

    /**
     * Constructor.
     * 
     * @param count
     *            count option value
     */
    public EntityCollectionParser(boolean count) {
        this.count = count;
    }

    @Override
    public InstanceData<EdmEntityType, AbstractEntityCollection> parse(SearchResponse response,
            ElasticEdmEntitySet entitySet) {
        ElasticEdmEntityType entityType = entitySet.getEntityType();
        EntityCollection entities = new EntityCollection();
        for (SearchHit hit : response.getHits()) {
            Map<String, Object> source = hit.getSourceAsMap();

            Entity entity = new Entity();
            entity.setId(ProcessorUtils.createId(entityType.getName(), hit.getId()));
            entity.addProperty(
                    createProperty(ElasticConstants.ID_FIELD_NAME, hit.getId(), entityType));

            for (Map.Entry<String, Object> entry : source.entrySet()) {
                ElasticEdmProperty edmProperty = entityType.findPropertyByEField(entry.getKey());
                entity.addProperty(
                        createProperty(edmProperty.getName(), entry.getValue(), entityType));
            }
            entities.getEntities().add(entity);
        }
        if (isCount()) {
            entities.setCount((int) response.getHits().getTotalHits());
        }
        return new InstanceData<>(entityType, entities);
    }

    public boolean isCount() {
        return count;
    }
}