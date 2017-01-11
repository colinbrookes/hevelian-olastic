package com.hevelian.olastic.core.elastic.parsers;

import java.util.Map;

import org.apache.olingo.commons.api.data.AbstractEntityCollection;
import org.apache.olingo.commons.api.data.Entity;
import org.apache.olingo.commons.api.data.EntityCollection;
import org.apache.olingo.commons.api.data.Property;
import org.apache.olingo.commons.api.edm.EdmEntityType;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.SearchHit;

import com.hevelian.olastic.core.edm.ElasticEdmEntitySet;
import com.hevelian.olastic.core.edm.ElasticEdmEntityType;
import com.hevelian.olastic.core.elastic.ElasticConstants;
import com.hevelian.olastic.core.processors.data.InstanceData;
import com.hevelian.olastic.core.utils.ProcessorUtils;

/**
 * Parser class for collection of entities.
 * 
 * @author rdidyk
 */
public class EntityCollectionParser
        extends AbstractParser<EdmEntityType, AbstractEntityCollection> {

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
            Entity entity = new Entity();
            entity.setId(ProcessorUtils.createId(entityType.getName(), hit.getId()));
            Property idProperty = createProperty(ElasticConstants.ID_FIELD_NAME, hit.getId(),
                    entityType);
            entity.addProperty(idProperty);

            for (Map.Entry<String, Object> entry : hit.getSource().entrySet()) {
                Property property = createProperty(
                        entityType.findPropertyByEField(entry.getKey()).getName(), entry.getValue(),
                        entityType);
                entity.addProperty(property);
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