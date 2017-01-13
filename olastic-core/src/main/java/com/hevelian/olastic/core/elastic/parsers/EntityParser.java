package com.hevelian.olastic.core.elastic.parsers;

import java.util.Map;

import org.apache.olingo.commons.api.data.Entity;
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
 * Parser class for single entity.
 * 
 * @author rdidyk
 */
public class EntityParser extends AbstractParser<EdmEntityType, Entity> {

    @Override
    public InstanceData<EdmEntityType, Entity> parse(SearchResponse response,
            ElasticEdmEntitySet entitySet) {
        ElasticEdmEntityType entityType = entitySet.getEntityType();
        SearchHit firstHit = response.getHits().getAt(0);
        Entity entity = new Entity();
        entity.setId(ProcessorUtils.createId(entityType.getName(), firstHit.getId()));
        Property idProperty = createProperty(ElasticConstants.ID_FIELD_NAME, firstHit.getId(),
                entityType);
        entity.addProperty(idProperty);

        for (Map.Entry<String, Object> entry : firstHit.getSource().entrySet()) {
            Property property = createProperty(
                    entityType.findPropertyByEField(entry.getKey()).getName(), entry.getValue(),
                    entityType);
            entity.addProperty(property);
        }
        return new InstanceData<>(entityType, entity);
    }

}