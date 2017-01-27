package com.hevelian.olastic.core.elastic.parsers;

import java.util.Iterator;
import java.util.Locale;
import java.util.Map;

import org.apache.http.HttpStatus;
import org.apache.olingo.commons.api.data.Entity;
import org.apache.olingo.commons.api.data.Property;
import org.apache.olingo.commons.api.edm.EdmEntityType;
import org.apache.olingo.server.api.ODataApplicationException;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.SearchHit;

import com.hevelian.olastic.core.edm.ElasticEdmEntitySet;
import com.hevelian.olastic.core.edm.ElasticEdmEntityType;
import com.hevelian.olastic.core.edm.ElasticEdmProperty;
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
            ElasticEdmEntitySet entitySet) throws ODataApplicationException {
        ElasticEdmEntityType entityType = entitySet.getEntityType();
        Iterator<SearchHit> hits = response.getHits().iterator();
        if (hits.hasNext()) {
            Entity entity = new Entity();
            SearchHit firstHit = hits.next();
            Object idSource = firstHit.getSource().get(ElasticConstants.ID_FIELD_NAME);
            if (idSource == null) {
                entity.setId(ProcessorUtils.createId(entityType.getName(), firstHit.getId()));
                Property idProperty = createProperty(ElasticConstants.ID_FIELD_NAME,
                        firstHit.getId(), entityType);
                entity.addProperty(idProperty);
            } else {
                entity.setId(ProcessorUtils.createId(entityType.getName(), idSource));
            }

            for (Map.Entry<String, Object> entry : firstHit.getSource().entrySet()) {
                ElasticEdmProperty edmProperty = entityType.findPropertyByEField(entry.getKey());
                entity.addProperty(
                        createProperty(edmProperty.getName(), entry.getValue(), entityType));
            }
            return new InstanceData<>(entityType, entity);
        } else {
            throw new ODataApplicationException("No data found", HttpStatus.SC_NOT_FOUND,
                    Locale.ROOT);
        }
    }

}