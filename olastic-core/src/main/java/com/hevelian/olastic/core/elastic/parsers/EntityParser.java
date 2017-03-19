package com.hevelian.olastic.core.elastic.parsers;

import com.hevelian.olastic.core.edm.ElasticEdmEntitySet;
import com.hevelian.olastic.core.edm.ElasticEdmEntityType;
import com.hevelian.olastic.core.edm.ElasticEdmProperty;
import com.hevelian.olastic.core.elastic.ElasticConstants;
import com.hevelian.olastic.core.processors.data.InstanceData;
import com.hevelian.olastic.core.utils.ProcessorUtils;
import org.apache.http.HttpStatus;
import org.apache.olingo.commons.api.data.Entity;
import org.apache.olingo.commons.api.edm.EdmEntityType;
import org.apache.olingo.server.api.ODataApplicationException;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.SearchHit;

import java.util.Iterator;
import java.util.Locale;
import java.util.Map;

/**
 * Parser class for single entity.
 * 
 * @author rdidyk
 */
public class EntityParser extends SingleResponseParser<EdmEntityType, Entity> {

    @Override
    public InstanceData<EdmEntityType, Entity> parse(SearchResponse response,
            ElasticEdmEntitySet entitySet) throws ODataApplicationException {
        ElasticEdmEntityType entityType = entitySet.getEntityType();
        Iterator<SearchHit> hits = response.getHits().iterator();
        if (hits.hasNext()) {
            SearchHit firstHit = hits.next();
            Map<String, Object> source = firstHit.getSource();

            Entity entity = new Entity();
            entity.setId(ProcessorUtils.createId(entityType.getName(), firstHit.getId()));
            entity.addProperty(
                    createProperty(ElasticConstants.ID_FIELD_NAME, firstHit.getId(), entityType));

            for (Map.Entry<String, Object> entry : source.entrySet()) {
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