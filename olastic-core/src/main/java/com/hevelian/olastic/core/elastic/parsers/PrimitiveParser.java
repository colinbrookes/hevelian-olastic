package com.hevelian.olastic.core.elastic.parsers;

import java.util.Map;
import java.util.Map.Entry;

import org.apache.olingo.commons.api.data.Property;
import org.apache.olingo.commons.api.edm.EdmPrimitiveType;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.SearchHit;

import com.hevelian.olastic.core.edm.ElasticEdmEntitySet;
import com.hevelian.olastic.core.edm.ElasticEdmEntityType;
import com.hevelian.olastic.core.edm.ElasticEdmProperty;
import com.hevelian.olastic.core.elastic.ElasticConstants;
import com.hevelian.olastic.core.processors.data.InstanceData;

/**
 * Parser class for single primitive property value.
 * 
 * @author rdidyk
 */
public class PrimitiveParser extends AbstractParser<EdmPrimitiveType, Property> {

    @Override
    public InstanceData<EdmPrimitiveType, Property> parse(SearchResponse response,
            ElasticEdmEntitySet entitySet) {
        ElasticEdmEntityType entityType = entitySet.getEntityType();
        SearchHit firstHit = response.getHits().getAt(0);

        ElasticEdmProperty edmProperty = null;
        Property property = null;

        Map<String, Object> source = firstHit.getSource();
        if (source.isEmpty()) {
            edmProperty = entityType.findPropertyByEField(ElasticConstants.ID_FIELD_NAME);
            property = createProperty(edmProperty.getName(), firstHit.getId(), entityType);
        } else {
            Entry<String, Object> entry = source.entrySet().iterator().next();
            edmProperty = entityType.findPropertyByEField(entry.getKey());
            property = createProperty(edmProperty.getName(), entry.getValue(), entityType);
        }
        return new InstanceData<>((EdmPrimitiveType) edmProperty.getType(), property);
    }

}
