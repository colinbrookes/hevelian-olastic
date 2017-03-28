package com.hevelian.olastic.core.elastic.parsers;

import com.hevelian.olastic.core.edm.ElasticEdmEntitySet;
import com.hevelian.olastic.core.edm.ElasticEdmEntityType;
import com.hevelian.olastic.core.edm.PropertyCreator;
import com.hevelian.olastic.core.processors.data.InstanceData;
import org.apache.olingo.commons.api.data.Property;
import org.apache.olingo.commons.api.http.HttpStatusCode;
import org.apache.olingo.server.api.ODataApplicationException;
import org.elasticsearch.action.search.SearchResponse;

import java.util.List;
import java.util.Locale;

/**
 * Abstract parser with common behavior for all parsers.
 *
 * @param <T> instance data type class
 * @param <V> instance data value class
 * @author Taras Kohut
 */
public abstract class SingleResponseParser<T, V> implements ESResponseParser<T, V> {
    private PropertyCreator propertyCreator;

    public SingleResponseParser() {
        propertyCreator = new PropertyCreator();
    }

    /**
     * Creates a property.
     * @param name property name
     * @param value property value
     * @param entityType property entity type
     * @return property instance
     */
    protected Property createProperty(String name, Object value, ElasticEdmEntityType entityType) {
        return propertyCreator.createProperty(name, value, entityType);
    }

    public InstanceData<T, V> parse(SearchResponse response, List<ElasticEdmEntitySet> responseEntitySets, ElasticEdmEntitySet returnEntitySet)
            throws ODataApplicationException {
        return parse(response, returnEntitySet);
    }

    public InstanceData<T, V> parse(SearchResponse response, ElasticEdmEntitySet entitySet)
            throws ODataApplicationException {
        throw new ODataApplicationException("Not implemented", HttpStatusCode.NOT_IMPLEMENTED.getStatusCode(),
                Locale.ROOT);
    }
}
