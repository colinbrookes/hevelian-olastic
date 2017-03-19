package com.hevelian.olastic.core.elastic.parsers;

import com.hevelian.olastic.core.edm.ElasticEdmEntityType;
import com.hevelian.olastic.core.edm.PropertyCreator;
import org.apache.olingo.commons.api.data.Property;

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
}
