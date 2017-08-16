package com.hevelian.olastic.core.elastic.parsers;

import com.hevelian.olastic.core.edm.ElasticEdmEntityType;
import com.hevelian.olastic.core.edm.PropertyCreator;
import org.apache.olingo.commons.api.data.Property;

/**
 * Abstract parser with common behavior for all multi response parsers.
 *
 * @param <T>
 *            instance data type class
 * @param <V>
 *            instance data value class
 * @author Taras Kohut
 */
public abstract class MultiResponseParser<T, V> implements ESMultiResponseParser<T, V> {
    private PropertyCreator propertyCreator;

    /**
     * Default constructor to initialize property creator.
     */
    public MultiResponseParser() {
        propertyCreator = new PropertyCreator();
    }

    /**
     * Creates a property.
     * 
     * @param name
     *            property name
     * @param value
     *            property value
     * @param entityType
     *            property entity type
     * @return property instance
     */
    protected Property createProperty(String name, Object value, ElasticEdmEntityType entityType) {
        return propertyCreator.createProperty(name, value, entityType);
    }
}
