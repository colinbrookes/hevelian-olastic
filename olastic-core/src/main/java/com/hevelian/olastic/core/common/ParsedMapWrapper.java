package com.hevelian.olastic.core.common;

import java.util.Map;

/**
 * Map wrapper for working with String/Object entries. Class provides methods
 * for casting values to the requested types. This class is especially useful
 * when working with responses from Elasticsearch, which are just maps, not the
 * objects.
 * 
 * @author yuflyud
 */
public class ParsedMapWrapper {
    // TODO: more getter methods for primitive types
    // TODO: getter for value to be casted to custom class
    private final Map<String, Object> map;

    /**
     * Initialize field.
     * 
     * @param map
     *            elasticsearch map response
     */
    public ParsedMapWrapper(Map<String, Object> map) {
        this.map = map;
    }

    /**
     * Gets inner map value wrapped into this class instance.
     * 
     * @param mapProperty
     *            map key
     * @return inner map wrapper instnace
     */
    @SuppressWarnings("unchecked")
    public ParsedMapWrapper mapValue(String mapProperty) {
        return new ParsedMapWrapper((Map<String, Object>) value(mapProperty));
    }

    /**
     * Gets String value from map.
     * 
     * @param mapProperty
     *            map key
     * @return value casted to String
     */
    public String stringValue(String mapProperty) {
        return (String) value(mapProperty);
    }

    /**
     * Gets original value from map.
     * 
     * @param mapProperty
     *            map key
     * @return original value
     */
    public Object value(String mapProperty) {
        return map.get(mapProperty);
    }

    public Map<String, Object> getMap() {
        return map;
    }
}
