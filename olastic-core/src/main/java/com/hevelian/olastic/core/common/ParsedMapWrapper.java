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
    public final Map<String, Object> map;

    public ParsedMapWrapper(Map<String, Object> map) {
        this.map = map;
    }

    @SuppressWarnings("unchecked")
    public ParsedMapWrapper mapValue(String mapProperty) {
        return new ParsedMapWrapper((Map<String, Object>) value(mapProperty));
    }

    // TODO more getter methods for primitive types
    // TODO getter for value to be casted to custom class

    public String stringValue(String mapProperty) {
        return (String) value(mapProperty);
    }

    public Object value(String mapProperty) {
        return map.get(mapProperty);
    }
}
