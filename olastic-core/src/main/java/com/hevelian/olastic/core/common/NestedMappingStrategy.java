package com.hevelian.olastic.core.common;

/**
 * Interface to provide different type of mappings for nested types. I.e.: By
 * default types in Elasticsearch have nested object with same properties, but
 * also sometimes there are types which have same nested object name but
 * different properties names, to resolve this situation implement this
 * strategy.
 * 
 * @author rdidyk
 */
public interface NestedMappingStrategy {

    /**
     * Get's name for complex type of Elasticsearch nested object.
     * 
     * @param type
     *            type name
     * @param field
     *            nested field name
     * @return
     */
    String getComplexTypeName(String type, String field);

}
