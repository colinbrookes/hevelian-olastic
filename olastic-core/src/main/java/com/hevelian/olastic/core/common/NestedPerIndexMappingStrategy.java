package com.hevelian.olastic.core.common;

/**
 * Default implementation of nested mapping strategy when types have same
 * properties names for nested object.
 * 
 * @author rdidyk
 */
public class NestedPerIndexMappingStrategy implements NestedMappingStrategy {

    @Override
    public String getComplexTypeName(String type, String field) {
        return field;
    }

}
