package com.hevelian.olastic.core.common;

/**
 * Custom implementation of nested mapping strategy when types have different
 * properties names for same nested object name.
 * 
 * @author rdidyk
 */
public class NestedPerTypeMappingStrategy implements NestedMappingStrategy {

    private static final String DEFAULT_SEPARATOR = "";

    private String separator;

    /**
     * Default constructor without separator.
     */
    public NestedPerTypeMappingStrategy() {
        this(DEFAULT_SEPARATOR);
    }

    /**
     * Constructor to provide custom separator.
     * 
     * @param separator
     *            separator between type and field
     */
    public NestedPerTypeMappingStrategy(String separator) {
        this.separator = separator;
    }

    @Override
    public String getComplexTypeName(String type, String field) {
        return type + separator + field;
    }

    public String getSeparator() {
        return separator;
    }

}
