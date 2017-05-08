package com.hevelian.olastic.core.elastic;

/**
 * Elasticsearch constants.
 * 
 * @author yuflyud
 * @author rdidyk
 * @author Taras Kohut
 */
public final class ElasticConstants {

    /** Field data type property. */
    public static final String FIELD_DATATYPE_PROPERTY = "type";
    /** Properties property name. */
    public static final String PROPERTIES_PROPERTY = "properties";
    /** Parent property name. */
    public static final String PARENT_PROPERTY = "_parent";
    /** ID field name. */
    public static final String ID_FIELD_NAME = "_id";
    /** Suffix for keyword (not-analyzed) field. */
    public static final String KEYWORD_SUFFIX = "keyword";
    /** Field suffix delimiter. */
    public static final String SUFFIX_DELIMITER = ".";
    public static final String NESTED_PATH_SEPARATOR = ".";
    public static final String WILDCARD_CHAR = "*";
    /**
     * The _all field is a special catch-all field which concatenates the values
     * of all of the other fields into one big string.
     */
    public static final String ALL_FIELD = "_all";

    private ElasticConstants() {
    }
}
