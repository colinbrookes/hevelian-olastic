package com.hevelian.olastic.core.elastic;

/**
 * Elasticsearch constants.
 * 
 * @author yuflyud
 */
public final class ElasticConstants {

    public static final String FIELD_DATATYPE_PROPERTY = "type";
    public static final String PROPERTIES_PROPERTY = "properties";
    public static final String PARENT_PROPERTY = "_parent";
    public static final String ID_FIELD_NAME = "_id";
    /** suffix for keyword (not-analyzed) field */
    public static final String KEYWORD_SUFFIX = "keyword";
    /** field suffix delimiter */
    public static final String SUFFIX_DELIMITER = ".";

    private ElasticConstants() {
    }
}
