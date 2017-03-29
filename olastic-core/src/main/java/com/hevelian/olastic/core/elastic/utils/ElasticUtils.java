package com.hevelian.olastic.core.elastic.utils;

import org.apache.olingo.commons.api.edm.EdmType;
import org.apache.olingo.commons.core.edm.primitivetype.EdmString;

import com.hevelian.olastic.core.elastic.ElasticConstants;

/**
 * Elasticsearch utils.
 */
public final class ElasticUtils {

    private ElasticUtils() {
    }

    /**
     * Returns keyword field name if needed. Keyword field is non analyzed
     * field.
     *
     * @param name
     *            field name
     * @param type
     *            field edm type
     * @return property's keyword field name
     */
    public static String addKeywordIfNeeded(String name, EdmType type) {
        return type instanceof EdmString ? addKeyword(name) : name;
    }

    /**
     * Returns keyword field name. Keyword field is non analyzed field.
     *
     * @param fieldName
     *            name of the field
     * @return property's keyword field name
     */
    public static String addKeyword(String fieldName) {
        return fieldName + ElasticConstants.SUFFIX_DELIMITER + ElasticConstants.KEYWORD_SUFFIX;
    }

}
