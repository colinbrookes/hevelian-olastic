package com.hevelian.olastic.core.utils;

import com.hevelian.olastic.core.elastic.ElasticConstants;

/**
 * Elasticsearch utils.
 */
public class ElasticUtils {
    /**
     *  Returns keyword field name. Keyword field is non analyzed field.
     * @param propertyName name of the property (field)
     * @return property's keyword field name
     */
    public static String getKeywordField(String propertyName) {
        return propertyName + ElasticConstants.SUFFIX_DELIMITER + ElasticConstants.KEYWORD_SUFFIX;
    }
}
