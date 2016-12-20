package com.hevelian.olastic.core.elastic.utils;

import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind;
import org.apache.olingo.commons.api.edm.FullQualifiedName;

import com.hevelian.olastic.core.edm.ElasticEdmEntityType;
import com.hevelian.olastic.core.edm.ElasticEdmProperty;
import com.hevelian.olastic.core.elastic.ElasticConstants;

/**
 * Elasticsearch utils.
 */
public final class ElasticUtils {

    private ElasticUtils() {
    }

    /**
     * Returns keyword field name. Keyword field is non analyzed field.
     * 
     * @param propertyName
     *            name of the property (field)
     * @return property's keyword field name
     */
    public static String getKeywordField(String propertyName) {
        return propertyName + ElasticConstants.SUFFIX_DELIMITER + ElasticConstants.KEYWORD_SUFFIX;
    }

    /**
     * Returns field with '.keyword' suffix if it's type 'Edm.String', otherwise
     * field name.
     * 
     * @param fieldName
     *            field name
     * @param entityType
     *            field entity type
     * @return field name
     */
    public static String addKeywordIfNeeded(String fieldName, ElasticEdmEntityType entityType) {
        ElasticEdmProperty property = entityType.getEProperties().get(fieldName);
        FullQualifiedName typeFQN = property.getType().getFullQualifiedName();
        return typeFQN.equals(EdmPrimitiveTypeKind.String.getFullQualifiedName())
                ? getKeywordField(fieldName) : fieldName;
    }

}
