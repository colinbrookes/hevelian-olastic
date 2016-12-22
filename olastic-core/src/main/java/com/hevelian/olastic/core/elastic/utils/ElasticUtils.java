package com.hevelian.olastic.core.elastic.utils;

import com.hevelian.olastic.core.api.uri.queryoption.expression.member.impl.TypedExpressionMember;
import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind;
import org.apache.olingo.commons.api.edm.FullQualifiedName;

import com.hevelian.olastic.core.edm.ElasticEdmEntityType;
import com.hevelian.olastic.core.edm.ElasticEdmProperty;
import com.hevelian.olastic.core.elastic.ElasticConstants;
import org.apache.olingo.commons.core.edm.primitivetype.EdmString;

/**
 * Elasticsearch utils.
 */
public final class ElasticUtils {

    private ElasticUtils() {
    }

    /**
     * Returns keyword field name if needed. Keyword field is non analyzed field.
     * 
     * @param property
     *            primitive expression property
     * @return property's keyword field name
     */
    public static String addKeywordIfNeeded(TypedExpressionMember property) {
        String field = property.getField();
        if (property.getEdmType() instanceof EdmString) {
            field =  addKeyword(field);
        }
        return field;
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
                ? addKeyword(fieldName) : fieldName;
    }
    /**
     * Returns keyword field name. Keyword field is non analyzed field.
     *
     * @param fieldName
     *            name of the field
     * @return property's keyword field name
     */
    public static String addKeyword (String fieldName) {
        return fieldName + ElasticConstants.SUFFIX_DELIMITER + ElasticConstants.KEYWORD_SUFFIX;
    }

}
