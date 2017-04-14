package com.hevelian.olastic.core.elastic.utils;

import com.hevelian.olastic.core.api.edm.annotations.AnnotationProvider;
import com.hevelian.olastic.core.elastic.ElasticConstants;
import org.apache.olingo.commons.api.edm.EdmAnnotation;

import java.util.List;
import java.util.Optional;

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
     * @param annotations
     *            field edm type
     * @return property's keyword field name
     */
    public static String addKeywordIfNeeded(String name, List<EdmAnnotation> annotations) {
        boolean isAnalyzed = false;
        Optional<EdmAnnotation> analyzedAnnotation = annotations.stream()
                .filter(annotation -> annotation.getTerm().getName().equals(AnnotationProvider.ANALYZED_TERM_NAME))
                .findFirst();
        if (analyzedAnnotation.isPresent()) {
            isAnalyzed = (Boolean)(analyzedAnnotation.get().getExpression().asConstant().asPrimitive());
        }
        return isAnalyzed ? addKeyword(name) : name;
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
