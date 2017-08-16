package com.hevelian.olastic.core.api.edm.annotations;

import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind;
import org.apache.olingo.commons.api.edm.provider.CsdlAnnotation;
import org.apache.olingo.commons.api.edm.provider.CsdlTerm;
import org.apache.olingo.commons.api.edm.provider.annotation.CsdlConstantExpression;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Contains annotations and their terms definitions.
 * 
 * @author Taras Kohut
 */
public class AnnotationProvider {
    /** Analyzed term name. */
    public static final String ANALYZED_TERM_NAME = "Analyzed";

    private HashMap<String, TermAnnotation> annotations = new HashMap<>();

    // declaring terms and annotations
    private CsdlAnnotation analyzedAnnotation = new CsdlAnnotation().setTerm("OData.Analyzed")
            .setExpression(new CsdlConstantExpression(
                    CsdlConstantExpression.ConstantExpressionType.Bool, "true"));

    private CsdlTerm analyzedTerm = new CsdlTerm().setAppliesTo(Arrays.asList("Property"))
            .setName(ANALYZED_TERM_NAME).setType(EdmPrimitiveTypeKind.String.getFullQualifiedName()
                    .getFullQualifiedNameAsString());

    /**
     * Default constructor. Sets default annotation.
     */
    public AnnotationProvider() {
        annotations.put(ANALYZED_TERM_NAME, new TermAnnotation(analyzedTerm, analyzedAnnotation));
    }

    /**
     * Gets annotation by term name.
     * 
     * @param termName
     *            term name
     * @return found annotation, otherwise null
     */
    public CsdlAnnotation getAnnotation(String termName) {
        TermAnnotation temAnnotation = annotations.get(termName);
        if (temAnnotation != null) {
            return annotations.get(termName).annotation;
        } else {
            return null;
        }
    }

    /**
     * Gets term by term name.
     * 
     * @param termName
     *            term name
     * @return found term, otherwise null
     */
    public CsdlTerm getTerm(String termName) {
        TermAnnotation temAnnotation = annotations.get(termName);
        if (temAnnotation != null) {
            return annotations.get(termName).term;
        } else {
            return null;
        }
    }

    public List<CsdlTerm> getTerms() {
        return annotations.entrySet().stream().map(entry -> entry.getValue().term)
                .collect(Collectors.toList());
    }

    /**
     * Class represents container for term and annotation.
     */
    private class TermAnnotation {
        private CsdlAnnotation annotation;
        private CsdlTerm term;

        TermAnnotation(CsdlTerm term, CsdlAnnotation annotation) {
            this.term = term;
            this.annotation = annotation;
        }
    }
}
