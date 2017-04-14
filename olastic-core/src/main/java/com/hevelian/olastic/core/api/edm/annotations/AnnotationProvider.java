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
 * @author Taras Kohut
 */
public class AnnotationProvider {

    public final static String ANALYZED_TERM_NAME = "Analyzed";

    private HashMap <String, TermAnnotation> annotations = new HashMap<>();

    private class TermAnnotation {
        private CsdlAnnotation annotation;
        private CsdlTerm term;

        TermAnnotation(CsdlTerm term, CsdlAnnotation annotation) {
            this.term = term;
            this.annotation = annotation;
        }
    }

    //declaring terms and annotations
    private CsdlAnnotation analyzedAnnotation = new CsdlAnnotation()
            .setTerm("OData.Analyzed")
            .setExpression(
                    new CsdlConstantExpression(CsdlConstantExpression
                            .ConstantExpressionType.Bool, "true"));

    private CsdlTerm analyzedTerm = new CsdlTerm().setAppliesTo(Arrays.asList("Property"))
            .setName(ANALYZED_TERM_NAME)
            .setType(EdmPrimitiveTypeKind.String.getFullQualifiedName().getFullQualifiedNameAsString());

    public AnnotationProvider() {
        annotations.put(ANALYZED_TERM_NAME, new TermAnnotation(analyzedTerm, analyzedAnnotation));
    }

    public CsdlAnnotation getAnnotation(String termName) {
        TermAnnotation temAnnotation = annotations.get(termName);
        if (temAnnotation != null) {
            return annotations.get(termName).annotation;
        } else {
            return null;
        }
    }

    public CsdlTerm getTerm(String termName) {
        TermAnnotation temAnnotation = annotations.get(termName);
        if (temAnnotation != null) {
            return annotations.get(termName).term;
        } else {
            return null;
        }
    }

    public List<CsdlTerm> getTerms() {
        return annotations.entrySet().stream().map(entry -> entry.getValue().term).collect(Collectors.toList());
    }

}
