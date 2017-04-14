package com.hevelian.olastic.core.api.uri.queryoption.expression.member.impl;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.experimental.FieldDefaults;
import org.apache.olingo.commons.api.edm.EdmAnnotation;

import java.util.List;

/**
 * Represents expression member with type.
 *
 * @author Taras Kohut
 */
@AllArgsConstructor
@FieldDefaults(makeFinal = true, level = AccessLevel.PRIVATE)
@Getter
public abstract class AnnotatedMember extends BaseMember {

    String field;
    List<EdmAnnotation> annotations;

}
