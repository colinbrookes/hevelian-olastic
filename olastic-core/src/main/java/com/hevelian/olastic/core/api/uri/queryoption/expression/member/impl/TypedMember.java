package com.hevelian.olastic.core.api.uri.queryoption.expression.member.impl;

import org.apache.olingo.commons.api.edm.EdmType;

/**
 * Represents expression member with type.
 *
 * @author Taras Kohut
 */
public abstract class TypedMember extends BaseMember {
    private EdmType edmType;
    private String field;

    public TypedMember(String field, EdmType edmType) {
        this.field = field;
        this.edmType = edmType;
    }

    public String getField() {
        return field;
    }

    public EdmType getEdmType() {
        return edmType;
    }
}
