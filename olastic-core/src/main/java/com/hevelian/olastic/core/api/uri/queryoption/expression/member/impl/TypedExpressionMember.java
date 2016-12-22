package com.hevelian.olastic.core.api.uri.queryoption.expression.member.impl;

import com.hevelian.olastic.core.api.uri.queryoption.expression.member.ExpressionMember;
import org.apache.olingo.commons.api.edm.EdmType;

/**
 * Represents expression member with type.
 * @author Taras Kohut
 */
public abstract class TypedExpressionMember extends ExpressionMember {
    private EdmType edmType;
    private String field;

    public TypedExpressionMember(String field, EdmType edmType) {
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
