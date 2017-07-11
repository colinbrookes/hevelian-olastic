package com.hevelian.olastic.core.api.uri.queryoption.expression.member.impl;

import java.util.List;

import org.apache.olingo.server.api.ODataApplicationException;
import org.elasticsearch.index.query.QueryBuilder;

/**
 * Wraps the data for parent nested query building.
 *
 * @author Ruslan Didyk
 */
public class ParentNestedMember extends ParentMember {

    private QueryBuilder query;

    public ParentNestedMember(List<String> parentTypes, QueryBuilder query) {
        super(parentTypes);
        this.query = query;
    }

    @Override
    public ExpressionResult any() throws ODataApplicationException {
        return buildParentQuery(query);
    }

}
