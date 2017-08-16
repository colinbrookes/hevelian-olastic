package com.hevelian.olastic.core.api.uri.queryoption.expression.member.impl;

import java.util.List;

import org.apache.olingo.server.api.ODataApplicationException;
import org.elasticsearch.index.query.QueryBuilder;

/**
 * Wraps the nested or child query info parent types query.
 *
 * @author Ruslan Didyk
 */
public class ParentWrapperMember extends ParentMember {

    private QueryBuilder query;

    /**
     * Initialize fields.
     * 
     * @param parentTypes
     *            list of parent type names
     * @param query
     *            inner query
     */
    public ParentWrapperMember(List<String> parentTypes, QueryBuilder query) {
        super(parentTypes);
        this.query = query;
    }

    @Override
    public ExpressionResult any() throws ODataApplicationException {
        return buildParentQuery(query);
    }

}
