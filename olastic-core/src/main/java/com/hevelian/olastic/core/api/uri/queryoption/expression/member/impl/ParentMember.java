package com.hevelian.olastic.core.api.uri.queryoption.expression.member.impl;

import static org.elasticsearch.join.query.JoinQueryBuilders.hasParentQuery;

import java.util.List;
import java.util.ListIterator;

import org.elasticsearch.index.query.QueryBuilder;

import lombok.AllArgsConstructor;

/**
 * Wraps the data for parent query building.
 *
 * @author Ruslan Didyk
 */
@AllArgsConstructor
public abstract class ParentMember extends BaseMember {

    private final List<String> parentTypes;

    /**
     * Builds parent ES query using provided query.
     * 
     * @param query
     *            query
     * @return parent query
     */
    public ExpressionResult buildParentQuery(QueryBuilder query) {
        ListIterator<String> iterator = parentTypes.listIterator(parentTypes.size());
        QueryBuilder resultQuery = query;
        while (iterator.hasPrevious()) {
            resultQuery = hasParentQuery(iterator.previous(), resultQuery, false);
        }
        return new ExpressionResult(resultQuery);
    }
}
