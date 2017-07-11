package com.hevelian.olastic.core.api.uri.queryoption.expression.member.impl;

import static org.elasticsearch.index.query.QueryBuilders.hasParentQuery;

import java.util.List;
import java.util.ListIterator;

import org.elasticsearch.index.query.QueryBuilder;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.experimental.FieldDefaults;

/**
 * Wraps the data for parent query building.
 *
 * @author Ruslan Didyk
 */
@AllArgsConstructor
@FieldDefaults(makeFinal = true, level = AccessLevel.PRIVATE)
public abstract class ParentMember extends BaseMember {

    List<String> parentTypes;

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
