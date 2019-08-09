package com.hevelian.olastic.core.api.uri.queryoption.expression.member.impl;

import static org.elasticsearch.join.query.JoinQueryBuilders.hasChildQuery;

import org.apache.lucene.search.join.ScoreMode;
import org.apache.olingo.server.api.ODataApplicationException;
import org.elasticsearch.index.query.QueryBuilder;

import lombok.AllArgsConstructor;

/**
 * Wraps the data for child query building.
 *
 * @author Taras Kohut
 */
@AllArgsConstructor
public class ChildMember extends BaseMember {

    private final String childType;
    private final QueryBuilder query;

    @Override
    public ExpressionResult any() throws ODataApplicationException {
        return new ExpressionResult(hasChildQuery(childType, query, ScoreMode.None));
    }

}
