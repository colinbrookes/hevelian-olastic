package com.hevelian.olastic.core.api.uri.queryoption.expression.member.impl;

import static org.elasticsearch.index.query.QueryBuilders.nestedQuery;

import org.apache.lucene.search.join.ScoreMode;
import org.apache.olingo.server.api.ODataApplicationException;
import org.elasticsearch.index.query.QueryBuilder;

import lombok.AllArgsConstructor;

/**
 * Wraps the data for nested query building.
 *
 * @author Taras Kohut
 */
@AllArgsConstructor
public class NestedMember extends BaseMember {

    private final String nestedPath;
    private final QueryBuilder query;

    @Override
    public ExpressionResult any() throws ODataApplicationException {
        return new ExpressionResult(nestedQuery(nestedPath, query, ScoreMode.None));
    }

}
