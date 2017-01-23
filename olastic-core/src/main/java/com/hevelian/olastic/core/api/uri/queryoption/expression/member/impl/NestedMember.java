package com.hevelian.olastic.core.api.uri.queryoption.expression.member.impl;

import org.apache.lucene.search.join.ScoreMode;
import org.apache.olingo.server.api.ODataApplicationException;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;

import lombok.AllArgsConstructor;

/**
 * Wraps the data for nested query building.
 *
 * @author Taras Kohut
 */
@AllArgsConstructor
public class NestedMember extends BaseMember {

	private String nestedType;
	private QueryBuilder query;

	@Override
	public ExpressionResult any() throws ODataApplicationException {
		return buildNestedQuery();
	}

	private ExpressionResult buildNestedQuery() {
		return new ExpressionResult(QueryBuilders.nestedQuery(nestedType, query, ScoreMode.None));
	}

}
