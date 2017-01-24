package com.hevelian.olastic.core.elastic.requests;

import org.elasticsearch.action.search.SearchResponse;

import com.hevelian.olastic.core.edm.ElasticEdmEntitySet;
import com.hevelian.olastic.core.elastic.ESClient;
import com.hevelian.olastic.core.elastic.queries.AggregateQuery;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.experimental.FieldDefaults;

/**
 * Aggregate request with aggregate query and count alias.
 * 
 * @author rdidyk
 */
@Getter
@FieldDefaults(makeFinal = true, level = AccessLevel.PRIVATE)
public class AggregateRequest extends BaseRequest {

	String countAlias;

	/**
	 * Constructor to initialize query and entity.
	 * 
	 * @param query
	 *            aggregate query
	 * @param entitySet
	 *            the edm entity set
	 * @param countAlias
	 *            name of count alias, or null if no count option applied
	 */
	public AggregateRequest(AggregateQuery query, ElasticEdmEntitySet entitySet, String countAlias) {
		super(query, entitySet);
		this.countAlias = countAlias;
	}

	@Override
	public SearchResponse execute() {
		return ESClient.getInstance().executeRequest(getQuery());
	}

	@Override
	public AggregateQuery getQuery() {
		return (AggregateQuery) super.getQuery();
	}

}
