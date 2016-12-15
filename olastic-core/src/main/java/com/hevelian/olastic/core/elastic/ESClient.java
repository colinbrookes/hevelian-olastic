package com.hevelian.olastic.core.elastic;

import java.util.Collection;
import java.util.List;

import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;

import com.hevelian.olastic.core.elastic.pagination.Pagination;
import com.hevelian.olastic.core.elastic.pagination.Sort;

/**
 * Retrieves the data from Elasticsearch.
 */
public class ESClient {
    /**
     * Execute query request with filter, pagination and included fields to
     * search.
     * 
     * @param index
     *            ES index
     * @param type
     *            ES type
     * @param client
     *            ES raw client
     * @param query
     *            ES raw search query
     * @param pagination
     *            pagination object
     * @param fields
     *            fields to return
     * @return ES search response
     */
    public static SearchResponse executeRequest(String index, String type, Client client,
            QueryBuilder query, Pagination pagination, Collection<String> fields) {
        SearchRequestBuilder request = client.prepareSearch(index).setTypes(type);
        List<Sort> orderBy = pagination.getOrderBy();
        for (Sort sort : orderBy) {
            FieldSortBuilder sortQuery = SortBuilders.fieldSort(sort.getProperty())
                    .order(SortOrder.valueOf(sort.getDirection().toString()));
            request.addSort(sortQuery);
        }
        request.setQuery(query);
        request.setSize(pagination.getTop()).setFrom(pagination.getSkip());
        if (fields != null && !fields.isEmpty()) {
            request.setFetchSource(fields.toArray(new String[fields.size()]), null);
        }
        return request.execute().actionGet();
    }

    /**
     * Execute query request with filter and aggregation.
     * 
     * @param index
     *            ES index
     * @param type
     *            ES type
     * @param client
     *            ES raw client
     * @param query
     *            ES raw search query
     * @param aggs
     *            aggregation query
     * @return ES search response
     */
    public static SearchResponse executeRequest(String index, String type, Client client,
            QueryBuilder query, List<AggregationBuilder> aggs) {
        SearchRequestBuilder requestBuilder = client.prepareSearch(index).setTypes(type)
                .setQuery(query);
        for (AggregationBuilder agg : aggs) {
            requestBuilder.addAggregation(agg);
        }
        return requestBuilder.setSize(0).execute().actionGet();
    }
}
