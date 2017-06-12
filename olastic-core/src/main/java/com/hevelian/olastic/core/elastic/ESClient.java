package com.hevelian.olastic.core.elastic;

import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.olingo.commons.api.http.HttpStatusCode;
import org.apache.olingo.server.api.ODataApplicationException;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.search.MultiSearchRequestBuilder;
import org.elasticsearch.action.search.MultiSearchResponse;
import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.NoNodeAvailableException;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;

import com.hevelian.olastic.core.elastic.pagination.Pagination;
import com.hevelian.olastic.core.elastic.pagination.Sort;
import com.hevelian.olastic.core.elastic.queries.AggregateQuery;
import com.hevelian.olastic.core.elastic.queries.SearchQuery;
import com.hevelian.olastic.core.exceptions.SearchException;

import lombok.extern.log4j.Log4j2;

/**
 * Central point to retrieve the data from Elasticsearch.
 * 
 * @author rdidyk
 */
@Log4j2
public class ESClient {

    private static ESClient INSTANCE;

    private Client client;

    private ESClient(Client client) {
        this.client = client;
    }

    /**
     * Get's instance.
     * 
     * @return created instance or if it wasn't initialized illegal state
     *         exception will be thrown
     */
    public static ESClient getInstance() {
        if (INSTANCE == null) {
            throw new IllegalStateException("Elasticsearch Client is not initialized.");
        }
        return INSTANCE;
    }

    /**
     * Method that initializes current client. It initializes new instance with
     * Elasticsearch Client. This method can be called only once, in other case
     * the illegal state exception will be thrown.
     * 
     * @param client
     *            Elasticsearch client instance
     */
    public static void init(Client client) {
        if (INSTANCE == null) {
            synchronized (ESClient.class) {
                if (INSTANCE == null) {
                    INSTANCE = new ESClient(client);
                } else {
                    throw new IllegalStateException(
                            "Elasticsearch query executor client is already initialized.");
                }
            }
        }
    }

    /**
     * Execute aggregate query request.
     * 
     * @param query
     *            aggregate query
     * @return ES search response
     * @throws ODataApplicationException
     *             if any error appeared during executing request
     */
    public SearchResponse executeRequest(AggregateQuery query) throws ODataApplicationException {
        SearchRequestBuilder requestBuilder = client.prepareSearch(query.getIndex())
                .setTypes(query.getTypes()).setQuery(query.getQueryBuilder());
        query.getAggregations().forEach(requestBuilder::addAggregation);
        query.getPipelineAggregations().forEach(requestBuilder::addAggregation);
        requestBuilder.setSize(0);
        return executeRequest(requestBuilder);
    }

    /**
     * Execute query request with filter and aggregations.
     * 
     * @param queries
     *            list of queries to execute
     * @return ES search response
     * @throws ODataApplicationException
     *             if any error appeared during executing request
     */
    public MultiSearchResponse executeRequest(List<SearchQuery> queries)
            throws ODataApplicationException {
        MultiSearchRequestBuilder multiSearchRequestBuilder = client.prepareMultiSearch();
        for (SearchQuery query : queries) {
            Pagination pagination = query.getPagination();
            SearchRequestBuilder requestBuilder = client.prepareSearch(query.getIndex())
                    .setTypes(query.getTypes()).setQuery(query.getQueryBuilder());
            if (pagination != null) {
                List<Sort> orderBy = pagination.getOrderBy();
                for (Sort sort : orderBy) {
                    FieldSortBuilder sortQuery = SortBuilders.fieldSort(sort.getProperty())
                            .order(SortOrder.valueOf(sort.getDirection().toString()));
                    requestBuilder.addSort(sortQuery);
                }
                requestBuilder.setSize(pagination.getTop()).setFrom(pagination.getSkip());
            }
            Set<String> fields = query.getFields();
            if (fields != null && !fields.isEmpty()) {
                requestBuilder.setFetchSource(fields.toArray(new String[fields.size()]), null);
            }
            multiSearchRequestBuilder.add(requestBuilder);
        }
        return executeRequest(multiSearchRequestBuilder);
    }

    /**
     * Execute query request with filter and aggregations.
     * 
     * @param query
     *            search query
     * @return ES search response
     * @throws ODataApplicationException
     *             if any error appeared during executing request
     */
    public SearchResponse executeRequest(SearchQuery query) throws ODataApplicationException {
        Pagination pagination = query.getPagination();
        SearchRequestBuilder requestBuilder = client.prepareSearch(query.getIndex())
                .setTypes(query.getTypes()).setQuery(query.getQueryBuilder());
        if (pagination != null) {
            List<Sort> orderBy = pagination.getOrderBy();
            for (Sort sort : orderBy) {
                FieldSortBuilder sortQuery = SortBuilders.fieldSort(sort.getProperty())
                        .order(SortOrder.valueOf(sort.getDirection().toString()));
                requestBuilder.addSort(sortQuery);
            }
            requestBuilder.setSize(pagination.getTop()).setFrom(pagination.getSkip());
        }
        Set<String> fields = query.getFields();
        if (fields != null && !fields.isEmpty()) {
            requestBuilder.setFetchSource(fields.toArray(new String[fields.size()]), null);
        }
        return executeRequest(requestBuilder);
    }

    /**
     * Method has to be used to execute any request. It has logging logic.
     *
     * @param request
     *            request to execute
     * @return request response
     * @throws ODataApplicationException
     *             if any error appeared during executing request
     */
    protected SearchResponse executeRequest(SearchRequestBuilder request)
            throws ODataApplicationException {
        SearchResponse response = null;
        ElasticsearchException searchError = null;
        try {
            response = request.execute().actionGet();
        } catch (SearchPhaseExecutionException | NoNodeAvailableException exception) {
            searchError = exception;
            throw new SearchException(searchError.getDetailedMessage());
        } catch (IndexNotFoundException exception) {
            searchError = exception;
            throw new ODataApplicationException(
                    String.format("One or more indices %s not found.",
                            indicesToString(request.request().indices())),
                    HttpStatusCode.NOT_FOUND.getStatusCode(), Locale.ROOT, exception);
        } finally {
            log.debug(String.format("Executing query request:%n%s", request.request()));
            if (response != null) {
                log.debug(String.format("Query execution took: %s", response.getTook()));
            } else {
                log.error("Failed to execute query: ", searchError);
            }
        }
        return response;
    }

    /**
     * Method has to be used to execute any request. It has logging logic.
     *
     * @param request
     *            request to execute
     * @return request response
     * @throws ODataApplicationException
     *             if any error appeared during executing request
     */
    protected MultiSearchResponse executeRequest(MultiSearchRequestBuilder request)
            throws ODataApplicationException {
        MultiSearchResponse response = null;
        ElasticsearchException searchError = null;
        try {
            response = request.execute().actionGet();
        } catch (SearchPhaseExecutionException | NoNodeAvailableException exception) {
            searchError = exception;
            throw new SearchException(searchError.getDetailedMessage());
        } catch (IndexNotFoundException exception) {
            searchError = exception;
            String indices = request.request().requests().stream()
                    .map(r -> indicesToString(r.indices())).collect(Collectors.joining(", "));
            throw new ODataApplicationException(
                    String.format("One or more indices %s not fount.", indices),
                    HttpStatusCode.NOT_FOUND.getStatusCode(), Locale.ROOT, exception);
        } finally {
            log.debug(String.format("Executing query requests:%n%s", request.request().requests()));
            if (response == null) {
                log.error("Failed to execute query: ", searchError);
            }
        }
        return response;
    }

    public Client getClient() {
        return client;
    }

    /**
     * Join indices to one String value. I.e.: author, book, address -> [author,
     * book, address]
     * 
     * @param indices
     *            indices array
     * @return joined indices
     */
    private static String indicesToString(String[] indices) {
        return Arrays.asList(indices).stream().map(Object::toString)
                .collect(Collectors.joining(", ", "[", "]"));
    }

}
