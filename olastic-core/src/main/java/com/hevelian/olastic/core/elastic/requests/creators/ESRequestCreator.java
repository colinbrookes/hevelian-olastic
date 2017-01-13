package com.hevelian.olastic.core.elastic.requests.creators;

import org.apache.olingo.server.api.ODataApplicationException;
import org.apache.olingo.server.api.uri.UriInfo;

import com.hevelian.olastic.core.elastic.requests.ESRequest;
import com.hevelian.olastic.core.elastic.requests.SearchRequest;

/**
 * Interface to provide behavior to create {@link SearchRequest} to retrieve
 * data from Elasticsearch.
 * 
 * @author rdidyk
 */
public interface ESRequestCreator {

    /**
     * Creates request to retrieve data.
     * 
     * @param uriInfo
     *            URI info
     * @return created request
     * @throws ODataApplicationException
     *             if any error occurred during request creation
     */
    ESRequest create(UriInfo uriInfo) throws ODataApplicationException;

}
