package com.hevelian.olastic.core.elastic.requests.creators;

import org.apache.olingo.server.api.ODataApplicationException;
import org.apache.olingo.server.api.uri.UriInfo;

import com.hevelian.olastic.core.elastic.builders.ESQueryBuilder;
import com.hevelian.olastic.core.elastic.requests.ESRequest;

/**
 * Contains logic needed for creating ES request.
 *
 * @author Taras Kohut
 */
public abstract class SingleRequestCreator extends RequestCreator {
    /**
     * Constructor to initialize default ES query builder.
     */
    public SingleRequestCreator() {
        super();
    }

    /**
     * Constructor to initialize ES query builder.
     * 
     * @param queryBuilder
     *            Es query builder instance
     */
    public SingleRequestCreator(ESQueryBuilder<?> queryBuilder) {
        super(queryBuilder);
    }

    /**
     * Creates request to retrieve data.
     * 
     * @param uriInfo
     *            URI info
     * @return created request
     * @throws ODataApplicationException
     *             if any error occurred during request creation
     */
    public abstract ESRequest create(UriInfo uriInfo) throws ODataApplicationException;

}
