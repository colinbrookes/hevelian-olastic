package com.hevelian.olastic.core.elastic.requests.creators;

import org.apache.olingo.server.api.ODataApplicationException;
import org.apache.olingo.server.api.uri.UriInfo;

import com.hevelian.olastic.core.elastic.builders.ESQueryBuilder;
import com.hevelian.olastic.core.elastic.queries.Query;
import com.hevelian.olastic.core.elastic.requests.ESMultiRequest;

/**
 * Contains logic needed for creating ES request.
 *
 * @author Taras Kohut
 * @param <T>
 *            query instance
 */
public abstract class MultiRequestCreator<T extends Query> extends RequestCreator {

    /**
     * Constructor to initialize default ES query builder.
     */
    public MultiRequestCreator() {
    }

    /**
     * Constructor to initialize ES query builder.
     * 
     * @param queryBuilder
     *            Es query builder instance
     */
    public MultiRequestCreator(ESQueryBuilder<?> queryBuilder) {
        super(queryBuilder);
    }

    /**
     * Creates multi request to retrieve data.
     *
     * @param uriInfo
     *            URI info
     * @return created request
     * @throws ODataApplicationException
     *             if any error occurred during request creation
     */
    public abstract ESMultiRequest<T> create(UriInfo uriInfo) throws ODataApplicationException;

}
