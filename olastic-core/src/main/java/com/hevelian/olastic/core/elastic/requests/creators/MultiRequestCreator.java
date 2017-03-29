package com.hevelian.olastic.core.elastic.requests.creators;

import org.apache.olingo.server.api.ODataApplicationException;
import org.apache.olingo.server.api.uri.UriInfo;

import com.hevelian.olastic.core.elastic.builders.ESQueryBuilder;
import com.hevelian.olastic.core.elastic.queries.Query;
import com.hevelian.olastic.core.elastic.requests.ESMultiRequest;
import com.hevelian.olastic.core.elastic.requests.MultiSearchRequest;

/**
 * Contains logic needed for creating ES {@link MultiSearchRequest}.
 *
 * @author Taras Kohut
 */
public abstract class MultiRequestCreator extends RequestCreator {

    /**
     * Constructor to initialize default ES query builder.
     */
    public MultiRequestCreator() {
    }

    /**
     * Constructor to initialize ES query builder.
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
    public abstract ESMultiRequest<? extends Query> create(UriInfo uriInfo)
            throws ODataApplicationException;

}
