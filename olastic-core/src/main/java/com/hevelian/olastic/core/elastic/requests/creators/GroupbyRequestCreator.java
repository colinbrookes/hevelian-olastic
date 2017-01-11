package com.hevelian.olastic.core.elastic.requests.creators;

import org.apache.olingo.server.api.ODataApplicationException;
import org.apache.olingo.server.api.uri.UriInfo;

import com.hevelian.olastic.core.elastic.requests.AggregateRequest;
import com.hevelian.olastic.core.elastic.requests.BaseRequest;

/**
 * Class responsible for creating {@link AggregateRequest} instance.
 * 
 * @author rdidyk
 */
public class GroupbyRequestCreator extends AbstractRequestCreator {

    @Override
    public BaseRequest create(UriInfo uriInfo) throws ODataApplicationException {
        BaseRequest baseRequest = super.create(uriInfo);
        // TODO implement
        return baseRequest;
    }

}
