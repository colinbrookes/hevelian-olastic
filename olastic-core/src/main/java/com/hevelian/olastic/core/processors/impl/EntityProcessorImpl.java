package com.hevelian.olastic.core.processors.impl;

import org.apache.olingo.commons.api.data.Entity;
import org.apache.olingo.commons.api.edm.EdmEntityType;
import org.apache.olingo.server.api.ODataApplicationException;
import org.apache.olingo.server.api.uri.UriInfo;
import org.elasticsearch.action.search.SearchResponse;

import com.hevelian.olastic.core.edm.ElasticEdmEntitySet;
import com.hevelian.olastic.core.elastic.parsers.EntityParser;
import com.hevelian.olastic.core.elastic.requests.ESRequest;
import com.hevelian.olastic.core.elastic.requests.creators.SearchRequestCreator;
import com.hevelian.olastic.core.processors.AbstractESEntityProcessor;
import com.hevelian.olastic.core.processors.data.InstanceData;

/**
 * Custom Elastic Processor for handling a single instance of an Entity Type.
 *
 * @author rdidyk
 */
public class EntityProcessorImpl extends AbstractESEntityProcessor {

    @Override
    protected ESRequest createRequest(UriInfo uriInfo) throws ODataApplicationException {
        return new SearchRequestCreator().create(uriInfo);
    }

    @Override
    protected InstanceData<EdmEntityType, Entity> parseResponse(SearchResponse response,
            ElasticEdmEntitySet entitySet) throws ODataApplicationException {
        return new EntityParser().parse(response, entitySet);
    }

}