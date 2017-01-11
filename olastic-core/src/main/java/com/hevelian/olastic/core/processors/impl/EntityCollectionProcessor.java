package com.hevelian.olastic.core.processors.impl;

import org.apache.olingo.commons.api.data.AbstractEntityCollection;
import org.apache.olingo.commons.api.edm.EdmEntityType;
import org.apache.olingo.server.api.ODataApplicationException;
import org.apache.olingo.server.api.uri.UriInfo;
import org.apache.olingo.server.api.uri.queryoption.CountOption;
import org.elasticsearch.action.search.SearchResponse;

import com.hevelian.olastic.core.edm.ElasticEdmEntitySet;
import com.hevelian.olastic.core.elastic.parsers.EntityCollectionParser;
import com.hevelian.olastic.core.elastic.requests.ESRequest;
import com.hevelian.olastic.core.elastic.requests.creators.SearchRequestCreator;
import com.hevelian.olastic.core.processors.AbstractESCollectionProcessor;
import com.hevelian.olastic.core.processors.data.InstanceData;

/**
 * Custom Elastic processor for handling a collection of entities.
 * 
 * @author rdidyk
 */
public class EntityCollectionProcessor extends AbstractESCollectionProcessor {

    private boolean isCount;

    @Override
    protected ESRequest createRequest(UriInfo uriInfo) throws ODataApplicationException {
        CountOption countOption = uriInfo.getCountOption();
        if (countOption != null) {
            isCount = countOption.getValue();
        }
        return new SearchRequestCreator().create(uriInfo);
    }

    @Override
    protected InstanceData<EdmEntityType, AbstractEntityCollection> parseResponse(
            SearchResponse response, ElasticEdmEntitySet entitySet) {
        return new EntityCollectionParser(isCount).parse(response, entitySet);
    }

}
