package com.hevelian.olastic.core.processors.impl;

import org.apache.olingo.commons.api.data.AbstractEntityCollection;
import org.apache.olingo.commons.api.edm.EdmEntityType;
import org.apache.olingo.server.api.ODataApplicationException;
import org.apache.olingo.server.api.uri.UriInfo;
import org.elasticsearch.action.search.SearchResponse;

import com.hevelian.olastic.core.edm.ElasticEdmEntitySet;
import com.hevelian.olastic.core.elastic.parsers.MetricsAggregationsParser;
import com.hevelian.olastic.core.elastic.requests.AggregateRequest;
import com.hevelian.olastic.core.elastic.requests.ESRequest;
import com.hevelian.olastic.core.elastic.requests.creators.MetricsAggregationsRequestCreator;
import com.hevelian.olastic.core.processors.AbstractESCollectionProcessor;
import com.hevelian.olastic.core.processors.data.InstanceData;

/**
 * Custom Elastic processor for handling metrics aggregations.
 * 
 * @author rdidyk
 */
public class MetricsAggregationsProccessor extends AbstractESCollectionProcessor {

    private String countAlias;

    @Override
    protected ESRequest createRequest(UriInfo uriInfo) throws ODataApplicationException {
        AggregateRequest request = new MetricsAggregationsRequestCreator().create(uriInfo);
        countAlias = request.getCountAlias();
        return request;
    }

    @Override
    protected InstanceData<EdmEntityType, AbstractEntityCollection> parseResponse(
            SearchResponse response, ElasticEdmEntitySet entitySet) {
        return new MetricsAggregationsParser(countAlias).parse(response, entitySet);
    }

}
