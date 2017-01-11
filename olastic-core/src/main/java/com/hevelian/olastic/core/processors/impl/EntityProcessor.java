package com.hevelian.olastic.core.processors.impl;

import static com.hevelian.olastic.core.utils.ProcessorUtils.throwNotImplemented;

import org.apache.olingo.commons.api.data.Entity;
import org.apache.olingo.commons.api.edm.EdmEntityType;
import org.apache.olingo.commons.api.format.ContentType;
import org.apache.olingo.server.api.ODataApplicationException;
import org.apache.olingo.server.api.ODataLibraryException;
import org.apache.olingo.server.api.ODataRequest;
import org.apache.olingo.server.api.ODataResponse;
import org.apache.olingo.server.api.serializer.EntitySerializerOptions;
import org.apache.olingo.server.api.serializer.ODataSerializer;
import org.apache.olingo.server.api.serializer.SerializerException;
import org.apache.olingo.server.api.serializer.SerializerResult;
import org.apache.olingo.server.api.uri.UriInfo;
import org.apache.olingo.server.api.uri.queryoption.ExpandOption;
import org.apache.olingo.server.api.uri.queryoption.SelectOption;
import org.elasticsearch.action.search.SearchResponse;

import com.hevelian.olastic.core.edm.ElasticEdmEntitySet;
import com.hevelian.olastic.core.elastic.parsers.EntityParser;
import com.hevelian.olastic.core.elastic.requests.SearchRequest;
import com.hevelian.olastic.core.elastic.requests.creators.SearchRequestCreator;
import com.hevelian.olastic.core.processors.ESEntityProcessor;
import com.hevelian.olastic.core.processors.data.InstanceData;

/**
 * Custom Elastic Processor for handling a single instance of an Entity Type.
 * 
 * @author rdidyk
 */
public class EntityProcessor extends ESEntityProcessor {

    @Override
    public void readEntity(ODataRequest request, ODataResponse response, UriInfo uriInfo,
            ContentType responseFormat) throws ODataApplicationException, ODataLibraryException {
        read(request, response, uriInfo, responseFormat);
    }

    @Override
    protected SearchRequest createRequest(UriInfo uriInfo) throws ODataApplicationException {
        return new SearchRequestCreator().create(uriInfo);
    }

    @Override
    protected InstanceData<EdmEntityType, Entity> parseResponse(SearchResponse response,
            ElasticEdmEntitySet entitySet) {
        return new EntityParser().parse(response, entitySet);
    }

    @Override
    protected SerializerResult serialize(ODataSerializer serializer,
            InstanceData<EdmEntityType, Entity> data, ElasticEdmEntitySet entitySet,
            UriInfo uriInfo) throws SerializerException {
        ExpandOption expand = uriInfo.getExpandOption();
        SelectOption select = uriInfo.getSelectOption();
        return serializer.entity(serviceMetadata, data.getType(), data.getValue(),
                EntitySerializerOptions.with()
                        .contextURL(createContextUrl(entitySet, true, expand, select, null))
                        .select(select).expand(expand).build());
    }

    @Override
    public void createEntity(ODataRequest request, ODataResponse response, UriInfo uriInfo,
            ContentType requestFormat, ContentType responseFormat)
            throws ODataApplicationException, ODataLibraryException {
        throwNotImplemented("Not supported.");
    }

    @Override
    public void updateEntity(ODataRequest request, ODataResponse response, UriInfo uriInfo,
            ContentType requestFormat, ContentType responseFormat)
            throws ODataApplicationException, ODataLibraryException {
        throwNotImplemented("Not supported.");
    }

    @Override
    public void deleteEntity(ODataRequest request, ODataResponse response, UriInfo uriInfo)
            throws ODataApplicationException, ODataLibraryException {
        throwNotImplemented("Not supported.");
    }

}