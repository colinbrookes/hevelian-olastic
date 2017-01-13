package com.hevelian.olastic.core.processors.impl;

import static com.hevelian.olastic.core.utils.ProcessorUtils.throwNotImplemented;

import java.util.List;

import org.apache.olingo.commons.api.data.Property;
import org.apache.olingo.commons.api.edm.EdmPrimitiveType;
import org.apache.olingo.commons.api.format.ContentType;
import org.apache.olingo.server.api.ODataApplicationException;
import org.apache.olingo.server.api.ODataLibraryException;
import org.apache.olingo.server.api.ODataRequest;
import org.apache.olingo.server.api.ODataResponse;
import org.apache.olingo.server.api.serializer.ODataSerializer;
import org.apache.olingo.server.api.serializer.PrimitiveSerializerOptions;
import org.apache.olingo.server.api.serializer.SerializerException;
import org.apache.olingo.server.api.serializer.SerializerResult;
import org.apache.olingo.server.api.uri.UriInfo;
import org.apache.olingo.server.api.uri.UriResource;
import org.apache.olingo.server.api.uri.UriResourceProperty;
import org.elasticsearch.action.search.SearchResponse;

import com.hevelian.olastic.core.edm.ElasticEdmEntitySet;
import com.hevelian.olastic.core.edm.ElasticEdmProperty;
import com.hevelian.olastic.core.elastic.parsers.PrimitiveParser;
import com.hevelian.olastic.core.elastic.requests.ESRequest;
import com.hevelian.olastic.core.elastic.requests.creators.SearchRequestCreator;
import com.hevelian.olastic.core.processors.ESPrimitiveProcessor;
import com.hevelian.olastic.core.processors.data.InstanceData;

/**
 * Custom elastic Processor for handling an instance of a primitive type, e.g.,
 * a primitive property of an entity.
 * 
 * @author rdidyk
 */
public class PrimitiveProcessor extends ESPrimitiveProcessor {

    @Override
    public void readPrimitive(ODataRequest request, ODataResponse response, UriInfo uriInfo,
            ContentType responseFormat) throws ODataApplicationException, ODataLibraryException {
        read(request, response, uriInfo, responseFormat);
    }

    @Override
    protected ESRequest createRequest(UriInfo uriInfo) throws ODataApplicationException {
        return new SearchRequestCreator().create(uriInfo);
    }

    @Override
    protected InstanceData<EdmPrimitiveType, Property> parseResponse(SearchResponse response,
            ElasticEdmEntitySet entitySet) {
        return new PrimitiveParser().parse(response, entitySet);
    }

    @Override
    protected SerializerResult serialize(ODataSerializer serializer,
            InstanceData<EdmPrimitiveType, Property> data, ElasticEdmEntitySet entitySet,
            UriInfo uriInfo) throws SerializerException {
        List<UriResource> resourceParts = uriInfo.getUriResourceParts();
        UriResourceProperty uriProperty = (UriResourceProperty) resourceParts
                .get(resourceParts.size() - 1);
        ElasticEdmProperty edmProperty = (ElasticEdmProperty) uriProperty.getProperty();
        String propertyName = edmProperty.getName();
        return serializer.primitive(serviceMetadata, data.getType(), data.getValue(),
                PrimitiveSerializerOptions.with()
                        .contextURL(createContextUrl(entitySet, true, null, null, propertyName))
                        .scale(edmProperty.getScale()).nullable(edmProperty.isNullable())
                        .precision(edmProperty.getPrecision()).maxLength(edmProperty.getMaxLength())
                        .unicode(edmProperty.isUnicode()).build());
    }

    @Override
    public void updatePrimitive(ODataRequest request, ODataResponse response, UriInfo uriInfo,
            ContentType requestFormat, ContentType responseFormat)
            throws ODataApplicationException, ODataLibraryException {
        throwNotImplemented();
    }

    @Override
    public void deletePrimitive(ODataRequest request, ODataResponse response, UriInfo uriInfo)
            throws ODataApplicationException, ODataLibraryException {
        throwNotImplemented();
    }

}
