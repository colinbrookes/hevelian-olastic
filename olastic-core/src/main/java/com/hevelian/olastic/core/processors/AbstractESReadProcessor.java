package com.hevelian.olastic.core.processors;

import com.hevelian.olastic.core.ElasticOData;
import com.hevelian.olastic.core.ElasticServiceMetadata;
import com.hevelian.olastic.core.edm.ElasticEdmEntitySet;
import com.hevelian.olastic.core.elastic.requests.ESRequest;
import com.hevelian.olastic.core.elastic.requests.SearchRequest;
import com.hevelian.olastic.core.processors.data.InstanceData;
import org.apache.olingo.commons.api.data.ContextURL;
import org.apache.olingo.commons.api.data.ContextURL.Suffix;
import org.apache.olingo.commons.api.format.ContentType;
import org.apache.olingo.commons.api.http.HttpHeader;
import org.apache.olingo.commons.api.http.HttpStatusCode;
import org.apache.olingo.server.api.ODataApplicationException;
import org.apache.olingo.server.api.ODataLibraryException;
import org.apache.olingo.server.api.ODataRequest;
import org.apache.olingo.server.api.ODataResponse;
import org.apache.olingo.server.api.serializer.ODataSerializer;
import org.apache.olingo.server.api.serializer.SerializerException;
import org.apache.olingo.server.api.serializer.SerializerResult;
import org.apache.olingo.server.api.uri.UriInfo;
import org.apache.olingo.server.api.uri.queryoption.ExpandOption;
import org.apache.olingo.server.api.uri.queryoption.SelectOption;
import org.elasticsearch.action.search.SearchResponse;

/**
 * Abstract class with template method to provide behavior for all read
 * processors.
 *
 * @param <T>
 *            instance data type class
 * @param <V>
 *            instance data value class
 * @author rdidyk
 */
public abstract class AbstractESReadProcessor<T, V> implements ESReadProcessor {

    protected ElasticOData odata;
    protected ElasticServiceMetadata serviceMetadata;
    protected ODataRequest request;

    @Override
    public void init(ElasticOData odata, ElasticServiceMetadata serviceMetadata) {
        this.odata = odata;
        this.serviceMetadata = serviceMetadata;
    }

    /**
     * Method is a template to provide behavior for all read processors.
     */
    @Override
    public void read(ODataRequest request, ODataResponse response, UriInfo uriInfo,
            ContentType responseFormat) throws ODataApplicationException, ODataLibraryException {
        this.request = request;
        ESRequest searchRequest = createRequest(uriInfo);
        ElasticEdmEntitySet entitySet = searchRequest.getEntitySet();
        SearchResponse searchResponse = searchRequest.execute();
        InstanceData<T, V> data = parseResponse(searchResponse, entitySet);

        ODataSerializer serializer = odata.createSerializer(responseFormat);
        SerializerResult serializerResult = serialize(serializer, data, entitySet, uriInfo);
        response.setContent(serializerResult.getContent());
        response.setStatusCode(HttpStatusCode.OK.getStatusCode());
        response.setHeader(HttpHeader.CONTENT_TYPE, responseFormat.toContentTypeString());
    }

    /**
     * Creates request to read data from Elasticsearch.
     *
     * @param uriInfo
     *            URI info for request
     * @return created {@link SearchRequest} instance
     * @throws ODataApplicationException
     */
    protected abstract ESRequest createRequest(UriInfo uriInfo) throws ODataApplicationException;

    /**
     * Parse response from Elasticsearch and returns instance data with type and
     * value to serialize.
     *
     * @param response
     *            response from Elasticsearch
     * @param entitySet
     *            the edm entity set
     * @return instance data with type and value
     * @throws ODataApplicationException
     *             if any error occurred during parsing response
     */
    protected abstract InstanceData<T, V> parseResponse(SearchResponse response,
            ElasticEdmEntitySet entitySet) throws ODataApplicationException;

    /**
     * Serializes instance data.
     *
     * @param serializer
     *            responsible serializer
     * @param data
     *            data to serialize
     * @param entitySet
     *            the emd entity set
     * @param uriInfo
     *            URI info
     * @return serialized result
     * @throws SerializerException
     *             if any error occurred during serialization
     */
    protected abstract SerializerResult serialize(ODataSerializer serializer,
            InstanceData<T, V> data, ElasticEdmEntitySet entitySet, UriInfo uriInfo)
            throws SerializerException;

    /**
     * Creates context URL for response serializer.
     *
     * @param entitySet
     *            the edm entity set
     * @param isSingleEntity
     *            is single entity
     * @param expand
     *            expand option
     * @param select
     *            select option
     * @param navOrPropertyPath
     *            property path
     * @return created context URL
     * @throws SerializerException
     *             if any error occurred
     */
    protected ContextURL createContextUrl(ElasticEdmEntitySet entitySet, boolean isSingleEntity,
            ExpandOption expand, SelectOption select, String navOrPropertyPath)
            throws SerializerException {
        return ContextURL.with().entitySet(entitySet)
                .selectList(odata.createUriHelper()
                        .buildContextURLSelectList(entitySet.getEntityType(), expand, select))
                .suffix(isSingleEntity ? Suffix.ENTITY : null).navOrPropertyPath(navOrPropertyPath)
                .build();
    }
}
