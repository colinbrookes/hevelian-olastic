package com.hevelian.olastic.core.processors.data;

import java.util.Arrays;
import java.util.List;
import java.util.Locale;

import org.apache.olingo.commons.api.data.Property;
import org.apache.olingo.commons.api.data.ValueType;
import org.apache.olingo.commons.api.edm.EdmPrimitiveType;
import org.apache.olingo.commons.api.format.ContentType;
import org.apache.olingo.commons.api.http.HttpStatusCode;
import org.apache.olingo.server.api.ODataApplicationException;
import org.apache.olingo.server.api.serializer.ODataSerializer;
import org.apache.olingo.server.api.serializer.PrimitiveSerializerOptions;
import org.apache.olingo.server.api.serializer.SerializerException;
import org.apache.olingo.server.api.serializer.SerializerResult;
import org.apache.olingo.server.api.uri.UriInfo;
import org.apache.olingo.server.api.uri.UriResource;
import org.apache.olingo.server.api.uri.UriResourceProperty;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.search.SearchHit;

import com.hevelian.olastic.core.ElasticOData;
import com.hevelian.olastic.core.ElasticServiceMetadata;
import com.hevelian.olastic.core.edm.ElasticEdmEntitySet;
import com.hevelian.olastic.core.edm.ElasticEdmProperty;
import com.hevelian.olastic.core.elastic.ElasticConstants;

/**
 * Provides high-level methods for retrieving and converting the data for only
 * one field.
 */
public class FieldRetriever extends DataRetriever {

    /**
     * Fully initializes {@link FieldRetriever}.
     *
     * @param uriInfo
     *            uriInfo object
     * @param odata
     *            odata instance
     * @param client
     *            ES raw client
     * @param rawBaseUri
     *            war base uri
     * @param serviceMetadata
     *            service metadata
     * @param responseFormat
     *            response format
     */
    public FieldRetriever(UriInfo uriInfo, ElasticOData odata, Client client, String rawBaseUri,
            ElasticServiceMetadata serviceMetadata, ContentType responseFormat) {
        super(uriInfo, odata, client, rawBaseUri, serviceMetadata, responseFormat);
    }

    @Override
    protected int getUsefulPartsSize() {
        // the last part is primitive field name
        return getUriInfo().getUriResourceParts().size() - 1;
    }

    @Override
    protected List<String> getSelectList() {
        List<UriResource> resourceParts = getUriInfo().getUriResourceParts();
        UriResourceProperty uriProperty = (UriResourceProperty) resourceParts
                .get(resourceParts.size() - 1);
        return Arrays.asList(uriProperty.getProperty().getName());
    }

    @Override
    protected SerializerResult serialize(SearchResponse response, ElasticEdmEntitySet entitySet)
            throws ODataApplicationException {
        List<UriResource> resourceParts = getUriInfo().getUriResourceParts();
        UriResourceProperty uriProperty = (UriResourceProperty) resourceParts
                .get(resourceParts.size() - 1);
        ElasticEdmProperty edmProperty = (ElasticEdmProperty) uriProperty.getProperty();
        String propertyName = edmProperty.getName();

        if (response.getHits().getTotalHits() == 0) {
            throw new ODataApplicationException("Entity not found",
                    HttpStatusCode.NOT_FOUND.getStatusCode(), Locale.ENGLISH);
        }
        SearchHit hit = response.getHits().getAt(0);
        // TODO take a lok to this
        Object value = propertyName == ElasticConstants.ID_FIELD_NAME ? hit.getId()
                : hit.getSource().get(edmProperty.getEField());
        Property property = new Property(null, propertyName, ValueType.PRIMITIVE, value);
        SerializerResult serializerResult = null;
        if (value != null) {
            try {
                ODataSerializer serializer = getOdata().createSerializer(getResponseFormat());
                serializerResult = serializer
                        .primitive(getServiceMetadata(), (EdmPrimitiveType) edmProperty.getType(),
                                property,
                                PrimitiveSerializerOptions.with()
                                        .contextURL(getContextUrl(entitySet, true, null, null,
                                                propertyName))
                                        .scale(edmProperty.getScale())
                                        .nullable(edmProperty.isNullable())
                                        .precision(edmProperty.getPrecision())
                                        .maxLength(edmProperty.getMaxLength())
                                        .unicode(edmProperty.isUnicode()).build());
            } catch (SerializerException e) {
                throw new ODataApplicationException("Failed to serialize data.",
                        HttpStatusCode.INTERNAL_SERVER_ERROR.getStatusCode(), Locale.ROOT, e);
            }
        }
        return serializerResult;
    }
}
