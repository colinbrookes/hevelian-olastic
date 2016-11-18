package com.hevelian.olastic.core.processors.data;

import com.hevelian.olastic.core.util.Util;
import org.apache.olingo.commons.api.data.ContextURL;
import org.apache.olingo.commons.api.data.Entity;
import org.apache.olingo.commons.api.data.Property;
import org.apache.olingo.commons.api.data.ValueType;
import org.apache.olingo.commons.api.edm.EdmEntitySet;
import org.apache.olingo.commons.api.edm.EdmPrimitiveType;
import org.apache.olingo.commons.api.edm.EdmProperty;
import org.apache.olingo.commons.api.format.ContentType;
import org.apache.olingo.commons.api.http.HttpStatusCode;
import org.apache.olingo.server.api.OData;
import org.apache.olingo.server.api.ODataApplicationException;
import org.apache.olingo.server.api.ServiceMetadata;
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

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * Provides high-level methods for retrieving and converting the data for only one field.
 */
public class FieldDataRetriever extends DataRetriever{

    public FieldDataRetriever(UriInfo uriInfo, OData odata, Client client, String rawBaseUri, ServiceMetadata serviceMetadata, ContentType responseFormat) {
        super(uriInfo, odata, client, rawBaseUri, serviceMetadata, responseFormat);
    }

    @Override
    protected int getUsefulPartsSize() {
        List<UriResource> resourceParts = uriInfo.getUriResourceParts();
        return resourceParts.size() - 1; //the last part is primitive field name
    }

    @Override
    protected List<String> getSelectList() {
        List<String> result = new ArrayList<>();
        List<UriResource> resourceParts = uriInfo.getUriResourceParts();
        UriResourceProperty uriProperty = (UriResourceProperty) resourceParts
                .get(resourceParts.size() - 1);
        EdmProperty edmProperty = uriProperty.getProperty();
        String edmPropertyName = edmProperty.getName();
        result.add(edmPropertyName);
        return result;
    }

    @Override
    protected SerializerResult serialize(SearchResponse response, EdmEntitySet responseEdmEntitySet) throws SerializerException, ODataApplicationException {
        List<UriResource> resourceParts = uriInfo.getUriResourceParts();
        UriResourceProperty uriProperty = (UriResourceProperty) resourceParts
                .get(resourceParts.size() - 1);
        EdmProperty edmProperty = uriProperty.getProperty();
        String edmPropertyName = edmProperty.getName();
        EdmPrimitiveType edmPropertyType = (EdmPrimitiveType) edmProperty.getType();

        if (response.getHits().getTotalHits() == 0) {
            throw new ODataApplicationException("Entity not found",
                    HttpStatusCode.NOT_FOUND.getStatusCode(), Locale.ENGLISH);
        }
        SearchHit hit = response.getHits().getAt(0);
        Entity entity = new Entity();
        entity.setId(Util.createId(responseEdmEntitySet.getName(), hit.getId()));
        entity.addProperty(new Property(null, edmPropertyName, ValueType.PRIMITIVE,
                hit.getSource().get(edmPropertyName)));

        Property property = entity.getProperty(edmPropertyName);
        if (property == null) {
            throw new ODataApplicationException("Property not found",
                    HttpStatusCode.NOT_FOUND.getStatusCode(), Locale.ENGLISH);
        }

        SerializerResult serializerResult = null;
        Object value = property.getValue();
        if (value != null) {
            ODataSerializer serializer = odata.createSerializer(responseFormat);

            ContextURL contextUrl = ContextURL.with().entitySet(responseEdmEntitySet)
                    .navOrPropertyPath(edmPropertyName).build();
            PrimitiveSerializerOptions options = PrimitiveSerializerOptions.with()
                    .contextURL(contextUrl).build();
            serializerResult = serializer.primitive(serviceMetadata,
                    edmPropertyType, property, options);
        } return serializerResult;
    }
}
