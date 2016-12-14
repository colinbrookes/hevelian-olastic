package com.hevelian.olastic.core.processors.data;

import java.util.Locale;
import java.util.Map;

import org.apache.olingo.commons.api.data.Entity;
import org.apache.olingo.commons.api.format.ContentType;
import org.apache.olingo.commons.api.http.HttpStatusCode;
import org.apache.olingo.server.api.ODataApplicationException;
import org.apache.olingo.server.api.serializer.EntitySerializerOptions;
import org.apache.olingo.server.api.serializer.ODataSerializer;
import org.apache.olingo.server.api.serializer.SerializerException;
import org.apache.olingo.server.api.serializer.SerializerResult;
import org.apache.olingo.server.api.uri.UriInfo;
import org.apache.olingo.server.api.uri.queryoption.ExpandOption;
import org.apache.olingo.server.api.uri.queryoption.SelectOption;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.search.SearchHit;

import com.hevelian.olastic.core.ElasticOData;
import com.hevelian.olastic.core.ElasticServiceMetadata;
import com.hevelian.olastic.core.edm.ElasticEdmEntitySet;
import com.hevelian.olastic.core.edm.ElasticEdmEntityType;
import com.hevelian.olastic.core.elastic.ElasticConstants;
import com.hevelian.olastic.core.utils.ProcessorUtils;

/**
 * Provides high-level methods for retrieving and converting the data for single
 * entity by id.
 * 
 * @author rdidyk
 */
public class EntityRetriever extends DataRetriever {

    /**
     * Fully initializes {@link EntityRetriever}.
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
    public EntityRetriever(UriInfo uriInfo, ElasticOData odata, Client client, String rawBaseUri,
            ElasticServiceMetadata serviceMetadata, ContentType responseFormat) {
        super(uriInfo, odata, client, rawBaseUri, serviceMetadata, responseFormat);
    }

    @Override
    protected SerializerResult serialize(SearchResponse response, ElasticEdmEntitySet entitySet)
            throws ODataApplicationException {
        ElasticEdmEntityType entityType = entitySet.getEntityType();
        SearchHit hit = response.getHits().getAt(0);
        Entity entity = new Entity();
        entity.setId(ProcessorUtils.createId(entityType.getName(), hit.getId()));
        addProperty(entity, ElasticConstants.ID_FIELD_NAME, hit.getId(), entityType);
        for (Map.Entry<String, Object> entry : hit.getSource().entrySet()) {
            addProperty(entity, entityType.findPropertyByEField(entry.getKey()).getName(),
                    entry.getValue(), entityType);
        }
        ExpandOption expand = getUriInfo().getExpandOption();
        SelectOption select = getUriInfo().getSelectOption();
        try {
            ODataSerializer serializer = getOdata().createSerializer(getResponseFormat());
            return serializer
                    .entity(getServiceMetadata(), entityType, entity,
                            EntitySerializerOptions.with()
                                    .contextURL(
                                            getContextUrl(entitySet, true, expand, select, null))
                                    .select(select).expand(expand).build());
        } catch (SerializerException e) {
            throw new ODataApplicationException("Failed to serialize data.",
                    HttpStatusCode.INTERNAL_SERVER_ERROR.getStatusCode(), Locale.ROOT, e);
        }
    }

}
