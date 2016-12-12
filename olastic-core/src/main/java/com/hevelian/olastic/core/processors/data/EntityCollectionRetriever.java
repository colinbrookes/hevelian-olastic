package com.hevelian.olastic.core.processors.data;

import java.util.Locale;
import java.util.Map;

import org.apache.olingo.commons.api.data.Entity;
import org.apache.olingo.commons.api.data.EntityCollection;
import org.apache.olingo.commons.api.format.ContentType;
import org.apache.olingo.commons.api.http.HttpStatusCode;
import org.apache.olingo.server.api.ODataApplicationException;
import org.apache.olingo.server.api.serializer.EntityCollectionSerializerOptions;
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
import com.hevelian.olastic.core.elastic.builders.ESQueryBuilder;
import com.hevelian.olastic.core.utils.Util;

/**
 * Provides high-level methods for retrieving and converting the data for
 * collection of entities.
 * 
 * @author rdidyk
 */
public class EntityCollectionRetriever extends DataRetriever {

    /**
     * Fully initializes {@link EntityCollectionRetriever}.
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
    public EntityCollectionRetriever(UriInfo uriInfo, ElasticOData odata, Client client,
            String rawBaseUri, ElasticServiceMetadata serviceMetadata, ContentType responseFormat) {
        super(uriInfo, odata, client, rawBaseUri, serviceMetadata, responseFormat);
    }

    @Override
    public SerializerResult getSerializedData() throws ODataApplicationException {
        QueryWithEntity queryWithEntity = getQueryWithEntity();
        ElasticEdmEntitySet entitySet = queryWithEntity.getEntitySet();
        ESQueryBuilder queryBuilder = queryWithEntity.getQuery();

        return serialize(retrieveData(queryBuilder, getFilterQuery()), entitySet);
    }

    @Override
    protected SerializerResult serialize(SearchResponse response, ElasticEdmEntitySet entitySet)
            throws ODataApplicationException {
        ElasticEdmEntityType entityType = entitySet.getEntityType();
        EntityCollection entities = new EntityCollection();
        for (SearchHit hit : response.getHits()) {
            Entity entity = new Entity();
            entity.setId(Util.createId(entityType.getName(), hit.getId()));
            addProperty(entity, ElasticConstants.ID_FIELD_NAME, hit.getId(), entityType);

            for (Map.Entry<String, Object> entry : hit.getSource().entrySet()) {
                addProperty(entity, entityType.findPropertyByEField(entry.getKey()).getName(),
                        entry.getValue(), entityType);
            }
            entities.getEntities().add(entity);
        }
        if (isCount()) {
            entities.setCount((int) response.getHits().getTotalHits());
        }
        return serializeEntities(entities, entitySet);
    }

    /**
     * Serializes entities.
     *
     * @param entities
     *            entities
     * @param entitySet
     *            entitySet
     * @return serialized data
     * @throws ODataApplicationException
     *             if any error occurred during serialization
     */
    protected SerializerResult serializeEntities(EntityCollection entities,
            ElasticEdmEntitySet entitySet) throws ODataApplicationException {
        ElasticEdmEntityType entityType = entitySet.getEntityType();
        String id = getRawBaseUri() + "/" + entityType.getName();
        ExpandOption expand = getUriInfo().getExpandOption();
        SelectOption select = getUriInfo().getSelectOption();
        try {
            ODataSerializer serializer = getOdata().createSerializer(getResponseFormat());
            return serializer
                    .entityCollection(getServiceMetadata(), entityType, entities,
                            EntityCollectionSerializerOptions.with()
                                    .contextURL(
                                            getContextUrl(entitySet, false, expand, select, null))
                                    .id(id).count(getUriInfo().getCountOption()).select(select)
                                    .expand(expand).build());
        } catch (SerializerException e) {
            throw new ODataApplicationException("Failed to serialize data.",
                    HttpStatusCode.INTERNAL_SERVER_ERROR.getStatusCode(), Locale.ROOT, e);
        }
    }

}
