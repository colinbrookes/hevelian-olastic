package com.hevelian.olastic.core.elastic.mappings;

import lombok.extern.log4j.Log4j2;
import org.apache.olingo.commons.api.ex.ODataRuntimeException;
import org.elasticsearch.action.admin.indices.mapping.get.GetFieldMappingsResponse;
import org.elasticsearch.action.admin.indices.mapping.get.GetFieldMappingsResponse.FieldMappingMetaData;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsAction;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsRequestBuilder;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;
import java.util.HashMap;

/**
 * Basic implementation of the mapping metadata provider that retrieves mappings
 * from the Elasticsearch using the user defined client instance.
 * 
 * @author yuflyud
 */
@Log4j2
public class DefaultMetaDataProvider implements MappingMetaDataProvider {
    private HashMap<String, Object> cache = new HashMap<>();
    private final Client client;

    /**
     * Initialize field.
     * 
     * @param client
     *            Elasticsearch client
     */
    public DefaultMetaDataProvider(Client client) {
        this.client = client;
    }

    @SuppressWarnings("unchecked")
    @Override
    public ImmutableOpenMap<String, MappingMetaData> getAllMappings(String index) {
        Object mapping = cache.get(makeKey(index));
        if (mapping == null) {
            mapping = new GetMappingsRequestBuilder(getClient(), GetMappingsAction.INSTANCE, index)
                    .get().mappings().get(index);
            cache.put(makeKey(index), mapping);
        }
        return (ImmutableOpenMap<String, MappingMetaData>) mapping;
    }

    @Override
    public MappingMetaData getMappingForType(String index, String type) {
        Object mapping = cache.get(makeKey(index, type));
        if (mapping == null) {
            mapping = getClient().admin().indices().prepareGetMappings(index).addTypes(type)
                    .execute().actionGet();
            cache.put(makeKey(index, type), mapping);
        }

        return ((GetMappingsResponse) mapping).getMappings().isEmpty() ? null
                : ((GetMappingsResponse) mapping).getMappings().get(index).get(type);
    }

    @Override
    public ImmutableOpenMap<String, FieldMappingMetaData> getMappingsForField(String index,
            String field) {
        Object mappingss = cache.get(makeKey(index, field));
        if (mappingss == null) {
            mappingss = getClient().admin().indices().prepareGetMappings(index).execute()
                    .actionGet();
            cache.put(makeKey(index, field), mappingss);
        }

        ImmutableOpenMap.Builder<String, FieldMappingMetaData> mappingsMapBuilder = new ImmutableOpenMap.Builder<>();
        // TODO: this workaround was implemented because of this ES 5.x issue
        // https://github.com/elastic/elasticsearch/issues/22209
        // revert this when the issue is fixed
        Object[] mappings = ((GetMappingsResponse) mappingss).getMappings().get(index).values()
                .toArray();
        for (Object mapping : mappings) {
            try (XContentBuilder contentBuilder = XContentFactory
                    .contentBuilder(XContentType.JSON)) {
                MappingMetaData mappingMetaData = (MappingMetaData) mapping;
                Object fieldMapping = mappingMetaData.getSourceAsMap().get(field);
                contentBuilder.startObject();
                contentBuilder.field(field, fieldMapping);
                contentBuilder.endObject();
                mappingsMapBuilder.put(mappingMetaData.type(),
                        new FieldMappingMetaData(field, contentBuilder.bytes()));
            } catch (IOException e) {
                log.debug(e);
                throw new ODataRuntimeException("Can't parse elasticsearch mapping");
            }
        }

        return mappingsMapBuilder.build();
    }

    @Override
    public FieldMappingMetaData getMappingForField(String index, String type, String field) {
        Object mapping = cache.get(makeKey(index, type, field));
        if (mapping == null) {
            mapping = getClient().admin().indices().prepareGetFieldMappings(index).setTypes(type)
                    .setFields(field).execute().actionGet();
            cache.put(makeKey(index, type, field), mapping);
        }

        return ((GetFieldMappingsResponse) mapping).mappings().get(index).get(type).get(field);
    }

    public Client getClient() {
        return client;
    }

    private String makeKey(String... args) {
        return String.join("/", args);
    }
}