package com.hevelian.olastic.core.elastic.mappings;

import java.util.Map;
import java.util.Map.Entry;

import org.elasticsearch.action.admin.indices.mapping.get.GetFieldMappingsResponse;
import org.elasticsearch.action.admin.indices.mapping.get.GetFieldMappingsResponse.FieldMappingMetaData;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsAction;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsRequestBuilder;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.collect.ImmutableOpenMap;

/**
 * Basic implementation of the mapping metadata provider that retrieves mappings
 * from the Elasticsearch using the user defined client instance.
 * 
 * @author yuflyud
 */
public class MappingMetaDataProvider implements IMappingMetaDataProvider {

    private final Client client;

    public MappingMetaDataProvider(Client client) {
        this.client = client;
    }

    @Override
    public ImmutableOpenMap<String, MappingMetaData> getAllMappings(String index) {
        return new GetMappingsRequestBuilder(getClient(), GetMappingsAction.INSTANCE, index).get()
                .mappings().get(index);
    }

    @Override
    public MappingMetaData getMappingForType(String index, String type) {
        GetMappingsResponse getMappingsResponse = getClient().admin().indices()
                .prepareGetMappings(index).addTypes(type).execute().actionGet();
        return getMappingsResponse.getMappings().get(index).get(type);
    }

    @Override
    public ImmutableOpenMap<String, FieldMappingMetaData> getMappingsForField(String index,
            String field) {
        GetFieldMappingsResponse fieldMappingsResponse = getClient().admin().indices()
                .prepareGetFieldMappings(index).setFields(field).execute().actionGet();
        ImmutableOpenMap.Builder<String, FieldMappingMetaData> b = new ImmutableOpenMap.Builder<>();
        for (Entry<String, Map<String, FieldMappingMetaData>> e : fieldMappingsResponse
                .mappings().get(index).entrySet()) {
            b.put(e.getKey(), (FieldMappingMetaData) e.getValue().get(field));
        }
        return b.build();
    }

    @Override
    public FieldMappingMetaData getMappingForField(String index, String type, String field) {
        GetFieldMappingsResponse getFieldMappingsResponse = getClient().admin().indices()
                .prepareGetFieldMappings(index).setTypes(type).setFields(field).execute()
                .actionGet();
        return getFieldMappingsResponse.mappings().get(index).get(type).get(field);
    }

    public Client getClient() {
        return client;
    }
}