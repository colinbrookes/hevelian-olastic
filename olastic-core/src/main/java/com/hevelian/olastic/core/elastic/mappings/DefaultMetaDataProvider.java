package com.hevelian.olastic.core.elastic.mappings;

import java.io.IOException;

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

import lombok.extern.log4j.Log4j2;

/**
 * Basic implementation of the mapping metadata provider that retrieves mappings
 * from the Elasticsearch using the user defined client instance.
 * 
 * @author yuflyud
 */
@Log4j2
public class DefaultMetaDataProvider implements MappingMetaDataProvider {

    private final Client client;

    public DefaultMetaDataProvider(Client client) {
        this.client = client;
    }

    @Override
    public ImmutableOpenMap<String, MappingMetaData> getAllMappings(String index) {
        return new GetMappingsRequestBuilder(getClient(), GetMappingsAction.INSTANCE, index).get()
                .mappings().get(index);
    }

    @Override
    public MappingMetaData getMappingForType(String index, String type) {
        GetMappingsResponse mappingsResponse = getClient().admin().indices()
                .prepareGetMappings(index).addTypes(type).execute().actionGet();
        return mappingsResponse.getMappings().get(index).get(type);
    }

    @Override
    public ImmutableOpenMap<String, FieldMappingMetaData> getMappingsForField(String index,
            String field) {
        GetMappingsResponse typeMappingsResponse = getClient().admin().indices()
                .prepareGetMappings(index).execute().actionGet();
        ImmutableOpenMap.Builder<String, FieldMappingMetaData> mappingsMapBuilder = new ImmutableOpenMap.Builder<>();
        // TODO this workaround was implemented because of this ES 5.x issue
        // https://github.com/elastic/elasticsearch/issues/22209
        // revert this when the issue is fixed
        try {
            Object[] mappings = typeMappingsResponse.getMappings().get(index).values().toArray();
            for (Object mapping : mappings) {
                MappingMetaData mappingMetaData = (MappingMetaData) mapping;
                String type = mappingMetaData.type();
                XContentBuilder xContentBuilder = XContentFactory.contentBuilder(XContentType.JSON);
                Object fieldMapping = mappingMetaData.getSourceAsMap().get(field);
                xContentBuilder.startObject();
                xContentBuilder.field(field, fieldMapping);
                xContentBuilder.endObject();
                mappingsMapBuilder.put(type,
                        new FieldMappingMetaData(field, xContentBuilder.bytes()));
            }
        } catch (IOException e) {
            log.debug(e);
            throw new ODataRuntimeException("Can't parse elasticsearch mapping");
        }
        return mappingsMapBuilder.build();
    }

    @Override
    public FieldMappingMetaData getMappingForField(String index, String type, String field) {
        GetFieldMappingsResponse fieldMappingsResponse = getClient().admin().indices()
                .prepareGetFieldMappings(index).setTypes(type).setFields(field).execute()
                .actionGet();
        return fieldMappingsResponse.mappings().get(index).get(type).get(field);
    }

    public Client getClient() {
        return client;
    }
}