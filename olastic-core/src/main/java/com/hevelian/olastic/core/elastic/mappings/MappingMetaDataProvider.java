package com.hevelian.olastic.core.elastic.mappings;

import org.elasticsearch.action.admin.indices.mapping.get.GetFieldMappingsResponse.FieldMappingMetaData;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.collect.ImmutableOpenMap;

/**
 * Provider of Elasticsearch mappings. This interface can be used for
 * implementations that work with other data storages as well, but was designed
 * for Elasticsearch and uses its client library's classes as return types.
 * 
 * @author yuflyud
 */
// TODO describe more the behavior in case the index/type is not found.
public interface MappingMetaDataProvider {
    /**
     * Return all the mappings for all the types inside a single index.
     * 
     * @param index
     *            name of the index.
     * @return Type/Mapping map.
     */
    ImmutableOpenMap<String, MappingMetaData> getAllMappings(String index);

    /**
     * Get mapping for a single type. The {@link #getAllMappings(String)} should
     * be used if the mappings for all the types are required.
     * 
     * @param index
     *            name of the index.
     * @param type
     *            name of the type within the index.
     * @return mapping metadata for a single type.
     */
    MappingMetaData getMappingForType(String index, String type);

    /**
     * Get all mappings for fields with the requested name within a single
     * instance.
     * 
     * @param index
     *            name of the index.
     * @param field
     *            name of the field.
     * @return type/field mapping map.
     */
    ImmutableOpenMap<String, FieldMappingMetaData> getMappingsForField(String index, String field);

    /**
     * Get mapping for a single field within a single type.
     * 
     * @param index
     *            name of the index.
     * @param type
     *            name of the type.
     * @param field
     *            name of the field.
     * @return mapping metadata for a single field.
     */
    FieldMappingMetaData getMappingForField(String index, String type, String field);
}
