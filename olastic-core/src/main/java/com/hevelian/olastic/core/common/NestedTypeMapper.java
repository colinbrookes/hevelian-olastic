package com.hevelian.olastic.core.common;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.apache.olingo.commons.api.edm.provider.CsdlComplexType;
import org.apache.olingo.commons.api.edm.provider.CsdlProperty;
import org.apache.olingo.commons.api.ex.ODataException;
import org.elasticsearch.index.mapper.ObjectMapper;

import com.carrotsearch.hppc.cursors.ObjectCursor;
import com.hevelian.olastic.core.api.edm.provider.ElasticCsdlProperty;
import com.hevelian.olastic.core.elastic.ElasticConstants;
import com.hevelian.olastic.core.elastic.mappings.IElasticToCsdlMapper;
import com.hevelian.olastic.core.elastic.mappings.IMappingMetaDataProvider;

/**
 * Class for mapping Elasticsearch nested types to Edm complex types.
 * 
 * @author rdidyk
 */
public class NestedTypeMapper {

    private NestedMappingStrategy nestedMappingStrategy;
    private IMappingMetaDataProvider mappingMetaDataProvider;
    private IElasticToCsdlMapper csdlMapper;
    private PrimitiveTypeMapper primitiveTypeMapper;

    /**
     * Constructor to initialize values with custom
     * {@link NestedMappingStrategy} strategy.
     * 
     * @param nestedMappingStrategy
     *            nested mapping strategy
     * @param mappingMetaDataProvider
     *            mapping meta data provide
     * @param csdlMapper
     *            Elasticsearch to CSDL mapper
     */
    public NestedTypeMapper(NestedMappingStrategy nestedMappingStrategy,
            IMappingMetaDataProvider mappingMetaDataProvider, IElasticToCsdlMapper csdlMapper) {
        this.nestedMappingStrategy = nestedMappingStrategy;
        this.mappingMetaDataProvider = mappingMetaDataProvider;
        this.csdlMapper = csdlMapper;
        this.primitiveTypeMapper = new PrimitiveTypeMapper();
    }

    /**
     * Get's complex types for specific Elasticsearch index.
     * 
     * @param index
     *            index name
     * @return list of complex types
     * @throws ODataException
     *             if any error occurred
     */
    public List<CsdlComplexType> getComplexTypes(String index) throws ODataException {
        Map<String, Set<CsdlProperty>> complexMappings = new HashMap<>();
        for (ObjectCursor<String> key : mappingMetaDataProvider.getAllMappings(index).keys()) {
            ParsedMapWrapper eTypeProperties = getTypeProperties(index, key.value);
            for (String eFieldName : eTypeProperties.map.keySet()) {
                ParsedMapWrapper fieldMap = eTypeProperties.mapValue(eFieldName);
                if (ObjectMapper.NESTED_CONTENT_TYPE
                        .equals(fieldMap.stringValue(ElasticConstants.FIELD_DATATYPE_PROPERTY))) {
                    String complexTypeName = getNestedMappingStrategy()
                            .getComplexTypeName(key.value, eFieldName);
                    Set<CsdlProperty> nestedProperties = getNestedProperties(index, key.value,
                            eFieldName, fieldMap.mapValue(ElasticConstants.PROPERTIES_PROPERTY));
                    getAndPut(complexMappings, complexTypeName, nestedProperties);
                }
            }
        }
        List<CsdlComplexType> complexTypes = new ArrayList<>();
        for (Entry<String, Set<CsdlProperty>> entry : complexMappings.entrySet()) {
            complexTypes.add(new CsdlComplexType().setName(entry.getKey())
                    .setProperties(new ArrayList<>(entry.getValue())));
        }
        return complexTypes;
    }

    private ParsedMapWrapper getTypeProperties(String index, String type) throws ODataException {
        try {
            return new ParsedMapWrapper(
                    mappingMetaDataProvider.getMappingForType(index, type).sourceAsMap())
                            .mapValue(ElasticConstants.PROPERTIES_PROPERTY);
        } catch (IOException e) {
            throw new ODataException("Unable to parse the mapping response from Elastcsearch.", e);
        }
    }

    private <K, V> void getAndPut(Map<K, Set<V>> map, K key, Set<V> newValues) {
        Set<V> oldValues = map.containsKey(key) ? map.get(key) : new HashSet<V>();
        oldValues.addAll(newValues);
        map.put(key, oldValues);
    }

    /**
     * Get's CSDL properties from nested object properties map.
     * 
     * @param index
     *            index name
     * @param nestedField
     *            nested field name
     * @param nestedProperties
     *            properties map
     * @return CSDL properties
     */
    protected Set<CsdlProperty> getNestedProperties(String index, String type, String nestedField,
            ParsedMapWrapper nestedProperties) {
        Set<CsdlProperty> complexTypeProperties = new HashSet<>();
        for (String nestedFieldName : nestedProperties.map.keySet()) {
            String eNestedFieldType = nestedProperties.mapValue(nestedFieldName)
                    .stringValue(ElasticConstants.FIELD_DATATYPE_PROPERTY);
            complexTypeProperties.add(new ElasticCsdlProperty().setEIndex(index)
                    .setEType(map(index, type, nestedField).getName()).setName(nestedFieldName)
                    .setType(primitiveTypeMapper.map(eNestedFieldType).getFullQualifiedName()));
        }
        return complexTypeProperties;
    }

    /**
     * Map Elasticsearch nested object name to CSDL complex type. By default
     * returns the name of the corresponding nested type.
     * 
     * @param index
     *            name of the index
     * @param field
     *            name of the nested object within the index
     * @return the corresponding complex type
     */
    public FullQualifiedName map(String index, String type, String field) {
        return new FullQualifiedName(csdlMapper.eIndexToCsdlNamespace(index),
                nestedMappingStrategy.getComplexTypeName(type, field));
    }

    public NestedMappingStrategy getNestedMappingStrategy() {
        return nestedMappingStrategy;
    }

}
