package com.hevelian.olastic.core.common;

import static com.hevelian.olastic.core.elastic.ElasticConstants.FIELD_DATATYPE_PROPERTY;
import static com.hevelian.olastic.core.elastic.ElasticConstants.PROPERTIES_PROPERTY;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.apache.olingo.commons.api.edm.provider.CsdlProperty;
import org.apache.olingo.commons.api.ex.ODataException;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.index.mapper.ObjectMapper;

import com.carrotsearch.hppc.cursors.ObjectCursor;
import com.hevelian.olastic.core.api.edm.provider.ElasticCsdlComplexProperty;
import com.hevelian.olastic.core.api.edm.provider.ElasticCsdlComplexType;
import com.hevelian.olastic.core.elastic.mappings.ElasticToCsdlMapper;
import com.hevelian.olastic.core.elastic.mappings.MappingMetaDataProvider;

/**
 * Class for mapping Elasticsearch nested types to Edm complex types.
 * 
 * @author rdidyk
 */
public abstract class AbstractNestedTypeMapper implements NestedTypeMapper {

    private MappingMetaDataProvider mappingMetaDataProvider;
    private ElasticToCsdlMapper csdlMapper;
    private PrimitiveTypeMapper primitiveTypeMapper;

    /**
     * Constructor to initialize values.
     * 
     * @param mappingMetaDataProvider
     *            mapping meta data provide
     * @param csdlMapper
     *            Elasticsearch to CSDL mapper
     */
    public AbstractNestedTypeMapper(MappingMetaDataProvider mappingMetaDataProvider,
            ElasticToCsdlMapper csdlMapper) {
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
    public List<ElasticCsdlComplexType> getComplexTypes(String index) throws ODataException {
        Map<ElasticCsdlComplexType, Set<CsdlProperty>> complexMappings = new HashMap<>();
        for (ObjectCursor<String> key : mappingMetaDataProvider.getAllMappings(index).keys()) {
            String type = key.value;
            ParsedMapWrapper typeProperties = getTypeProperties(index, type);
            for (String field : typeProperties.getMap().keySet()) {
                ParsedMapWrapper fieldMap = typeProperties.mapValue(field);
                if (isNested(fieldMap.stringValue(FIELD_DATATYPE_PROPERTY))) {
                    ParsedMapWrapper properties = fieldMap.mapValue(PROPERTIES_PROPERTY);
                    createComplexTypes(index, type, field, properties).entrySet().stream()
                            .forEach(e -> getAndPut(complexMappings, e.getKey(), e.getValue()));
                }
            }
        }
        List<ElasticCsdlComplexType> complexTypes = new ArrayList<>();
        for (Entry<ElasticCsdlComplexType, Set<CsdlProperty>> entry : complexMappings.entrySet()) {
            ElasticCsdlComplexType complexType = entry.getKey();
            complexType.setProperties(new ArrayList<>(entry.getValue()));
            complexTypes.add(complexType);
        }
        return complexTypes;
    }

    /**
     * Creates complex type and all complex type inside it recursively.
     * 
     * @param index
     *            index name
     * @param type
     *            type name
     * @param nested
     *            nested property name
     * @param properties
     *            property mappings
     * @return all complex types
     */
    private Map<ElasticCsdlComplexType, Set<CsdlProperty>> createComplexTypes(String index,
            String type, String nested, ParsedMapWrapper properties) {
        Map<ElasticCsdlComplexType, Set<CsdlProperty>> complexMappings = new HashMap<>();
        ElasticCsdlComplexType complexType = new ElasticCsdlComplexType().setESIndex(index)
                .setESType(type).setENestedType(nested);
        complexType.setName(
                getComplexTypeName(type, csdlMapper.esFieldToCsdlProperty(index, type, nested)));

        Set<CsdlProperty> complexTypeProperties = new HashSet<>();
        for (String nestedFieldName : properties.getMap().keySet()) {
            ParsedMapWrapper nestedFieldMap = properties.mapValue(nestedFieldName);
            String nestedFieldType = nestedFieldMap.stringValue(FIELD_DATATYPE_PROPERTY);
            String mappedNestedName = csdlMapper.esFieldToCsdlProperty(index, type,
                    nestedFieldName);
            ElasticCsdlComplexProperty complexProperty = new ElasticCsdlComplexProperty()
                    .setESIndex(index).setESType(type);
            if (isNested(nestedFieldType)) {
                complexProperty.setESNestedType(nestedFieldName)
                        .setType(getComplexType(index, type, mappedNestedName));
                createComplexTypes(index, type, nestedFieldName,
                        nestedFieldMap.mapValue(PROPERTIES_PROPERTY)).entrySet().stream()
                                .forEach(entry -> getAndPut(complexMappings, entry.getKey(),
                                        entry.getValue()));
            } else {
                complexProperty.setESNestedType(nested)
                        .setType(primitiveTypeMapper.map(nestedFieldType).getFullQualifiedName());
            }
            complexTypeProperties.add(complexProperty.setName(mappedNestedName)
                    .setCollection(csdlMapper.esFieldIsCollection(index, type, nestedFieldName)));
        }
        getAndPut(complexMappings, complexType, complexTypeProperties);
        return complexMappings;
    }

    private ParsedMapWrapper getTypeProperties(String index, String type) throws ODataException {
        try {
            return new ParsedMapWrapper(
                    mappingMetaDataProvider.getMappingForType(index, type).sourceAsMap())
                            .mapValue(PROPERTIES_PROPERTY);
        } catch (ElasticsearchException e) {
            throw new ODataException("Unable to parse the mapping response from Elasticsearch.", e);
        }
    }

    @Override
    public FullQualifiedName getComplexType(String index, String type, String field) {
        return new FullQualifiedName(csdlMapper.esIndexToCsdlNamespace(index),
                getComplexTypeName(type, field));
    }

    /**
     * Get's name for complex type of Elasticsearch nested object.
     * 
     * @param type
     *            type name
     * @param field
     *            nested field name
     * @return name of complex type
     */
    public abstract String getComplexTypeName(String type, String field);

    private static <K, V> void getAndPut(Map<K, Set<V>> map, K key, Set<V> newValues) {
        Set<V> oldValues = map.containsKey(key) ? map.get(key) : new HashSet<>();
        oldValues.addAll(newValues);
        map.put(key, oldValues);
    }

    private static boolean isNested(String type) {
        return ObjectMapper.NESTED_CONTENT_TYPE.equals(type);
    }

}
