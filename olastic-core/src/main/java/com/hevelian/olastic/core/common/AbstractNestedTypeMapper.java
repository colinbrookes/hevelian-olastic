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
import org.apache.olingo.commons.api.edm.provider.CsdlProperty;
import org.apache.olingo.commons.api.ex.ODataException;
import org.elasticsearch.index.mapper.ObjectMapper;

import com.carrotsearch.hppc.cursors.ObjectCursor;
import com.hevelian.olastic.core.api.edm.provider.ElasticCsdlComplexProperty;
import com.hevelian.olastic.core.api.edm.provider.ElasticCsdlComplexType;
import com.hevelian.olastic.core.elastic.ElasticConstants;
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
	public AbstractNestedTypeMapper(MappingMetaDataProvider mappingMetaDataProvider, ElasticToCsdlMapper csdlMapper) {
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
			ParsedMapWrapper eTypeProperties = getTypeProperties(index, type);
			for (String eFieldName : eTypeProperties.map.keySet()) {
				ParsedMapWrapper fieldMap = eTypeProperties.mapValue(eFieldName);
				if (ObjectMapper.NESTED_CONTENT_TYPE
						.equals(fieldMap.stringValue(ElasticConstants.FIELD_DATATYPE_PROPERTY))) {
					Set<CsdlProperty> nestedProperties = getNestedProperties(index, type, eFieldName,
							fieldMap.mapValue(ElasticConstants.PROPERTIES_PROPERTY));
					ElasticCsdlComplexType complexType = new ElasticCsdlComplexType().setEIndex(index).setEType(type)
							.setENestedType(eFieldName);
					String complexTypeName = getComplexTypeName(type,
							csdlMapper.eFieldToCsdlProperty(index, type, eFieldName));
					complexType.setName(complexTypeName);
					getAndPut(complexMappings, complexType, nestedProperties);
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

	private ParsedMapWrapper getTypeProperties(String index, String type) throws ODataException {
		try {
			return new ParsedMapWrapper(mappingMetaDataProvider.getMappingForType(index, type).sourceAsMap())
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
	 * @param nestedType
	 *            nested field name
	 * @param nestedProperties
	 *            properties map
	 * @return CSDL properties
	 */
	protected Set<CsdlProperty> getNestedProperties(String index, String type, String nestedType,
			ParsedMapWrapper nestedProperties) {
		Set<CsdlProperty> complexTypeProperties = new HashSet<>();
		for (String nestedFieldName : nestedProperties.map.keySet()) {
			String eNestedFieldType = nestedProperties.mapValue(nestedFieldName)
					.stringValue(ElasticConstants.FIELD_DATATYPE_PROPERTY);
			complexTypeProperties.add(new ElasticCsdlComplexProperty().seteNestedType(nestedType).setEIndex(index)
					.setEType(type).setName(nestedFieldName)
					.setType(primitiveTypeMapper.map(eNestedFieldType).getFullQualifiedName()));
		}
		return complexTypeProperties;
	}

	@Override
	public FullQualifiedName getComplexType(String index, String type, String field) {
		return new FullQualifiedName(csdlMapper.eIndexToCsdlNamespace(index), getComplexTypeName(type, field));
	}

	/**
	 * Get's name for complex type of Elasticsearch nested object.
	 * 
	 * @param type
	 *            type name
	 * @param field
	 *            nested field name
	 * @return
	 */
	public abstract String getComplexTypeName(String type, String field);

}
