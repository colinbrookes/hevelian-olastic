package com.hevelian.olastic.core.common;

import com.hevelian.olastic.core.elastic.mappings.ElasticToCsdlMapper;
import com.hevelian.olastic.core.elastic.mappings.MappingMetaDataProvider;

/**
 * Default implementation of nested type mapper when types have same properties
 * names for nested object.
 * 
 * @author rdidyk
 */
public class NestedPerIndexMapper extends AbstractNestedTypeMapper {

    /**
     * Constructor to initialize values.
     * 
     * @param mappingMetaDataProvider
     *            mapping meta data provide
     * @param csdlMapper
     *            Elasticsearch to CSDL mapper
     */
    public NestedPerIndexMapper(MappingMetaDataProvider mappingMetaDataProvider,
            ElasticToCsdlMapper csdlMapper) {
        super(mappingMetaDataProvider, csdlMapper);
    }

    @Override
    public String getComplexTypeName(String type, String field) {
        return field;
    }

}
