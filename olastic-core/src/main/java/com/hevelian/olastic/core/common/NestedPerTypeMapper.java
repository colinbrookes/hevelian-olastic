package com.hevelian.olastic.core.common;

import com.hevelian.olastic.core.elastic.mappings.ElasticToCsdlMapper;
import com.hevelian.olastic.core.elastic.mappings.MappingMetaDataProvider;
import com.hevelian.olastic.core.utils.MetaDataUtils;

/**
 * Custom implementation of nested type mapper when types have different
 * properties names for same nested object name.
 * 
 * @author rdidyk
 */
public class NestedPerTypeMapper extends AbstractNestedTypeMapper {

    private static final String DEFAULT_SEPARATOR = MetaDataUtils.NAMESPACE_SEPARATOR;

    private final String separator;

    /**
     * Constructor to initialize values.
     * 
     * @param mappingMetaDataProvider
     *            mapping meta data provide
     * @param csdlMapper
     *            Elasticsearch to CSDL mapper
     */
    public NestedPerTypeMapper(MappingMetaDataProvider mappingMetaDataProvider,
            ElasticToCsdlMapper csdlMapper) {
        super(mappingMetaDataProvider, csdlMapper);
        this.separator = DEFAULT_SEPARATOR;
    }

    /**
     * Constructor to initialize values.
     * 
     * @param mappingMetaDataProvider
     *            mapping meta data provide
     * @param csdlMapper
     *            Elasticsearch to CSDL mapper
     * @param separator
     *            separator between type and field
     */
    public NestedPerTypeMapper(MappingMetaDataProvider mappingMetaDataProvider,
            ElasticToCsdlMapper csdlMapper, String separator) {
        super(mappingMetaDataProvider, csdlMapper);
        this.separator = separator;
    }

    @Override
    public String getComplexTypeName(String type, String field) {
        return type + separator + field;
    }

    public String getSeparator() {
        return separator;
    }

}
