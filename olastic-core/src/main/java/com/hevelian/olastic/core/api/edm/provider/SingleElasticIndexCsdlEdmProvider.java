package com.hevelian.olastic.core.api.edm.provider;

import com.hevelian.olastic.core.common.NestedTypeMapper;
import com.hevelian.olastic.core.elastic.mappings.ElasticToCsdlMapper;
import com.hevelian.olastic.core.elastic.mappings.MappingMetaDataProvider;

import java.util.Arrays;
import java.util.List;

/**
 * Implementation of {@link ElasticCsdlEdmProvider} to work with Elasticsearch
 * single index.
 */
public class SingleElasticIndexCsdlEdmProvider extends ElasticCsdlEdmProvider {

    private final String index;

    /**
     * Constructor to initialize mapping metadata provider and single index to
     * work with.
     * 
     * @param metaDataProvider
     *            mapping meta data provider
     * @param index
     *            index name
     */
    public SingleElasticIndexCsdlEdmProvider(MappingMetaDataProvider metaDataProvider,
            String index) {
        super(metaDataProvider);
        this.index = index;
    }

    /**
     * Constructor to initialize mapping metadata provider, single index to work
     * with and {@link ElasticToCsdlMapper} implementation.
     * 
     * @param metaDataProvider
     *            mapping meta data provider
     * @param index
     *            index name
     * @param csdlMapper
     *            ES to CSDL mapper
     */
    public SingleElasticIndexCsdlEdmProvider(MappingMetaDataProvider metaDataProvider, String index,
            ElasticToCsdlMapper csdlMapper) {
        super(metaDataProvider, csdlMapper);
        this.index = index;
    }

    /**
     * Constructor to initialize mapping metadata provider, single index to work
     * with and custom {@link NestedTypeMapper} implementation.
     * 
     * @param metaDataProvider
     *            mapping meta data provider
     * @param index
     *            ES index
     * @param nestedTypeMapper
     *            nested type mapper
     */
    public SingleElasticIndexCsdlEdmProvider(MappingMetaDataProvider metaDataProvider, String index,
            NestedTypeMapper nestedTypeMapper) {
        super(metaDataProvider, nestedTypeMapper);
        this.index = index;
    }

    /**
     * Constructor to initialize mapping metadata provider, single index to work
     * with, {@link ElasticToCsdlMapper} and {@link NestedTypeMapper}
     * implementations.
     * 
     * @param metaDataProvider
     *            mapping meta data provider
     * @param index
     *            index name
     * @param csdlMapper
     *            ES to CSDL mapper
     * @param nestedTypeMapper
     *            nested type mapper
     */
    public SingleElasticIndexCsdlEdmProvider(MappingMetaDataProvider metaDataProvider, String index,
            ElasticToCsdlMapper csdlMapper, NestedTypeMapper nestedTypeMapper) {
        super(metaDataProvider, csdlMapper, nestedTypeMapper);
        this.index = index;
    }

    @Override
    protected List<String> getSchemaNamespaces() {
        return Arrays.asList(csdlMapper.esIndexToCsdlNamespace(index));
    }

    @Override
    protected String namespaceToIndex(String namespace) {
        return getSchemaNamespaces().contains(namespace) ? index : null;
    }

}
