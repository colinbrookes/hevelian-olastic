package com.hevelian.olastic.core.api.edm.provider;

import java.util.Arrays;
import java.util.List;

import com.hevelian.olastic.core.common.NestedMappingStrategy;
import com.hevelian.olastic.core.elastic.mappings.IElasticToCsdlMapper;
import com.hevelian.olastic.core.elastic.mappings.IMappingMetaDataProvider;

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
    public SingleElasticIndexCsdlEdmProvider(IMappingMetaDataProvider metaDataProvider,
            String index) {
        super(metaDataProvider);
        this.index = index;
    }

    /**
     * Constructor to initialize mapping metadata provider, single index to work
     * with and {@link IElasticToCsdlMapper} implementation.
     * 
     * @param metaDataProvider
     *            mapping meta data provider
     * @param index
     *            index name
     * @param csdlMapper
     *            ES to CSDL mapper
     */
    public SingleElasticIndexCsdlEdmProvider(IMappingMetaDataProvider metaDataProvider,
            String index, IElasticToCsdlMapper csdlMapper) {
        super(metaDataProvider, csdlMapper);
        this.index = index;
    }

    /**
     * Constructor to initialize mapping metadata provider, single index to work
     * with and custom {@link NestedMappingStrategy} implementation.
     * 
     * @param metaDataProvider
     *            mapping meta data provider
     * @param indices
     *            indices names
     * @param nestedMappingStrategy
     *            mapping strategy
     */
    public SingleElasticIndexCsdlEdmProvider(IMappingMetaDataProvider metaDataProvider,
            String index, NestedMappingStrategy nestedMappingStrategy) {
        super(metaDataProvider, nestedMappingStrategy);
        this.index = index;
    }

    /**
     * Constructor to initialize mapping metadata provider, single index to work
     * with, {@link IElasticToCsdlMapper} and {@link NestedMappingStrategy}
     * implementations.
     * 
     * @param metaDataProvider
     *            mapping meta data provider
     * @param index
     *            index name
     * @param csdlMapper
     *            ES to CSDL mapper
     * @param nested
     *            mapping strategy
     */
    public SingleElasticIndexCsdlEdmProvider(IMappingMetaDataProvider metaDataProvider,
            String index, IElasticToCsdlMapper csdlMapper,
            NestedMappingStrategy nestedMappingStrategy) {
        super(metaDataProvider, csdlMapper, nestedMappingStrategy);
        this.index = index;
    }

    @Override
    protected List<String> getSchemaNamespaces() {
        return Arrays.asList(csdlMapper.eIndexToCsdlNamespace(index));
    }

    @Override
    protected String namespaceToIndex(String namespace) {
        return getSchemaNamespaces().contains(namespace) ? index : null;
    }

}
