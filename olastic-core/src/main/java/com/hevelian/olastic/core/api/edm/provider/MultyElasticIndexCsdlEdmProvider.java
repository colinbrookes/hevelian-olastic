package com.hevelian.olastic.core.api.edm.provider;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.hevelian.olastic.core.common.NestedTypeMapper;
import com.hevelian.olastic.core.elastic.mappings.ElasticToCsdlMapper;
import com.hevelian.olastic.core.elastic.mappings.MappingMetaDataProvider;

/**
 * Implementation of {@link ElasticCsdlEdmProvider} to work with Elasticsearch
 * multiple indices.
 */
public class MultyElasticIndexCsdlEdmProvider extends ElasticCsdlEdmProvider {

    private final Map<String, String> namespaceToIndexMap = new HashMap<>();
    private List<String> namespaces;

    /**
     * Constructor to initialize mapping metadata provider and multiple indices
     * to work with.
     * 
     * @param metaDataProvider
     *            mapping meta data provider
     * @param indices
     *            indices names
     */
    public MultyElasticIndexCsdlEdmProvider(MappingMetaDataProvider metaDataProvider,
            Set<String> indices) {
        super(metaDataProvider);
        initializeNamespaces(indices);
    }

    /**
     * Constructor to initialize mapping metadata provider, multiple indices to
     * work with and custom {@link NestedTypeMapper} implementation.
     * 
     * @param metaDataProvider
     *            mapping meta data provider
     * @param indices
     *            indices names
     * @param nestedTypeMapper
     *            mapping strategy
     */
    public MultyElasticIndexCsdlEdmProvider(MappingMetaDataProvider metaDataProvider,
            Set<String> indices, NestedTypeMapper nestedTypeMapper) {
        super(metaDataProvider, nestedTypeMapper);
        initializeNamespaces(indices);
    }

    /**
     * Constructor to initialize mapping metadata provider, multiple indices to
     * work with and custom {@link ElasticToCsdlMapper} implementation.
     * 
     * @param metaDataProvider
     *            mapping meta data provider
     * @param indices
     *            indices names
     * @param csdlMapper
     *            ES to CSDL mapper
     */
    public MultyElasticIndexCsdlEdmProvider(MappingMetaDataProvider metaDataProvider,
            Set<String> indices, ElasticToCsdlMapper csdlMapper) {
        super(metaDataProvider, csdlMapper);
        initializeNamespaces(indices);
    }

    /**
     * Constructor to initialize mapping metadata provider, multiple indices to
     * work with, {@link IElasticToCsdlMapper} and {@link NestedTypeMapper}
     * implementations.
     * 
     * @param metaDataProvider
     *            mapping meta data provider
     * @param indices
     *            indices names
     * @param csdlMapper
     *            ES to CSDL mapper
     * @param nestedTypeMapper
     *            mapping strategy
     */
    public MultyElasticIndexCsdlEdmProvider(MappingMetaDataProvider metaDataProvider,
            Set<String> indices, ElasticToCsdlMapper csdlMapper,
            NestedTypeMapper nestedTypeMapper) {
        super(metaDataProvider, csdlMapper, nestedTypeMapper);
        initializeNamespaces(indices);
    }

    /**
     * Method to initialize list of schema namespace to work with.
     * 
     * @param indices
     *            list of indices from Elasticsearch
     */
    protected void initializeNamespaces(Set<String> indices) {
        this.namespaces = new ArrayList<>(indices.size());
        for (String index : indices) {
            String namespace = csdlMapper.eIndexToCsdlNamespace(index);
            namespaces.add(namespace);
            namespaceToIndexMap.put(namespace, index);
        }
    }

    @Override
    protected List<String> getSchemaNamespaces() {
        return namespaces;
    }

    @Override
    protected String namespaceToIndex(String namespace) {
        return namespaceToIndexMap.get(namespace);
    }

}
