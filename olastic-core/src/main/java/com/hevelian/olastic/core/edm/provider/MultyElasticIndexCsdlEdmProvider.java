package com.hevelian.olastic.core.edm.provider;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.hevelian.olastic.core.common.NestedMappingStrategy;
import com.hevelian.olastic.core.elastic.mappings.IElasticToCsdlMapper;
import com.hevelian.olastic.core.elastic.mappings.IMappingMetaDataProvider;

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
    public MultyElasticIndexCsdlEdmProvider(IMappingMetaDataProvider metaDataProvider,
            Set<String> indices) {
        super(metaDataProvider);
        initalizeNamespaces(indices);
    }

    /**
     * Constructor to initialize mapping metadata provider, multiple indices to
     * work with and custom {@link NestedMappingStrategy} implementation.
     * 
     * @param metaDataProvider
     *            mapping meta data provider
     * @param indices
     *            indices names
     * @param nestedMappingStrategy
     *            mapping strategy
     */
    public MultyElasticIndexCsdlEdmProvider(IMappingMetaDataProvider metaDataProvider,
            Set<String> indices, NestedMappingStrategy nestedMappingStrategy) {
        super(metaDataProvider, nestedMappingStrategy);
        initalizeNamespaces(indices);
    }

    /**
     * Constructor to initialize mapping metadata provider, multiple indices to
     * work with and custom {@link IElasticToCsdlMapper} implementation.
     * 
     * @param metaDataProvider
     *            mapping meta data provider
     * @param indices
     *            indices names
     * @param csdlMapper
     *            ES to CSDL mapper
     */
    public MultyElasticIndexCsdlEdmProvider(IMappingMetaDataProvider metaDataProvider,
            Set<String> indices, IElasticToCsdlMapper csdlMapper) {
        super(metaDataProvider, csdlMapper);
        initalizeNamespaces(indices);
    }

    /**
     * Constructor to initialize mapping metadata provider, multiple indices to
     * work with, {@link IElasticToCsdlMapper} and {@link NestedMappingStrategy}
     * implementations.
     * 
     * @param metaDataProvider
     *            mapping meta data provider
     * @param indices
     *            indices names
     * @param csdlMapper
     *            ES to CSDL mapper
     * @param nestedMappingStrategy
     *            mapping strategy
     */
    public MultyElasticIndexCsdlEdmProvider(IMappingMetaDataProvider metaDataProvider,
            Set<String> indices, IElasticToCsdlMapper csdlMapper,
            NestedMappingStrategy nestedMappingStrategy) {
        super(metaDataProvider, csdlMapper, nestedMappingStrategy);
        initalizeNamespaces(indices);
    }

    /**
     * Method to initialize list of schema namespace to work with.
     * 
     * @param indices
     *            list of indices from Elasticsearch
     */
    protected void initalizeNamespaces(Set<String> indices) {
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
