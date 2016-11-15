package com.hevelian.olastic.core.edm.provider;

import java.util.Arrays;
import java.util.List;

import org.elasticsearch.client.Client;

import com.hevelian.olastic.core.common.NestedMappingStrategy;
import com.hevelian.olastic.core.elastic.mappings.IElasticToCsdlMapper;

/**
 * Implementation of {@link ElasticCsdlEdmProvider} to work with Elasticsearch
 * single index.
 */
public class SingleElasticIndexCsdlEdmProvider extends ElasticCsdlEdmProvider {

    private final String index;

    /**
     * Constructor to initialize ES Client and single index to work with.
     * 
     * @param client
     *            ES Client
     * @param index
     *            index name
     */
    public SingleElasticIndexCsdlEdmProvider(Client client, String index) {
        super(client);
        this.index = index;
    }

    /**
     * Constructor to initialize ES Client, single index to work with and
     * {@link IElasticToCsdlMapper} implementation.
     * 
     * @param client
     *            ES Client
     * @param index
     *            index name
     * @param csdlMapper
     *            ES to CSDL mapper
     */
    public SingleElasticIndexCsdlEdmProvider(Client client, String index,
            IElasticToCsdlMapper csdlMapper) {
        super(client, csdlMapper);
        this.index = index;
    }

    /**
     * Constructor to initialize ES Client, single index to work with and custom
     * {@link NestedMappingStrategy} implementation.
     * 
     * @param client
     *            ES Client
     * @param indices
     *            indices names
     * @param nestedMappingStrategy
     *            mapping strategy
     */
    public SingleElasticIndexCsdlEdmProvider(Client client, String index,
            NestedMappingStrategy nestedMappingStrategy) {
        super(client, nestedMappingStrategy);
        this.index = index;
    }

    /**
     * Constructor to initialize ES Client, single index to work with,
     * {@link IElasticToCsdlMapper} and {@link NestedMappingStrategy}
     * implementations.
     * 
     * @param client
     *            ES Client
     * @param index
     *            index name
     * @param csdlMapper
     *            ES to CSDL mapper
     * @param nested
     *            mapping strategy
     */
    public SingleElasticIndexCsdlEdmProvider(Client client, String index,
            IElasticToCsdlMapper csdlMapper, NestedMappingStrategy nestedMappingStrategy) {
        super(client, csdlMapper, nestedMappingStrategy);
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
