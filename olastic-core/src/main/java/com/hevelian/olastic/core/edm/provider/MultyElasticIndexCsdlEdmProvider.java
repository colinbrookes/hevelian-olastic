package com.hevelian.olastic.core.edm.provider;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.elasticsearch.client.Client;

import com.hevelian.olastic.core.elastic.mappings.IElasticToCsdlMapper;

public class MultyElasticIndexCsdlEdmProvider extends ElasticCsdlEdmProvider {

    private final Map<String, String> namespaceToIndexMap = new HashMap<>();
    private List<String> namespaces;

    /**
     * Constructor to initialize ES Client and multiple indices to work with.
     * 
     * @param client
     *            ES Client
     * @param indices
     *            indices names
     */
    public MultyElasticIndexCsdlEdmProvider(Client client, Set<String> indices) {
        super(client);
        initalizeNamespaces(indices);
    }

    /**
     * Constructor to initialize ES Client and multiple indices to work with and
     * custom {@link IElasticToCsdlMapper} implementation.
     * 
     * @param client
     *            ES Client
     * @param indices
     *            indices names
     * @param csdlMapper
     *            ES to CSDL mapper
     */
    public MultyElasticIndexCsdlEdmProvider(Client client, Set<String> indices,
            IElasticToCsdlMapper csdlMapper) {
        super(client, csdlMapper);
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
