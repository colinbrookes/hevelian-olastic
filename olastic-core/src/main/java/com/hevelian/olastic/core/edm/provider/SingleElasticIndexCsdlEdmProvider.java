package com.hevelian.olastic.core.edm.provider;

import java.util.Arrays;
import java.util.List;

import org.elasticsearch.client.Client;

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

    @Override
    protected List<String> getSchemaNamespaces() {
        return Arrays.asList(csdlMapper.eIndexToCsdlNamespace(index));
    }

    @Override
    protected String namespaceToIndex(String namespace) {
        return getSchemaNamespaces().contains(namespace) ? index : null;
    }

}
