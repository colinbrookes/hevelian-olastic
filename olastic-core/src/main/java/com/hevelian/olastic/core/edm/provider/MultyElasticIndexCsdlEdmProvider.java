package com.hevelian.olastic.core.edm.provider;

import java.util.ArrayList;
import java.util.List;

import org.elasticsearch.client.Client;

public class MultyElasticIndexCsdlEdmProvider extends ElasticCsdlEdmProvider {

    private final List<String> indices;

    public MultyElasticIndexCsdlEdmProvider(Client client, List<String> indices) {
        super(client);
        this.indices = indices;
    }

    @Override
    protected List<String> getSchemaNamespaces() {
        List<String> namespaces = new ArrayList<>(indices.size());
        for (String str : indices) {
            namespaces.add(getNamespace() + "." + str);
        }
        return namespaces;
    }

    @Override
    protected String namespaceToIndex(String namespace) {
        if(!getSchemaNamespaces().contains(namespace)){
            return null;
        }
        return namespace.substring(namespace.lastIndexOf(".")+1);
    }

}
