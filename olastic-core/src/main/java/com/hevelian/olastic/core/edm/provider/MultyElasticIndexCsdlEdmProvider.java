package com.hevelian.olastic.core.edm.provider;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.client.Client;

import com.hevelian.olastic.core.edm.utils.MetaDataUtils;

public class MultyElasticIndexCsdlEdmProvider extends ElasticCsdlEdmProvider {

    private final Set<String> indices;

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
        this.indices = indices;
    }

    @Override
    protected List<String> getSchemaNamespaces() {
        List<String> namespaces = new ArrayList<>(indices.size());
        for (String index : indices) {
            namespaces.add(csdlMapper.eIndexToCsdlNamespace(index));
        }
        return namespaces;
    }

    @Override
    protected String namespaceToIndex(String namespace) {
        return getSchemaNamespaces().contains(namespace)
                ? StringUtils.substringAfterLast(namespace, MetaDataUtils.NEMESPACE_SEPARATOR)
                : null;
    }

}
