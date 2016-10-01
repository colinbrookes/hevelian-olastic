package com.hevelian.odataelastic.core.edm.provider;

import java.util.Arrays;
import java.util.List;

import org.elasticsearch.client.Client;

public class SingleElasticIndexCsdlEdmProvider extends ElasticCsdlEdmProvider {

	private final String index;

	public SingleElasticIndexCsdlEdmProvider(Client client, String index) {
		super(client);
		this.index = index;
	}

	@Override
	protected List<String> getSchemaNamespaces() {
		return Arrays.asList(getNamespace());
	}

	@Override
	protected String namespaceToIndex(String namespace) {
		return index;
	}

}
