package com.hevelian.olastic.web;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Set;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.olingo.commons.api.edm.provider.CsdlEdmProvider;
import org.apache.olingo.commons.api.edmx.EdmxReference;
import org.apache.olingo.server.api.OData;
import org.apache.olingo.server.api.ODataHttpHandler;
import org.apache.olingo.server.api.ServiceMetadata;
import org.elasticsearch.client.Client;

import com.hevelian.olastic.config.ESConfig;
import com.hevelian.olastic.core.ElasticOData;
import com.hevelian.olastic.core.api.edm.provider.MultyElasticIndexCsdlEdmProvider;
import com.hevelian.olastic.core.elastic.mappings.MappingMetaDataProvider;
import com.hevelian.olastic.core.processors.impl.ESEntityCollectionProcessorImpl;
import com.hevelian.olastic.core.processors.impl.ESEntityProcessorImpl;
import com.hevelian.olastic.core.processors.impl.ESPrimitiveProcessorImpl;

/**
 * OData servlet that currently connects to the local instance of the
 * Elasticsearch and exposes its mappings and data through OData interface.
 * 
 * @author yuflyud
 * @contributor rdidyk
 */
public class ODataServlet extends HttpServlet {

    private static final long serialVersionUID = -7048611704658443045L;

    private ESConfig config;

    @Override
    public void init() throws ServletException {
        config = (ESConfig) getServletContext().getAttribute(ESConfig.getName());
    }

    @Override
    protected void service(HttpServletRequest req, HttpServletResponse resp)
            throws ServletException, IOException {
        OData odata = ElasticOData.newInstance();
        CsdlEdmProvider provider = createCsdlEdmProvider(config.getClient(), config.getIndices());
        ServiceMetadata matadata = createServiceMetadata(odata, provider);
        ODataHttpHandler handler = odata.createHandler(matadata);
        registerProcessors(handler, config.getClient());
        handler.process(req, resp);
    }

    /**
     * Create's {@link ServiceMetadata} metadata.
     * 
     * @param odata
     *            OData instance
     * @param provider
     *            CSDL provider
     * @return metadata
     */
    protected ServiceMetadata createServiceMetadata(OData odata, CsdlEdmProvider provider) {
        return odata.createServiceMetadata(provider, new ArrayList<EdmxReference>());
    }

    /**
     * Create's {@link CsdlEdmProvider} provider.
     * 
     * @param client
     *            Elasticsearch client
     * @param indices
     *            indices from Elasticsearch
     * @return provider instance
     */
    protected CsdlEdmProvider createCsdlEdmProvider(Client client, Set<String> indices) {
        return new MultyElasticIndexCsdlEdmProvider(new MappingMetaDataProvider(client), indices);
    }

    /**
     * Registers additional custom processor implementations for handling OData
     * requests
     * 
     * @param handler
     *            OData handler
     * @param client
     *            Elasticsearch client
     */
    protected void registerProcessors(ODataHttpHandler handler, Client client) {
        handler.register(new ESEntityProcessorImpl(client));
        handler.register(new ESEntityCollectionProcessorImpl(client));
        handler.register(new ESPrimitiveProcessorImpl(client));
    }

    protected ESConfig getConfig() {
        return config;
    }
}
