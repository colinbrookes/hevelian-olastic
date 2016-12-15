package com.hevelian.olastic.web;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Set;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.olingo.commons.api.edmx.EdmxReference;
import org.apache.olingo.server.api.OData;
import org.apache.olingo.server.api.ODataHttpHandler;
import org.apache.olingo.server.api.ServiceMetadata;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

import com.hevelian.olastic.core.ElasticOData;
import com.hevelian.olastic.core.api.edm.provider.MultyElasticIndexCsdlEdmProvider;
import com.hevelian.olastic.core.elastic.mappings.MappingMetaDataProvider;
import com.hevelian.olastic.core.processors.impl.ESEntityCollectionProcessorImpl;
import com.hevelian.olastic.core.processors.impl.ESEntityProcessorImpl;
import com.hevelian.olastic.core.processors.impl.ESPrimitiveProcessorImpl;

import lombok.extern.log4j.Log4j2;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

/**
 * OData servlet that currently connects to the local instance of the
 * Elasticsearch and exposes its mappings and data through OData interface.
 * 
 * @author yuflyud
 *
 */
// TODO implement data providers, specify client url through some config. Make
// some abstraction for servlets to make them more flexible.
@Log4j2
public class ODataServlet extends HttpServlet {
    private static final long serialVersionUID = -7048611704658443045L;
    private static Client CLIENT;
    private static Set<String> INDICES;

    // TODO do no do the initialization in a static block.
    static {
        Settings settings = Settings.builder().put("cluster.name", "opmc-local").build();
        try {
            CLIENT = new PreBuiltTransportClient(settings).addTransportAddress(
                    new InetSocketTransportAddress(InetAddress.getByName("localhost"), 9300));
            INDICES = CLIENT.admin().indices().stats(new IndicesStatsRequest()).actionGet()
                    .getIndices().keySet();
        } catch (UnknownHostException e) {
            log.debug(e);
        }
    }

    @Override
    protected void service(HttpServletRequest req, HttpServletResponse resp)
            throws ServletException, IOException {
        OData odata = ElasticOData.newInstance();
        ServiceMetadata edm = odata.createServiceMetadata(
                new MultyElasticIndexCsdlEdmProvider(new MappingMetaDataProvider(CLIENT), INDICES),
                new ArrayList<EdmxReference>());
        ODataHttpHandler handler = odata.createHandler(edm);
        handler.register(new ESEntityProcessorImpl(CLIENT));
        handler.register(new ESEntityCollectionProcessorImpl(CLIENT));
        handler.register(new ESPrimitiveProcessorImpl(CLIENT));
        handler.process(req, resp);
    }
}
