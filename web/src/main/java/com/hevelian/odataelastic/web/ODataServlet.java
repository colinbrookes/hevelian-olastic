package com.hevelian.odataelastic.web;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
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
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

import com.hevelian.odataelastic.core.edm.provider.MultyElasticIndexCsdlEdmProvider;

/**
 * OData servlet that currently connects to the local instance of the
 * Elasticsearch and exposes its mappings and data through OData interface.
 * 
 * @author yuflyud
 *
 */
// TODO implement data providers, specify client url through some config. Make
// some abstraction for servlets to make them more flexible.
public class ODataServlet extends HttpServlet {
    private static final long serialVersionUID = -7048611704658443045L;
    private static Client CLIENT;
    private static Set<String> INDICES;

    // TODOdo no do the initialization in a static block.
    static {
        Settings settings = Settings.settingsBuilder().build();
        try {
            CLIENT = TransportClient.builder().settings(settings).build().addTransportAddress(
                    new InetSocketTransportAddress(InetAddress.getByName("localhost"), 9300));
            INDICES = CLIENT.admin().indices().stats(new IndicesStatsRequest()).actionGet()
                    .getIndices().keySet();
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
    }

    @Override
    protected void service(HttpServletRequest req, HttpServletResponse resp)
            throws ServletException, IOException {
        OData odata = OData.newInstance();
        ServiceMetadata edm = odata.createServiceMetadata(
                new MultyElasticIndexCsdlEdmProvider(CLIENT,
                        Arrays.asList(INDICES.toArray(new String[] {}))),
                new ArrayList<EdmxReference>());
        ODataHttpHandler handler = odata.createHandler(edm);
        // handler.register(new DefaultDebugSupport());
        handler.process(req, resp);
    }
}
