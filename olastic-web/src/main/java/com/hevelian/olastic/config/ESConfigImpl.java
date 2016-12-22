package com.hevelian.olastic.config;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.elasticsearch.action.admin.indices.stats.IndicesStatsRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

/**
 * Elasticsearch client configuration class.
 * 
 * @author rdidyk
 */
public class ESConfigImpl implements ESConfig {

    private final Client client;
    private final Set<String> indices;

    /**
     * Creates a new Elasticsearch client configuration with predefined
     * properties.
     * 
     * @param host
     *            host name
     * @param port
     *            port number
     * @param cluster
     *            cluster name
     * @throws UnknownHostException
     *             if no IP address for the host could be found
     */
    public ESConfigImpl(String host, int port, String cluster) throws UnknownHostException {
        Settings settings = Settings.builder().put("cluster.name", cluster).build();
        this.client = initClient(settings,
                new InetSocketTransportAddress(InetAddress.getByName(host), port));
        this.indices = loadIndices();
    }

    /**
     * Initialize's Elasticsearch {@link Client}.
     * 
     * @param settings
     *            the settings passed to transport client
     * @param address
     *            transport address that will be used to connect to
     * @return client instance
     */
    protected Client initClient(Settings settings, TransportAddress address) {
        PreBuiltTransportClient preBuildClient = new PreBuiltTransportClient(settings);
        preBuildClient.addTransportAddress(address);
        return preBuildClient;
    }

    /**
     * Load's indices from Elasticsearch.
     * 
     * @return indices set
     */
    protected Set<String> loadIndices() {
        return client.admin().indices().stats(new IndicesStatsRequest()).actionGet().getIndices()
                .keySet();
    }

    @Override
    public void close() {
        client.close();
    }

    @Override
    public Client getClient() {
        return client;
    }

    @Override
    public Set<String> getIndices() {
        return indices;
    }

    @Override
    public Set<String> getIndices(Predicate<String> filter) {
        return indices.stream().filter(filter).collect(Collectors.toSet());
    }

}
