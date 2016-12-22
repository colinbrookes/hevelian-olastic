package com.hevelian.olastic.config;

import java.util.Set;
import java.util.function.Predicate;

import org.elasticsearch.client.Client;

/**
 * Interface to provide behavior for Elasticsearch configuration class.
 * 
 * @author rdidyk
 */
public interface ESConfig {

    /**
     * Return's attribute name to specify it in context.
     * 
     * @return attribute name
     */
    public static String getName() {
        return "ElasticsearchConfiguration";
    }

    /**
     * Return's {@link Client} instance.
     * 
     * @return client
     */
    Client getClient();

    /**
     * Close {@link Client}.
     */
    void close();

    /**
     * Return's indices from client.
     * 
     * @return indices set
     */
    Set<String> getIndices();

    /**
     * Return's filtered indices from client.
     * 
     * @param filter
     *            filter predicate
     * @return indices set
     */
    Set<String> getIndices(Predicate<String> filter);
}
