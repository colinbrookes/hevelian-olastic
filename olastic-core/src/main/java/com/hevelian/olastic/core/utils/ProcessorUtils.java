package com.hevelian.olastic.core.utils;

import org.apache.olingo.commons.api.ex.ODataRuntimeException;
import org.apache.olingo.commons.api.http.HttpStatusCode;
import org.apache.olingo.server.api.ODataApplicationException;

import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Locale;

/**
 * Contains utility methods.
 * 
 * @author Taras Kohut
 * @author rdidyk
 */
public final class ProcessorUtils {
    private ProcessorUtils() {
    }

    /**
     * Generates id string, for example: record(2).
     * 
     * @param entitySetName
     *            name of entity set
     * @param id
     *            odata id string
     * @return id URI
     */
    public static URI createId(String entitySetName, Object id) {
        try {
            URI uri;
            Object escapedId = id;
            if (id instanceof String) {
                escapedId = URLEncoder.encode((String) id, StandardCharsets.UTF_8.name());
            }
            uri = new URI(entitySetName + "(" + escapedId + ")");
            return uri;

        } catch (URISyntaxException | UnsupportedEncodingException e) {
            throw new ODataRuntimeException("Unable to create id for entity: " + entitySetName, e);
        }
    }

    /**
     * Method throws exception with HTTP.501 status code and with appropriate
     * message.
     * 
     * @param msg
     *            message to show
     * @param <T>
     *            type
     * @return just for the signature
     * @throws ODataApplicationException
     *             created exception
     */
    public static <T> T throwNotImplemented(String msg) throws ODataApplicationException {
        throw new ODataApplicationException(msg, HttpStatusCode.NOT_IMPLEMENTED.getStatusCode(),
                Locale.ROOT);
    }

    /**
     * Method throws exception with HTTP.501 status code and with default
     * message.
     * 
     * @param <T>
     *            type
     * @return just for the signature
     * @throws ODataApplicationException
     *             created exception
     */
    public static <T> T throwNotImplemented() throws ODataApplicationException {
        return throwNotImplemented("Not implemented.");
    }

}