package com.hevelian.olastic.core.utils;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Locale;

import org.apache.olingo.commons.api.ex.ODataRuntimeException;
import org.apache.olingo.commons.api.http.HttpStatusCode;
import org.apache.olingo.server.api.ODataApplicationException;

/**
 * Contains utility methods.
 * 
 * @author Taras Kohut
 * @contributor rdidyk
 */
public final class ProcessorUtils {
    private ProcessorUtils() {
    }

    /**
     * Generates id string, for example: record(2)
     * 
     * @param entitySetName
     *            name of entity set
     * @param id
     *            odata id string
     * @return
     */
    public static URI createId(String entitySetName, Object id) {
        try {
            return new URI(entitySetName + "(" + id + ")");
        } catch (URISyntaxException e) {
            throw new ODataRuntimeException("Unable to create id for entity: " + entitySetName, e);
        }
    }

    /**
     * Method throws exception with HTTP.501 status code and with appropriate
     * message.
     * 
     * @param msg
     *            message to show
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
     * @throws ODataApplicationException
     *             created exception
     */
    public static <T> T throwNotImplemented() throws ODataApplicationException {
        return throwNotImplemented("Not implemented.");
    }

}