package com.hevelian.olastic.core.utils;

import org.apache.olingo.commons.api.ex.ODataRuntimeException;

/**
 * Utility class for meta data.
 * 
 * @author rdidyk
 */
public final class MetaDataUtils {

    /** Name space separator. */
    public static final String NAMESPACE_SEPARATOR = ".";

    private MetaDataUtils() {
    }

    public static <T> T castToType(Object object, Class<T> clazz) {
        if (clazz.isInstance(object)) {
            return clazz.cast(object);
        }
        throw new ODataRuntimeException(
                String.format("Invalid %s instance. Only %s class is supported.",
                        object.getClass().getSimpleName(), clazz.getName()));
    }

}
