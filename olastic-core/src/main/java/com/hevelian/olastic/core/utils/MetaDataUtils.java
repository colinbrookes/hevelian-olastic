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
    /** Target path separator. */
    public static final String TARGET_SEPARATOR = "/";

    private MetaDataUtils() {
    }

    /**
     * Casts object to specified class.
     * 
     * @param object
     *            object to cast
     * @param clazz
     *            class to cast
     * @param <T>
     *            class type
     * @return casted instance, or exception will be thrown in case object is
     *         not instance of specified class
     */
    public static <T> T castToType(Object object, Class<T> clazz) {
        if (clazz.isInstance(object)) {
            return clazz.cast(object);
        }
        throw new ODataRuntimeException(
                String.format("Invalid %s instance. Only %s class is supported.",
                        object.getClass().getSimpleName(), clazz.getName()));
    }

}
