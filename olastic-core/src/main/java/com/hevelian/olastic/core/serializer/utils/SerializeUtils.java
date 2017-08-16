package com.hevelian.olastic.core.serializer.utils;

import java.util.Date;

import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind;
import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.apache.olingo.commons.api.ex.ODataRuntimeException;

/**
 * Utility class with helper methods for serialization.
 * 
 * @author rdidyk
 */
public final class SerializeUtils {

    private SerializeUtils() {
    }

    /**
     * Get {@link FullQualifiedName} property type by value type.
     * 
     * @param value
     *            value
     * @return property type
     */
    public static FullQualifiedName getPropertyType(Object value) {
        FullQualifiedName fqn = null;
        if (value instanceof String) {
            fqn = EdmPrimitiveTypeKind.String.getFullQualifiedName();
        } else if (value instanceof Byte) {
            fqn = EdmPrimitiveTypeKind.Byte.getFullQualifiedName();
        } else if (value instanceof Short) {
            fqn = EdmPrimitiveTypeKind.Int16.getFullQualifiedName();
        } else if (value instanceof Integer) {
            fqn = EdmPrimitiveTypeKind.Int32.getFullQualifiedName();
        } else if (value instanceof Long) {
            fqn = EdmPrimitiveTypeKind.Int64.getFullQualifiedName();
        } else if (value instanceof Double) {
            fqn = EdmPrimitiveTypeKind.Double.getFullQualifiedName();
        } else if (value instanceof Boolean) {
            fqn = EdmPrimitiveTypeKind.Boolean.getFullQualifiedName();
        } else if (value instanceof Date) {
            fqn = EdmPrimitiveTypeKind.Date.getFullQualifiedName();
        } else {
            throw new ODataRuntimeException("Property type is not supported.");
        }
        return fqn;
    }

}
