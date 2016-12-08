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
        if (value instanceof String) {
            return EdmPrimitiveTypeKind.String.getFullQualifiedName();
        } else if (value instanceof Byte) {
            return EdmPrimitiveTypeKind.Byte.getFullQualifiedName();
        } else if (value instanceof Short) {
            return EdmPrimitiveTypeKind.Int16.getFullQualifiedName();
        } else if (value instanceof Integer) {
            return EdmPrimitiveTypeKind.Int32.getFullQualifiedName();
        } else if (value instanceof Long) {
            return EdmPrimitiveTypeKind.Int64.getFullQualifiedName();
        } else if (value instanceof Double) {
            return EdmPrimitiveTypeKind.Double.getFullQualifiedName();
        } else if (value instanceof Boolean) {
            return EdmPrimitiveTypeKind.Boolean.getFullQualifiedName();
        } else if (value instanceof Date) {
            return EdmPrimitiveTypeKind.Date.getFullQualifiedName();
        } else {
            throw new ODataRuntimeException("Property type is not supported.");
        }
    }

}
