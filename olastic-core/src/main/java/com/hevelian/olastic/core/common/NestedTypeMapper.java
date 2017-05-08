package com.hevelian.olastic.core.common;

import com.hevelian.olastic.core.api.edm.provider.ElasticCsdlComplexType;
import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.apache.olingo.commons.api.ex.ODataException;

import java.util.List;

/**
 * Interface to provide different type of mappings for nested types. I.e.: By
 * default types in Elasticsearch have nested object with same properties, but
 * also sometimes there are types which have same nested object name but
 * different properties names, to resolve this situation implement this
 * interface.
 * 
 * @author rdidyk
 */
public interface NestedTypeMapper {

    /**
     * Get's complex types for specific Elasticsearch index.
     * 
     * @param index
     *            index name
     * @return list of complex types
     * @throws ODataException
     *             if any error occurred
     */
    List<ElasticCsdlComplexType> getComplexTypes(String index) throws ODataException;

    /**
     * Map Elasticsearch nested object name to CSDL complex type. By default
     * returns the name of the corresponding nested type.
     * 
     * @param index
     *            name of the index
     * @param type
     *            name of the type
     * @param field
     *            name of the nested object within the index
     * @return the corresponding complex type
     */
    FullQualifiedName getComplexType(String index, String type, String field);

}
