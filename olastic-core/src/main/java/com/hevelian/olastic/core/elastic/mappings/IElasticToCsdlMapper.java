package com.hevelian.olastic.core.elastic.mappings;

import org.apache.olingo.commons.api.edm.FullQualifiedName;

/**
 * Mapper between Elasticsearch and CSDL. Interface has methods to map
 * Elasticsearch types to entity type and set, navigation properties, and for
 * entity properties.
 * 
 * @author rdidyk
 */
public interface IElasticToCsdlMapper {

    /**
     * Map Elasticsearch field to CSDL property name. By default returns the
     * name of the corresponding field.
     * 
     * @param index
     *            name of the index
     * @param type
     *            name of the type within the index
     * @param field
     *            name of the field
     * @return name of the corresponding field
     */
    String eFieldToCsdlProperty(String index, String type, String field);

    /**
     * Map Elasticsearch field to CSDL property isCollection value. By default
     * returns false.
     * 
     * @param index
     *            name of the index
     * @param type
     *            name of the type within the index
     * @param field
     *            name of the field
     * @return name of the corresponding field
     */
    boolean eFieldToCollection(String index, String type, String field);

    /**
     * Map Elasticsearch type name to CSDL entity type. By default returns the
     * name of the corresponding entity type.
     * 
     * @param index
     *            name of the index
     * @param type
     *            name of the type within the index
     * @return the corresponding entity type
     */
    FullQualifiedName eTypeToEntityType(String index, String type);

    /**
     * Map Elasticsearch index name to CSDL namespace. By default returns the
     * name of the corresponding index.
     * 
     * @param index
     *            name of the index
     * @return the corresponding namespace
     */
    String eIndexToCsdlNamespace(String index);

    /**
     * Map Elasticsearch type name to CSDL entity set name. By default returns
     * the name of the corresponding entity type.
     * 
     * @param index
     *            name of the index
     * @param type
     *            name of the type within the index
     * @return name of the corresponding entity set
     */
    String eTypeToEntitySet(String index, String type);

    /**
     * Convert a child relationship of Elasticsearch to a navigation property
     * name.
     * 
     * @param index
     *            name of the index
     * @param child
     *            name of the child type
     * @param parent
     *            name of the parent type
     * @return Navigation property name for child relationship. Default
     *         implementation returns the corresponding child entity type's name
     */
    String eChildRelationToNavPropName(String index, String child, String parent);

    /**
     * Convert a parent relationship of Elasticsearch to a navigation property
     * name.
     * 
     * @param index
     *            name of the index
     * @param parent
     *            name of the parent type
     * @param child
     *            name of the child type
     * @return Navigation property name for parent relationship. Default
     *         implementation returns the corresponding parent entity type's
     *         name
     */
    String eParentRelationToNavPropName(String index, String parent, String child);

}
