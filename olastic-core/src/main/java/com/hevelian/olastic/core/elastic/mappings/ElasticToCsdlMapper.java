package com.hevelian.olastic.core.elastic.mappings;

import org.apache.olingo.commons.api.edm.FullQualifiedName;

import com.hevelian.olastic.core.edm.utils.MetaDataUtils;

/**
 * Default implementation of {@link IElasticToCsdlMapper} interface.
 * 
 * @author rdidyk
 */
public class ElasticToCsdlMapper implements IElasticToCsdlMapper {

    /** Default sFchema name space. */
    private static final String DEFAULT_NAMESPACE = "Olastic.OData";

    private final String namespace;

    /**
     * Default constructor.
     */
    public ElasticToCsdlMapper() {
        this(DEFAULT_NAMESPACE);
    }

    /**
     * Constructor to initialize namespace.
     * 
     * @param namespace
     *            namespace
     */
    public ElasticToCsdlMapper(String namespace) {
        this.namespace = namespace;
    }

    @Override
    public String eFieldToCsdlProperty(String index, String type, String field) {
        return field;
    }

    @Override
    public FullQualifiedName eTypeToEntityType(String index, String type) {
        return new FullQualifiedName(eIndexToCsdlNamespace(index), type);
    }

    @Override
    public String eIndexToCsdlNamespace(String index) {
        return namespace + MetaDataUtils.NEMESPACE_SEPARATOR + index;
    }

    @Override
    public String eTypeToEntitySet(String index, String type) {
        return eTypeToEntityType(index, type).getName();
    }

    @Override
    public String eChildRelationToNavPropName(String index, String child, String parent) {
        return eTypeToEntityType(index, child).getName();
    }

    @Override
    public String eParentRelationToNavPropName(String index, String parent, String child) {
        return eTypeToEntityType(index, parent).getName();
    }

    public String getNamespace() {
        return namespace;
    }

}
