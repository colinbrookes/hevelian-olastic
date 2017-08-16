package com.hevelian.olastic.core.elastic.mappings;

import org.apache.olingo.commons.api.edm.FullQualifiedName;

import com.hevelian.olastic.core.utils.MetaDataUtils;

/**
 * Default implementation of {@link ElasticToCsdlMapper} interface.
 * 
 * @author rdidyk
 */
public class DefaultElasticToCsdlMapper implements ElasticToCsdlMapper {

    /** Default schema name space. */
    public static final String DEFAULT_NAMESPACE = "Olastic.OData";

    private final String namespace;

    /**
     * Default constructor.
     */
    public DefaultElasticToCsdlMapper() {
        this(DEFAULT_NAMESPACE);
    }

    /**
     * Constructor to initialize namespace.
     * 
     * @param namespace
     *            namespace
     */
    public DefaultElasticToCsdlMapper(String namespace) {
        this.namespace = namespace;
    }

    @Override
    public String esFieldToCsdlProperty(String index, String type, String field) {
        return field;
    }

    @Override
    public boolean esFieldIsCollection(String index, String type, String field) {
        return false;
    }

    @Override
    public FullQualifiedName esTypeToEntityType(String index, String type) {
        return new FullQualifiedName(esIndexToCsdlNamespace(index), type);
    }

    @Override
    public String esIndexToCsdlNamespace(String index) {
        return namespace + MetaDataUtils.NAMESPACE_SEPARATOR + index;
    }

    @Override
    public String esTypeToEntitySet(String index, String type) {
        return esTypeToEntityType(index, type).getName();
    }

    @Override
    public String esChildRelationToNavPropName(String index, String child, String parent) {
        return esTypeToEntityType(index, child).getName();
    }

    @Override
    public String esParentRelationToNavPropName(String index, String parent, String child) {
        return esTypeToEntityType(index, parent).getName();
    }

    public String getNamespace() {
        return namespace;
    }

}
