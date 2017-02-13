package com.hevelian.olastic.core.api.edm.provider;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind;
import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.apache.olingo.commons.api.edm.provider.CsdlAbstractEdmProvider;
import org.apache.olingo.commons.api.edm.provider.CsdlEdmProvider;
import org.apache.olingo.commons.api.edm.provider.CsdlEntityContainer;
import org.apache.olingo.commons.api.edm.provider.CsdlEntityContainerInfo;
import org.apache.olingo.commons.api.edm.provider.CsdlEntitySet;
import org.apache.olingo.commons.api.edm.provider.CsdlNavigationPropertyBinding;
import org.apache.olingo.commons.api.edm.provider.CsdlProperty;
import org.apache.olingo.commons.api.edm.provider.CsdlPropertyRef;
import org.apache.olingo.commons.api.edm.provider.CsdlSchema;
import org.apache.olingo.commons.api.ex.ODataException;
import org.elasticsearch.action.admin.indices.mapping.get.GetFieldMappingsResponse.FieldMappingMetaData;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.index.mapper.ObjectMapper;

import com.carrotsearch.hppc.cursors.ObjectCursor;
import com.carrotsearch.hppc.cursors.ObjectObjectCursor;
import com.hevelian.olastic.core.common.NestedPerIndexMapper;
import com.hevelian.olastic.core.common.NestedTypeMapper;
import com.hevelian.olastic.core.common.ParsedMapWrapper;
import com.hevelian.olastic.core.common.PrimitiveTypeMapper;
import com.hevelian.olastic.core.elastic.ElasticConstants;
import com.hevelian.olastic.core.elastic.mappings.DefaultElasticToCsdlMapper;
import com.hevelian.olastic.core.elastic.mappings.ElasticToCsdlMapper;
import com.hevelian.olastic.core.elastic.mappings.MappingMetaDataProvider;

/**
 * {@link CsdlEdmProvider} implementation that generates the service/metadata
 * documents based on the Elasticsearch mappings.
 *
 * @author yuflyud
 * @contributor rdidyk
 */
public abstract class ElasticCsdlEdmProvider extends CsdlAbstractEdmProvider {

    private static final FullQualifiedName DEFAULT_CONTAINER_NAME = new FullQualifiedName("OData",
            "ODataService");

    private final PrimitiveTypeMapper primitiveTypeMapper;
    private final NestedTypeMapper nestedTypeMapper;
    private final MappingMetaDataProvider mappingMetaDataProvider;
    protected final ElasticToCsdlMapper csdlMapper;

    private FullQualifiedName containerName;

    /**
     * Initializes mapping metadata provider with default
     * {@link ElasticToCsdlMapper} implementation.
     *
     * @param metaDataProvider
     *            mapping meta data provider
     */
    public ElasticCsdlEdmProvider(MappingMetaDataProvider metaDataProvider) {
        this(metaDataProvider, new DefaultElasticToCsdlMapper());
    }

    /**
     * Initializes mapping metadata provider with custom
     * {@link ElasticToCsdlMapper} implementation.
     *
     * @param metaDataProvider
     *            mapping meta data provider
     * @param csdlMapper
     *            ES to CSDL mapper
     */
    public ElasticCsdlEdmProvider(MappingMetaDataProvider metaDataProvider,
            ElasticToCsdlMapper csdlMapper) {
        this(metaDataProvider, csdlMapper, new NestedPerIndexMapper(metaDataProvider, csdlMapper));
    }

    /**
     * Initializes mapping metadata provider with custom
     * {@link NestedTypeMapper} implementation.
     *
     * @param metaDataProvider
     *            mapping meta data provider
     * @param nestedTypeMapper
     *            nested type mapper
     */
    public ElasticCsdlEdmProvider(MappingMetaDataProvider metaDataProvider,
            NestedTypeMapper nestedTypeMapper) {
        this(metaDataProvider, new DefaultElasticToCsdlMapper(), nestedTypeMapper);
    }

    /**
     * Initializes mapping metadata provider with custom
     * {@link ElasticToCsdlMapper} and {@link NestedTypeMapper} implementation.
     *
     * @param metaDataProvider
     *            mapping meta data provider
     * @param csdlMapper
     *            ES to CSDL mapper
     * @param nestedTypeMapper
     *            nested type mapper
     */
    public ElasticCsdlEdmProvider(MappingMetaDataProvider metaDataProvider,
            ElasticToCsdlMapper csdlMapper, NestedTypeMapper nestedTypeMapper) {
        this.mappingMetaDataProvider = metaDataProvider;
        this.csdlMapper = csdlMapper;
        this.primitiveTypeMapper = new PrimitiveTypeMapper();
        this.nestedTypeMapper = nestedTypeMapper;
        setContainerName(DEFAULT_CONTAINER_NAME);
    }

    @Override
    public ElasticCsdlEntityType getEntityType(FullQualifiedName entityTypeName)
            throws ODataException {
        String eIndex = namespaceToIndex(entityTypeName.getNamespace());
        // If there is no index mapping for provided namespace - return null, no
        // entity type is found.
        if (eIndex != null) {
            List<ElasticCsdlEntityType> entityTypes = getEntityTypes(eIndex);
            for (ElasticCsdlEntityType entityType : entityTypes) {
                if (entityType.getEType().equals(entityTypeName.getName())) {
                    return entityType;
                }
            }
        }
        return null;
    }

    /**
     * Creates entity type definition for type from index. This method calls the
     * mappingMetaDataProvider method to retrieve the corresponding Elastic type
     * mappings. <br>
     * If no 'id' property found in mappings then the 'id' is added and used as
     * the OData key property.
     */
    public ElasticCsdlEntityType createEntityType(String index, String type) throws ODataException {
        MappingMetaData typeMappings = mappingMetaDataProvider.getMappingForType(index, type);
        if (typeMappings == null) {
            throw new ODataException(String.format("No mappings found for type '%s'", type));
        }
        ElasticCsdlEntityType entityType = new ElasticCsdlEntityType();
        entityType.setEIndex(index);
        entityType.setEType(type);
        FullQualifiedName entityTypeName = csdlMapper.eTypeToEntityType(index, type);
        entityType.setName(entityTypeName.getName());
        // Retrieve type fields from Elasticsearch
        entityType.setProperties(getProperties(index, type, typeMappings));

        if (entityType.getProperty(ElasticConstants.ID_FIELD_NAME) == null) {
            // Add id property if there is no 'id' in Elasticsearch mappings.
            CsdlProperty idProperty = new ElasticCsdlProperty()
                    .setName(ElasticConstants.ID_FIELD_NAME)
                    .setType(EdmPrimitiveTypeKind.String.getFullQualifiedName()).setNullable(false);
            entityType.getProperties().add(idProperty);
        }

        // Add navigation properties
        entityType.getNavigationProperties().addAll(getNavigationProperties(index, type));

        // create PropertyRef for Key element
        CsdlPropertyRef propertyRef = new CsdlPropertyRef();
        propertyRef.setName(ElasticConstants.ID_FIELD_NAME);
        entityType.setKey(Arrays.asList(propertyRef));
        return entityType;
    }

    /**
     * Retrieve properties for the entity type.
     *
     * @param entityTypeName
     *            name of the entity type
     * @return list of properties
     */
    protected List<CsdlProperty> getProperties(String index, String type, MappingMetaData metaData)
            throws ODataException {
        try {
            ParsedMapWrapper eTypeProperties = new ParsedMapWrapper(metaData.sourceAsMap())
                    .mapValue(ElasticConstants.PROPERTIES_PROPERTY);
            List<CsdlProperty> properties = new ArrayList<>();
            for (String eFieldName : eTypeProperties.map.keySet()) {
                String name = csdlMapper.eFieldToCsdlProperty(index, type, eFieldName);
                ParsedMapWrapper fieldMap = eTypeProperties.mapValue(eFieldName);
                String eFieldType = fieldMap.stringValue(ElasticConstants.FIELD_DATATYPE_PROPERTY);
                FullQualifiedName typeFQN;
                if (ObjectMapper.NESTED_CONTENT_TYPE.equals(eFieldType)) {
                    typeFQN = getNestedTypeMapper().getComplexType(index, type, name);
                } else {
                    typeFQN = primitiveTypeMapper.map(eFieldType).getFullQualifiedName();
                }
                properties.add(new ElasticCsdlProperty().setEIndex(index).setEType(type)
                        .setEField(eFieldName).setName(name).setType(typeFQN)
                        .setCollection(csdlMapper.eFieldIsCollection(index, type, eFieldName)));
            }
            return properties;
        } catch (IOException e) {
            throw new ODataException("Unable to parse the mapping response from Elastcsearch.", e);
        }
    }

    /**
     * Retrieve navigation properties for the entity type.
     *
     * @param entityTypeName
     *            name of the entity type.
     * @return list of navigation properties.
     */
    protected List<ElasticCsdlNavigationProperty> getNavigationProperties(String index,
            String type) {
        List<ElasticCsdlNavigationProperty> navigationProperties = new ArrayList<>();
        ImmutableOpenMap<String, FieldMappingMetaData> eFieldMappings = mappingMetaDataProvider
                .getMappingsForField(index, ElasticConstants.PARENT_PROPERTY);
        for (ObjectObjectCursor<String, FieldMappingMetaData> e : eFieldMappings) {
            ParsedMapWrapper eParent = new ParsedMapWrapper(e.value.sourceAsMap())
                    .mapValue(ElasticConstants.PARENT_PROPERTY);
            if (eParent.map == null) {
                continue;
            }
            String eParentType = eParent.stringValue(ElasticConstants.FIELD_DATATYPE_PROPERTY);
            // Create Child Relations
            if (type.equals(eParentType)) {
                ElasticCsdlNavigationProperty navProp = new ElasticCsdlNavigationProperty()
                        .setEIndex(index).setEType(e.key);
                navProp.setName(csdlMapper.eChildRelationToNavPropName(index, e.key, type))
                        .setType(csdlMapper.eTypeToEntityType(index, e.key)).setCollection(true)
                        .setPartner(csdlMapper.eParentRelationToNavPropName(index, type, e.key));
                navigationProperties.add(navProp);
            }
            // Create Parent Relation
            if (type.equals(e.key)) {
                ElasticCsdlNavigationProperty navProp = new ElasticCsdlNavigationProperty()
                        .setEIndex(index).setEType(eParentType);
                navProp.setName(csdlMapper.eParentRelationToNavPropName(index, eParentType, type))
                        .setType(csdlMapper.eTypeToEntityType(index, eParentType))
                        .setNullable(false).setPartner(
                                csdlMapper.eChildRelationToNavPropName(index, e.key, eParentType));
                navigationProperties.add(navProp);
            }
        }
        return navigationProperties;
    }

    @Override
    public ElasticCsdlEntitySet getEntitySet(FullQualifiedName entityContainer,
            String entitySetName) throws ODataException {
        // Check whether root entity container is used
        if (getContainerName().getNamespace().equals(entityContainer.getNamespace())) {
            return (ElasticCsdlEntitySet) getEntityContainer().getEntitySet(entitySetName);
        } else {
            throw new ODataException("No entity container found for schema.");
        }
    }

    @Override
    public CsdlEntityContainerInfo getEntityContainerInfo(FullQualifiedName entityContainerName) {
        CsdlEntityContainerInfo entityContainerInfo = null;
        if (entityContainerName == null) {
            entityContainerInfo = new CsdlEntityContainerInfo();
            entityContainerInfo.setContainerName(getContainerName());
        }
        return entityContainerInfo;
    }

    @Override
    public List<CsdlSchema> getSchemas() throws ODataException {
        List<CsdlSchema> schemas = new ArrayList<>();
        for (String namespace : getSchemaNamespaces()) {
            schemas.add(createSchema(namespace));
        }
        return schemas;
    }

    /**
     * Create's schema for namespace.
     * 
     * @param namespace
     *            namespace
     * @return created schema
     * @throws ODataException
     *             if any error occurred
     */
    protected CsdlSchema createSchema(String namespace) throws ODataException {
        // create Schema
        CsdlSchema schema = new CsdlSchema();
        schema.setNamespace(namespace);

        // add Entity Types
        String index = namespaceToIndex(namespace);
        schema.getEntityTypes().addAll(getEntityTypes(index));
        // add Complex Types
        schema.getComplexTypes().addAll(getNestedTypeMapper().getComplexTypes(index));
        schema.setEntityContainer(getEntityContainerForSchema(index));
        return schema;
    }

    /**
     * Get a list of Entity Types for specific Elasticsearch index.
     *
     * @param index
     *            index name
     * @return list of Entity Types
     * @throws ODataException
     *             if any error occurred
     */
    protected List<ElasticCsdlEntityType> getEntityTypes(String index) throws ODataException {
        List<ElasticCsdlEntityType> entityTypes = new ArrayList<>();
        for (ObjectCursor<String> key : mappingMetaDataProvider.getAllMappings(index).keys()) {
            entityTypes.add(createEntityType(index, key.value));
        }
        return entityTypes;
    }

    /**
     * Get a specific entity container for a schema.
     *
     * @param index
     *            schema index name
     * @return Entity Container
     */
    protected CsdlEntityContainer getEntityContainerForSchema(String index) throws ODataException {
        CsdlEntityContainer entityContainer = new CsdlEntityContainer();
        entityContainer.setName(getContainerName().getName());
        entityContainer.getEntitySets().addAll(getEntitySets(index));
        return entityContainer;
    }

    /**
     * Get a list of Entity Sets for specific Elasticsearch index.
     *
     * @param index
     *            index name
     * @return list of Entity Sets
     */
    protected List<ElasticCsdlEntitySet> getEntitySets(String index) {
        List<ElasticCsdlEntitySet> entitySets = new ArrayList<>();
        for (ObjectCursor<String> key : mappingMetaDataProvider.getAllMappings(index).keys()) {
            entitySets.add(createEntitySet(index, key.value));
        }
        return entitySets;
    }

    /**
     * Create's entity set for particular index and type.
     * 
     * @param index
     *            index name
     * @param type
     *            type name
     * @return entity set instance
     */
    protected ElasticCsdlEntitySet createEntitySet(String index, String type) {
        ElasticCsdlEntitySet entitySet = new ElasticCsdlEntitySet();
        entitySet.setEIndex(index);
        entitySet.setEType(type);
        entitySet.setName(csdlMapper.eTypeToEntitySet(index, type));
        FullQualifiedName entityType = csdlMapper.eTypeToEntityType(index, type);
        entitySet.setType(entityType);

        // define navigation property bindings
        List<CsdlNavigationPropertyBinding> navigationBindings = new ArrayList<>();
        for (ElasticCsdlNavigationProperty property : getNavigationProperties(index, type)) {
            CsdlNavigationPropertyBinding navPropBinding = new CsdlNavigationPropertyBinding();
            navPropBinding.setTarget(csdlMapper.eTypeToEntitySet(
                    namespaceToIndex(property.getTypeFQN().getNamespace()), property.getEType()));
            navPropBinding.setPath(property.getName());
            navigationBindings.add(navPropBinding);
        }
        entitySet.setNavigationPropertyBindings(navigationBindings);
        return entitySet;
    }

    @Override
    public CsdlEntityContainer getEntityContainer() throws ODataException {
        // create EntityContainer
        CsdlEntityContainer entityContainer = new CsdlEntityContainer();
        entityContainer.setName(getContainerName().getName());

        List<CsdlSchema> schemas = getSchemas();
        for (CsdlSchema schema : schemas) {
            for (CsdlEntitySet entitySet : schema.getEntityContainer().getEntitySets()) {
                if (entitySet.isIncludeInServiceDocument()) {
                    entityContainer.getEntitySets().add(entitySet);
                }
            }
        }
        return entityContainer;
    }

    @Override
    public ElasticCsdlComplexType getComplexType(FullQualifiedName complexTypeName)
            throws ODataException {
        for (CsdlSchema schema : getSchemas()) {
            if (schema.getNamespace().equals(complexTypeName.getNamespace())) {
                return (ElasticCsdlComplexType) schema.getComplexType(complexTypeName.getName());
            }
        }
        return null;
    }

    /**
     * Return's list of Schema name spaces.
     *
     * @return list of name spaces
     */
    protected abstract List<String> getSchemaNamespaces();

    /**
     * Map CSDL namespace to Elasticsearch index.
     *
     * @param namespace
     *            CSDL namespace.
     * @return index that corresponds to the namespace or null if there is no
     *         mapping for this namespace.
     */
    protected abstract String namespaceToIndex(String namespace);

    public ElasticToCsdlMapper getCsdlMapper() {
        return csdlMapper;
    }

    public NestedTypeMapper getNestedTypeMapper() {
        return nestedTypeMapper;
    }

    public MappingMetaDataProvider getMappingMetaDataProvider() {
        return mappingMetaDataProvider;
    }

    public FullQualifiedName getContainerName() {
        return containerName;
    }

    public void setContainerName(FullQualifiedName containerName) {
        this.containerName = containerName;
    }
}
