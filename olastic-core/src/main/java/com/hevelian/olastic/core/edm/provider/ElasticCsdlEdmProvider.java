package com.hevelian.olastic.core.edm.provider;

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
import org.apache.olingo.commons.api.edm.provider.CsdlEntityType;
import org.apache.olingo.commons.api.edm.provider.CsdlNavigationProperty;
import org.apache.olingo.commons.api.edm.provider.CsdlNavigationPropertyBinding;
import org.apache.olingo.commons.api.edm.provider.CsdlProperty;
import org.apache.olingo.commons.api.edm.provider.CsdlPropertyRef;
import org.apache.olingo.commons.api.edm.provider.CsdlSchema;
import org.apache.olingo.commons.api.ex.ODataException;
import org.elasticsearch.action.admin.indices.mapping.get.GetFieldMappingsResponse.FieldMappingMetaData;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.collect.ImmutableOpenMap;

import com.carrotsearch.hppc.cursors.ObjectCursor;
import com.carrotsearch.hppc.cursors.ObjectObjectCursor;
import com.hevelian.olastic.core.common.ParsedMapWrapper;
import com.hevelian.olastic.core.common.PrimitiveTypeMapper;
import com.hevelian.olastic.core.elastic.ElasticConstants;
import com.hevelian.olastic.core.elastic.mappings.ElasticToCsdlMapper;
import com.hevelian.olastic.core.elastic.mappings.IElasticToCsdlMapper;
import com.hevelian.olastic.core.elastic.mappings.MappingMetaDataProvider;

/**
 * {@link CsdlEdmProvider} implementation that generates the service/metadata
 * documents based on the Elasticsearch mappings.
 * 
 * @author yuflyud
 * @contributor rdidyk
 */
// TODO - review the great amout of custom protected methods. Possibly use some
// delegate and filter out some unnecessary methods.
public abstract class ElasticCsdlEdmProvider extends CsdlAbstractEdmProvider {

    private static final String DEFAULT_CONTAINER_NAME = "ODataService";
    private static final String DEFAULT_CONTAINER_NAMESPACE = "OData";

    private final PrimitiveTypeMapper primitiveTypeMapper;
    private final MappingMetaDataProvider mappingMetaDataProvider;
    protected final IElasticToCsdlMapper csdlMapper;

    private String containerNamespace;
    private String containerName;

    /**
     * Initializes ES client with default {@link IElasticToCsdlMapper}
     * implementation.
     * 
     * @param client
     *            ES client
     */
    public ElasticCsdlEdmProvider(Client client) {
        this(client, new ElasticToCsdlMapper());
    }

    /**
     * Initializes ES client with custom {@link IElasticToCsdlMapper}
     * implementation.
     * 
     * @param client
     *            ES client
     * @param esToCsdlMapper
     *            ES to CSDL mapper
     */
    public ElasticCsdlEdmProvider(Client client, IElasticToCsdlMapper esToCsdlMapper) {
        this.primitiveTypeMapper = new PrimitiveTypeMapper();
        this.mappingMetaDataProvider = new MappingMetaDataProvider(client);
        this.csdlMapper = esToCsdlMapper;
        setContainerName(DEFAULT_CONTAINER_NAME);
        setContainerNamespace(DEFAULT_CONTAINER_NAMESPACE);
    }

    /**
     * Get entity type definition by fully qualified name. This method calls the
     * {@link #getMappingForType(FullQualifiedName)} method to retrieve the
     * corresponding Elastic type mappings. <br>
     * The _id field is added and used as the OData key property.
     */
    @Override
    public CsdlEntityType getEntityType(FullQualifiedName entityTypeName) throws ODataException {
        String eIndex = namespaceToIndex(entityTypeName.getNamespace());
        // If there is no index mapping for provided namespace - return null, no
        // entity type is found.
        if (eIndex == null) {
            return null;
        }
        CsdlEntityType entityType = new CsdlEntityType();
        entityType.setName(entityTypeName.getName());
        // Retrieve type fields from Elasticsearch
        // TODO check type exists
        entityType.setProperties(getProperties(entityTypeName,
                mappingMetaDataProvider.getMappingForType(eIndex, entityTypeName.getName())));

        // Add _id property
        CsdlProperty idProperty = new CsdlProperty().setName(ElasticConstants.ID_FIELD_NAME)
                .setType(EdmPrimitiveTypeKind.String.getFullQualifiedName()).setNullable(false);
        entityType.getProperties().add(idProperty);

        // Add navigation properties
        entityType.setNavigationProperties(getNavigationProperties(entityTypeName));

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
     *            name of the entity type.
     * @return list of properties
     */
    protected List<CsdlProperty> getProperties(FullQualifiedName entityTypeName,
            MappingMetaData metaData) throws ODataException {
        try {
            ParsedMapWrapper eTypeProperties = new ParsedMapWrapper(metaData.sourceAsMap())
                    .mapValue(ElasticConstants.PROPERTIES_PROPERTY);
            List<CsdlProperty> properties = new ArrayList<>();
            for (String eFieldName : eTypeProperties.map.keySet()) {
                String name = csdlMapper.eFieldToCsdlProperty(
                        namespaceToIndex(entityTypeName.getNamespace()), entityTypeName.getName(),
                        eFieldName);

                String eFieldType = eTypeProperties.mapValue(eFieldName)
                        .stringValue(ElasticConstants.FIELD_DATATYPE_PROPERTY);
                // TODO handle nested properties
                FullQualifiedName type = primitiveTypeMapper.map(eFieldType).getFullQualifiedName();
                properties.add(new CsdlProperty().setName(name).setType(type));
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
    protected List<CsdlNavigationProperty> getNavigationProperties(
            FullQualifiedName entityTypeName) {
        List<CsdlNavigationProperty> navPropList = new ArrayList<>();

        String namespace = entityTypeName.getNamespace();
        String index = namespaceToIndex(namespace);
        ImmutableOpenMap<String, FieldMappingMetaData> eFieldMappings = mappingMetaDataProvider
                .getMappingsForField(namespaceToIndex(namespace), ElasticConstants.PARENT_PROPERTY);
        String eType = entityTypeName.getName();

        for (ObjectObjectCursor<String, FieldMappingMetaData> e : eFieldMappings) {
            ParsedMapWrapper eParent = new ParsedMapWrapper(e.value.sourceAsMap())
                    .mapValue(ElasticConstants.PARENT_PROPERTY);
            if (eParent.map == null) {
                continue;
            }
            String eParentType = eParent.stringValue(ElasticConstants.FIELD_DATATYPE_PROPERTY);
            // Create Child Relations
            if (eType.equals(eParentType)) {
                CsdlNavigationProperty navProp = new CsdlNavigationProperty()
                        .setName(csdlMapper.eChildRelationToNavPropName(index, e.key,
                                entityTypeName.getName()))
                        .setType(csdlMapper.eTypeToEntityType(index, e.key)).setCollection(true)
                        .setPartner(csdlMapper.eParentRelationToNavPropName(index, eType,
                                new FullQualifiedName(namespace, e.key).getName()));
                navPropList.add(navProp);
            }
            // Create Parent Relation
            if (eType.equals(e.key)) {
                CsdlNavigationProperty navProp = new CsdlNavigationProperty()
                        .setName(csdlMapper.eParentRelationToNavPropName(index, eParentType,
                                entityTypeName.getName()))
                        .setType(csdlMapper.eTypeToEntityType(index, eParentType))
                        .setNullable(false).setPartner(csdlMapper.eChildRelationToNavPropName(index,
                                e.key, new FullQualifiedName(namespace, eParentType).getName()));
                navPropList.add(navProp);
            }
        }
        return navPropList;
    }

    @Override
    public CsdlEntitySet getEntitySet(FullQualifiedName entityContainer, String entitySetName)
            throws ODataException {
        FullQualifiedName entityTypeName = entitySetToEntityType(entityContainer, entitySetName);
        if (entityTypeName == null) {
            return null;
        }
        CsdlEntitySet entitySet = new CsdlEntitySet();
        entitySet.setName(csdlMapper
                .eTypeToEntitySet(namespaceToIndex(entityContainer.getNamespace()), entitySetName));
        entitySet.setType(entityTypeName);

        // define navigation property bindings
        List<CsdlNavigationProperty> navigationProperties = getNavigationProperties(entityTypeName);
        List<CsdlNavigationPropertyBinding> navPropBindingList = new ArrayList<>();
        for (CsdlNavigationProperty property : navigationProperties) {
            CsdlNavigationPropertyBinding navPropBinding = new CsdlNavigationPropertyBinding();
            navPropBinding.setTarget(csdlMapper.eTypeToEntitySet(
                    namespaceToIndex(property.getTypeFQN().getNamespace()),
                    property.getTypeFQN().getName()));
            navPropBinding.setPath(property.getName());
            navPropBindingList.add(navPropBinding);
        }
        entitySet.setNavigationPropertyBindings(navPropBindingList);
        return entitySet;
    }

    /**
     * Get a type of the EntitySet.
     * 
     * @param entityContainer
     *            name of the Entity Container.
     * @param entitySetName
     *            name of the Entity Set.
     * @return Entity type of the Entity Set.
     * @throws ODataException
     *             in case no entity container was found.
     */
    protected FullQualifiedName entitySetToEntityType(FullQualifiedName entityContainer,
            String entitySetName) throws ODataException {
        FullQualifiedName entityTypeName;
        // Check whether schema entity container is used
        String namespace = entityContainer.getNamespace();
        if (getSchemaNamespaces().contains(namespace)) {
            entityTypeName = csdlMapper.eTypeToEntityType(namespaceToIndex(namespace),
                    new FullQualifiedName(namespace, entitySetName).getName());
        } else {
            throw new ODataException("No entity container found for schema.");
        }
        return entityTypeName;
    }

    @Override
    public CsdlEntityContainerInfo getEntityContainerInfo(FullQualifiedName entityContainerName) {
        CsdlEntityContainerInfo entityContainerInfo = null;
        FullQualifiedName myEntityContainerName = new FullQualifiedName(getContainerNamespace(),
                getContainerName());
        if (entityContainerName == null) {
            entityContainerInfo = new CsdlEntityContainerInfo();
            entityContainerInfo.setContainerName(myEntityContainerName);
        }
        return entityContainerInfo;
    }

    @Override
    public List<CsdlSchema> getSchemas() throws ODataException {
        List<CsdlSchema> schemas = new ArrayList<>();
        for (String namespace : getSchemaNamespaces()) {
            // create Schema
            CsdlSchema schema = new CsdlSchema();
            schema.setNamespace(namespace);

            // add EntityTypes
            String index = namespaceToIndex(namespace);
            List<CsdlEntityType> entityTypes = new ArrayList<>();
            for (ObjectCursor<String> key : mappingMetaDataProvider.getAllMappings(index).keys()) {
                entityTypes.add(getEntityType(csdlMapper.eTypeToEntityType(index, key.value)));
            }

            schema.setEntityTypes(entityTypes);
            schema.setEntityContainer(getEntityContainerForSchema(namespace));
            schemas.add(schema);
        }
        return schemas;
    }

    /**
     * Get a specific entity container for a schema.
     * 
     * @param namespace
     *            csdl schema namespace.
     * @return Entity Container.
     */
    protected CsdlEntityContainer getEntityContainerForSchema(String namespace)
            throws ODataException {
        CsdlEntityContainer entityContainer = new CsdlEntityContainer();
        entityContainer.setName(getContainerName());

        List<CsdlEntitySet> entitySets = new ArrayList<>();
        for (ObjectCursor<String> key : mappingMetaDataProvider
                .getAllMappings(namespaceToIndex(namespace)).keys()) {
            entitySets.add(
                    getEntitySet(new FullQualifiedName(namespace, getContainerName()), key.value));
        }
        entityContainer.setEntitySets(entitySets);
        return entityContainer;
    }

    @Override
    public CsdlEntityContainer getEntityContainer() throws ODataException {
        // create EntityContainer
        CsdlEntityContainer entityContainer = new CsdlEntityContainer();
        entityContainer.setName(getContainerName());

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

    public IElasticToCsdlMapper getCsdlMapper() {
        return csdlMapper;
    }

    /**
     * Get the namespace of the root entity container.
     */
    public String getContainerNamespace() {
        return containerNamespace;
    }

    /**
     * Get the name of the root entity container.
     */
    public String getContainerName() {
        return containerName;
    }

    public void setContainerName(String containerName) {
        this.containerName = containerName;
    }

    public void setContainerNamespace(String containerNamespace) {
        this.containerNamespace = containerNamespace;
    }

}
