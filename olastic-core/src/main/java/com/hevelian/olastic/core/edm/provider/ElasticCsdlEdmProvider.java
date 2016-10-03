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
import org.elasticsearch.common.collect.ImmutableOpenMap;

import com.carrotsearch.hppc.cursors.ObjectCursor;
import com.carrotsearch.hppc.cursors.ObjectObjectCursor;
import com.hevelian.olastic.core.common.ParsedMapWrapper;
import com.hevelian.olastic.core.common.PrimitiveTypeMapper;
import com.hevelian.olastic.core.elastic.ElasticConstants;
import com.hevelian.olastic.core.elastic.mappings.MappingMetaDataProvider;

/**
 * {@link CsdlEdmProvider} implementation that generates the service/metadata
 * documents based on the Elasticsearch mappings.
 * 
 * @author yuflyud
 */
// TODO - review the great amout of custom protected methods. Possibly use some
// delegate and filter out some unnecessary methods.
public abstract class ElasticCsdlEdmProvider extends CsdlAbstractEdmProvider {
    private static final String DEFAULT_CONTAINER_NAME = "ODataService";
    private static final String DEFAULT_NAMESPACE = "OData";
    private final PrimitiveTypeMapper primitiveTypeMapper;
    private final MappingMetaDataProvider mappingMetaDataProvider;

    public ElasticCsdlEdmProvider(Client client) {
        this.primitiveTypeMapper = new PrimitiveTypeMapper();
        this.mappingMetaDataProvider = new MappingMetaDataProvider(client);
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

        // Retrieve type fields from Elasticsearch
        ParsedMapWrapper eTypeProperties;
        try {
            eTypeProperties = new ParsedMapWrapper(mappingMetaDataProvider
                    .getMappingForType(eIndex, entityTypeNameToEType(entityTypeName)).sourceAsMap())
                            .mapValue(ElasticConstants.PROPERTIES_PROPERTY);
            // TODO check type exists
        } catch (IOException e) {
            throw new ODataException("Unable to parse the mapping response from Elastcsearch.", e);
        }

        CsdlEntityType entityType = new CsdlEntityType();
        entityType.setName(entityTypeName.getName());

        // Map Elasticsearch fields to Csdl properties
        for (String eField : eTypeProperties.map.keySet()) {
            String eFieldType = eTypeProperties.mapValue(eField)
                    .stringValue(ElasticConstants.FIELD_DATATYPE_PROPERTY);

            FullQualifiedName type = null;
            // TODO handle nested properties
            type = primitiveTypeMapper.map(eFieldType).getFullQualifiedName();
            CsdlProperty p = new CsdlProperty().setName(eField).setType(type);
            entityType.getProperties().add(p);
        }

        // Add _id property
        CsdlProperty p = new CsdlProperty().setName(ElasticConstants.ID_FIELD_NAME)
                .setType(EdmPrimitiveTypeKind.String.getFullQualifiedName()).setNullable(false);
        entityType.getProperties().add(p);

        // Add navigation properties
        List<CsdlNavigationProperty> navPropList = getNavigationProperties(entityTypeName);
        entityType.setNavigationProperties(navPropList);

        // create PropertyRef for Key element
        CsdlPropertyRef propertyRef = new CsdlPropertyRef();
        propertyRef.setName(ElasticConstants.ID_FIELD_NAME);
        entityType.setKey(Arrays.asList(propertyRef));

        return entityType;

    }

    /**
     * Map entity type name to elastic type. By default returns the same name.
     * 
     * @param entityTypeName
     *            name of the entity type.
     * @return Elastic type name.
     */
    protected String entityTypeNameToEType(FullQualifiedName entityTypeName) {
        return entityTypeName.getName();
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

        ImmutableOpenMap<String, FieldMappingMetaData> eFieldMappings = mappingMetaDataProvider
                .getMappingsForField(namespaceToIndex(entityTypeName.getNamespace()),
                        ElasticConstants.PARENT_PROPERTY);
        String eType = entityTypeNameToEType(entityTypeName);

        for (ObjectObjectCursor<String, FieldMappingMetaData> e : eFieldMappings) {

            ParsedMapWrapper eParent = new ParsedMapWrapper(e.value.sourceAsMap())
                    .mapValue(ElasticConstants.PARENT_PROPERTY);

            if (eParent.map == null) {
                continue;
            }

            String eParentType = eParent.stringValue(ElasticConstants.FIELD_DATATYPE_PROPERTY)
                    .toString();

            // Create Child Relations
            if (eType.equals(eParentType)) {
                CsdlNavigationProperty navProp = new CsdlNavigationProperty()
                        .setName(eChildTypeToNavigationPropertyName(entityTypeName, e.key))
                        .setType(eTypeToEntityType(entityTypeName.getNamespace(), e.key))
                        .setCollection(true)
                        .setPartner(eParentTypeToNavigationPropertyName(
                                new FullQualifiedName(entityTypeName.getNamespace(), e.key),
                                eType));
                navPropList.add(navProp);
            }

            // Create Parent Relation
            if (eType.equals(e.key)) {
                CsdlNavigationProperty navProp = new CsdlNavigationProperty()
                        .setName(eParentTypeToNavigationPropertyName(entityTypeName, eParentType))
                        .setType(eTypeToEntityType(entityTypeName.getNamespace(), eParentType))
                        .setNullable(false)
                        .setPartner(eChildTypeToNavigationPropertyName(
                                new FullQualifiedName(entityTypeName.getNamespace(), eParentType),
                                e.key));
                navPropList.add(navProp);
            }
        }

        return navPropList;
    }

    /**
     * Convert a child relationship of Elasticsearch to a navigation property
     * name.
     * 
     * @param parentEntityType
     *            parent entity type to return a navigation property for.
     * @param eChildType
     *            Elasticsearch child type.
     * @return Navigation property name for child relationship. Default
     *         implementation returns the corresponding child entity type's
     *         name.
     */
    protected String eChildTypeToNavigationPropertyName(FullQualifiedName parentEntityType,
            String eChildType) {
        return eTypeToEntityType(parentEntityType.getNamespace(), eChildType).getName();
    }

    /**
     * Convert a parent relationship of Elasticsearch to a navigation property
     * name.
     * 
     * @param childEntityType
     *            child entity type to return a navigation property for.
     * @param eParentType
     *            Elasticsearch parent type.
     * @return Navigation property name for parent relationship. Default
     *         implementation returns the corresponding parent entity type's
     *         name.
     */
    protected String eParentTypeToNavigationPropertyName(FullQualifiedName childEntityType,
            String eParentType) {
        return eTypeToEntityType(childEntityType.getNamespace(), eParentType).getName();
    }

    public FullQualifiedName eTypeToEntityType(String namespace, String type) {
        return new FullQualifiedName(namespace, type);
    }

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

    @Override
    public CsdlEntitySet getEntitySet(FullQualifiedName entityContainer, String entitySetName)
            throws ODataException {

        FullQualifiedName entityTypeName = entitySetToEntityType(entityContainer, entitySetName);

        if (entityTypeName == null) {
            return null;
        }

        CsdlEntitySet entitySet = new CsdlEntitySet();
        entitySet.setName(entitySetName);
        entitySet.setType(entityTypeName);

        // define navigation property bindings
        List<CsdlNavigationProperty> navigationProperties = getNavigationProperties(entityTypeName);
        List<CsdlNavigationPropertyBinding> navPropBindingList = new ArrayList<CsdlNavigationPropertyBinding>();
        for (CsdlNavigationProperty p : navigationProperties) {
            CsdlNavigationPropertyBinding navPropBinding = new CsdlNavigationPropertyBinding();
            navPropBinding.setTarget(eTypeToEntitySet(p.getTypeFQN().getNamespace(),
                    entityTypeNameToEType(p.getTypeFQN())));
            navPropBinding.setPath(p.getName());
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
        if (getSchemaNamespaces().contains(entityContainer.getNamespace())) {
            entityTypeName = eTypeToEntityType(entityContainer.getNamespace(),
                    entitySetToEType(entityContainer, entitySetName));
        }
        // Check whether root entity container is used
        else if (getNamespace().equals(entityContainer.getNamespace())) {
            CsdlEntitySet entitySet = getEntityContainer().getEntitySet(entitySetName);
            entityTypeName = entitySet == null ? null : entitySet.getTypeFQN();
        } else {
            throw new ODataException("No entity container found for schema.");
        }
        return entityTypeName;
    }

    // TODO do we need this? needs review.
    protected String entitySetToEType(FullQualifiedName entityContainer, String entitySetName) {
        return entityTypeNameToEType(
                new FullQualifiedName(entityContainer.getNamespace(), entitySetName));
    }

    @Override
    public CsdlEntityContainerInfo getEntityContainerInfo(FullQualifiedName entityContainerName) {
        CsdlEntityContainerInfo entityContainerInfo = null;
        FullQualifiedName myEntityContainerName = new FullQualifiedName(getNamespace(),
                getContainerName());
        if (entityContainerName == null/*
                                        * TODO || entityContainerName.equals(
                                        * myEntityContainerName)
                                        */) {
            entityContainerInfo = new CsdlEntityContainerInfo();
            entityContainerInfo.setContainerName(myEntityContainerName);
        }
        return entityContainerInfo;
    }

    @Override
    public List<CsdlSchema> getSchemas() throws ODataException {
        List<CsdlSchema> schemas = new ArrayList<CsdlSchema>();
        for (String namespace : getSchemaNamespaces()) {
            // create Schema
            CsdlSchema schema = new CsdlSchema();
            schema.setNamespace(namespace);

            // add EntityTypes
            List<CsdlEntityType> entityTypes = new ArrayList<CsdlEntityType>();

            for (ObjectCursor<String> key : mappingMetaDataProvider
                    .getAllMappings(namespaceToIndex(namespace)).keys()) {
                entityTypes.add(getEntityType(eTypeToEntityType(namespace, key.value)));
            }

            schema.setEntityTypes(entityTypes);
            schema.setEntityContainer(getEntityContainerForSchema(namespace));
            schemas.add(schema);
        }
        return schemas;
    }

    /**
     * Map Elasticsearch type name to CSDL entity set name. By default returns
     * the name of the corresponding entity type.
     * 
     * @param namespace
     *            csdl namespace
     * @param eType
     *            Elasticsearch type.
     * @return name of the corresponding entity set.
     */
    protected String eTypeToEntitySet(String namespace, String eType) {
        return eTypeToEntityType(namespace, eType).getName();
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

        List<CsdlEntitySet> entitySets = new ArrayList<CsdlEntitySet>();
        for (ObjectCursor<String> key : mappingMetaDataProvider
                .getAllMappings(namespaceToIndex(namespace)).keys()) {
            entitySets.add(getEntitySet(new FullQualifiedName(namespace, getContainerName()),
                    eTypeToEntitySet(namespace, key.value)));
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

    // TODO review the following methods, do we need them?
    // Getters/Setters
    /**
     * Get the namespace of the root entity container.
     */
    public String getNamespace() {
        return DEFAULT_NAMESPACE;
    }

    /**
     * Get the name of the root entity container.
     */
    public String getContainerName() {
        return DEFAULT_CONTAINER_NAME;
    }
}
