package com.hevelian.olastic.core.api.edm.provider;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.apache.olingo.commons.api.edm.provider.CsdlComplexType;
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
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.collect.ImmutableOpenMap.Builder;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.junit.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;

import com.hevelian.olastic.core.common.NestedTypeMapper;
import com.hevelian.olastic.core.elastic.ElasticConstants;
import com.hevelian.olastic.core.elastic.mappings.DefaultElasticToCsdlMapper;
import com.hevelian.olastic.core.elastic.mappings.ElasticToCsdlMapper;
import com.hevelian.olastic.core.elastic.mappings.MappingMetaDataProvider;
import com.hevelian.olastic.core.utils.MetaDataUtils;

/**
 * JUnit tests for {@link MultyElasticIndexCsdlEdmProvider} class.
 *
 * @author rdidyk
 */
@RunWith(MockitoJUnitRunner.class)
public class MultyElasticIndexCsdlEdmProviderTest {

    private static final String AUTHOR_TYPE = "author";
    private static final String AUTHORS_INDEX = "authors";
    private static final String WRITERS_INDEX = "writers";
    private static final String BOOK_TYPE = "book";
    private static final FullQualifiedName AUTHORS_FQN = new FullQualifiedName(
            addNamespace(AUTHORS_INDEX));
    private static final FullQualifiedName AUTHOR_FQN = new FullQualifiedName(
            addNamespace(AUTHORS_INDEX), AUTHOR_TYPE);
    private static final FullQualifiedName WRITERS_FQN = new FullQualifiedName(
            addNamespace(WRITERS_INDEX));
    private static final FullQualifiedName BOOK_FQN = new FullQualifiedName(
            addNamespace(AUTHORS_INDEX), BOOK_TYPE);
    private static final String AUTHORS_FQN_STRING = AUTHORS_FQN.getFullQualifiedNameAsString();
    private static final String WRITERS_FQN_STRING = WRITERS_FQN.getFullQualifiedNameAsString();

    private static Set<String> indices;
    @Mock
    private MappingMetaDataProvider metaDataProvider;
    @Mock
    private NestedTypeMapper nestedTypeMapper;

    @BeforeClass
    public static void setUpBeforeClass() {
        indices = new HashSet<String>();
        indices.add(AUTHORS_INDEX);
        indices.add(WRITERS_INDEX);
    }

    private static String addNamespace(String... path) {
        StringBuffer result = new StringBuffer(DefaultElasticToCsdlMapper.DEFAULT_NAMESPACE);
        for (int i = 0; i < path.length; i++) {
            if (i == 0 || i != path.length - 1) {
                result.append(MetaDataUtils.NAMESPACE_SEPARATOR);
            }
            result.append(path[i]);
        }
        return result.toString();
    }

    @Before
    public void setUp() {
        when(nestedTypeMapper.getComplexType(anyString(), anyString(), anyString()))
                .thenAnswer(new Answer<FullQualifiedName>() {
                    @Override
                    public FullQualifiedName answer(InvocationOnMock invocation) throws Throwable {
                        Object[] args = invocation.getArguments();
                        return new FullQualifiedName((String) args[1], (String) args[2]);
                    }
                });
    }

    @Test
    public void constructor_MappingMetadataProvider_Setted() {
        MultyElasticIndexCsdlEdmProvider edmProvider = new MultyElasticIndexCsdlEdmProvider(
                metaDataProvider, indices);
        assertEquals(metaDataProvider, edmProvider.getMappingMetaDataProvider());
        assertNotNull(edmProvider.getCsdlMapper());
        assertNotNull(edmProvider.getNestedTypeMapper());
    }

    @Test
    public void constructor_MappingMetadataProviderAndCsdlMapper_Setted() {
        ElasticToCsdlMapper csdlMapper = mock(ElasticToCsdlMapper.class);
        MultyElasticIndexCsdlEdmProvider edmProvider = new MultyElasticIndexCsdlEdmProvider(
                metaDataProvider, indices, csdlMapper);
        assertEquals(metaDataProvider, edmProvider.getMappingMetaDataProvider());
        assertEquals(csdlMapper, edmProvider.getCsdlMapper());
        assertNotNull(edmProvider.getNestedTypeMapper());
    }

    @Test
    public void constructor_MappingMetadataProviderAndCsdlMapperAndNestedMappingStrategy_Setted() {
        ElasticToCsdlMapper csdlMapper = mock(ElasticToCsdlMapper.class);
        MultyElasticIndexCsdlEdmProvider edmProvider = new MultyElasticIndexCsdlEdmProvider(
                metaDataProvider, indices, csdlMapper, nestedTypeMapper);
        assertEquals(metaDataProvider, edmProvider.getMappingMetaDataProvider());
        assertEquals(csdlMapper, edmProvider.getCsdlMapper());
        assertNotNull(edmProvider.getNestedTypeMapper());
        assertEquals(nestedTypeMapper, edmProvider.getNestedTypeMapper());
    }

    @Test
    public void getSchemaNamespaces_SetOfIndices_ShemaNamespacesRetrieved() throws ODataException {
        MultyElasticIndexCsdlEdmProvider edmProvider = new MultyElasticIndexCsdlEdmProvider(
                metaDataProvider, indices);
        List<String> schemaNamespaces = edmProvider.getSchemaNamespaces();
        assertEquals(2, schemaNamespaces.size());
        assertTrue(schemaNamespaces.contains(AUTHORS_FQN_STRING));
        assertTrue(schemaNamespaces.contains(WRITERS_FQN_STRING));
    }

    @Test
    public void getSchemaNamespaces_EmptyIndices_EmptyShemaNamespacesRetrieved()
            throws ODataException {
        MultyElasticIndexCsdlEdmProvider edmProvider = new MultyElasticIndexCsdlEdmProvider(
                metaDataProvider, new HashSet<String>());
        List<String> schemaNamespaces = edmProvider.getSchemaNamespaces();
        assertTrue(schemaNamespaces.isEmpty());
    }

    @Test
    public void namespaceToIndex_DifferentNamespaces_ExpectedValuesRetrieved()
            throws ODataException {
        MultyElasticIndexCsdlEdmProvider edmProvider = new MultyElasticIndexCsdlEdmProvider(
                metaDataProvider, indices);
        assertEquals(AUTHORS_INDEX, edmProvider.namespaceToIndex(AUTHORS_FQN_STRING));
        assertEquals(WRITERS_INDEX, edmProvider.namespaceToIndex(WRITERS_FQN_STRING));
        assertNull(edmProvider.namespaceToIndex("Olingo.Test.authors"));
    }

    @Test
    public void getProperties_TypeNameAndCorrectMetaData_ListOfCsdlPropertiesRetrieved()
            throws IOException, ODataException {
        MultyElasticIndexCsdlEdmProvider edmProvider = new MultyElasticIndexCsdlEdmProvider(
                metaDataProvider, indices, nestedTypeMapper);
        List<CsdlProperty> csdlProperties = edmProvider.getProperties(AUTHORS_INDEX, AUTHOR_TYPE,
                getStubProperties());
        assertEquals(3, csdlProperties.size());
        for (CsdlProperty property : csdlProperties) {
            assertTrue(property instanceof ElasticCsdlProperty);
            assertEquals(AUTHORS_INDEX, ((ElasticCsdlProperty) property).getEIndex());
            assertEquals(AUTHOR_TYPE, ((ElasticCsdlProperty) property).getEType());
            assertEquals(property.getName(), ((ElasticCsdlProperty) property).getEField());
            assertNotNull(property.getTypeAsFQNObject());
        }
    }

    @Test(expected = ODataException.class)
    public void getProperties_MetaDataThrowsIOException_ODataExceptionRetrieved()
            throws IOException, ODataException {
        MappingMetaData mappingMetaData = mock(MappingMetaData.class);
        when(mappingMetaData.sourceAsMap()).thenThrow(new IOException("test cause"));
        MultyElasticIndexCsdlEdmProvider edmProvider = new MultyElasticIndexCsdlEdmProvider(
                metaDataProvider, indices);
        edmProvider.getProperties(AUTHORS_INDEX, AUTHOR_TYPE, mappingMetaData);
    }

    @Test
    public void getNavigationProperties_EntityTypeNameAndEmptyMappings_EmptyListRetrieved() {
        Builder<String, FieldMappingMetaData> builder = ImmutableOpenMap.builder();
        ImmutableOpenMap<String, FieldMappingMetaData> map = builder.build();
        when(metaDataProvider.getMappingsForField(AUTHORS_INDEX, ElasticConstants.PARENT_PROPERTY))
                .thenReturn(map);
        MultyElasticIndexCsdlEdmProvider edmProvider = new MultyElasticIndexCsdlEdmProvider(
                metaDataProvider, indices);
        assertTrue(edmProvider.getNavigationProperties(AUTHORS_INDEX, AUTHOR_TYPE).isEmpty());
    }

    @Test
    public void getNavigationProperties_EntityTypeNameAndMappingsEmptyValueMap_EmptyListRetrieved() {
        Builder<String, FieldMappingMetaData> builder = ImmutableOpenMap.builder();
        FieldMappingMetaData mappingMetaData = mock(FieldMappingMetaData.class);
        when(mappingMetaData.sourceAsMap()).thenReturn(new HashMap<String, Object>());
        builder.put(BOOK_TYPE, mappingMetaData);
        ImmutableOpenMap<String, FieldMappingMetaData> map = builder.build();
        when(metaDataProvider.getMappingsForField(AUTHORS_INDEX, ElasticConstants.PARENT_PROPERTY))
                .thenReturn(map);
        MultyElasticIndexCsdlEdmProvider edmProvider = new MultyElasticIndexCsdlEdmProvider(
                metaDataProvider, indices);
        assertTrue(edmProvider.getNavigationProperties(AUTHORS_INDEX, AUTHOR_TYPE).isEmpty());
    }

    @Test
    public void getNavigationProperties_EntityTypeNameAndMappings_OneChildPropertyRetrieved() {
        doReturn(getParentChildMappings()).when(metaDataProvider).getMappingsForField(AUTHORS_INDEX,
                ElasticConstants.PARENT_PROPERTY);
        MultyElasticIndexCsdlEdmProvider edmProvider = new MultyElasticIndexCsdlEdmProvider(
                metaDataProvider, indices);
        List<ElasticCsdlNavigationProperty> navigationProperties = edmProvider
                .getNavigationProperties(AUTHORS_INDEX, AUTHOR_TYPE);
        assertEquals(1, navigationProperties.size());
        CsdlNavigationProperty navigationProperty = navigationProperties.get(0);
        assertEquals(BOOK_TYPE, navigationProperty.getName());
        assertTrue(navigationProperty.isCollection());
        assertEquals(AUTHOR_TYPE, navigationProperty.getPartner());
    }

    @Test
    public void getNavigationProperties_EntityTypeNameAndMappings_OneParentPropertyRetrieved() {
        doReturn(getParentChildMappings()).when(metaDataProvider).getMappingsForField(AUTHORS_INDEX,
                ElasticConstants.PARENT_PROPERTY);
        MultyElasticIndexCsdlEdmProvider edmProvider = new MultyElasticIndexCsdlEdmProvider(
                metaDataProvider, indices);
        List<ElasticCsdlNavigationProperty> navigationProperties = edmProvider
                .getNavigationProperties(AUTHORS_INDEX, BOOK_TYPE);
        assertEquals(1, navigationProperties.size());
        CsdlNavigationProperty navigationProperty = navigationProperties.get(0);
        assertEquals(AUTHOR_TYPE, navigationProperty.getName());
        assertFalse(navigationProperty.isCollection());
        assertEquals(BOOK_TYPE, navigationProperty.getPartner());
    }

    @Test
    public void getEntityType_IndexDoesntExist_NullRetrived() throws ODataException {
        MultyElasticIndexCsdlEdmProvider edmProvider = new MultyElasticIndexCsdlEdmProvider(
                metaDataProvider, indices);
        assertNull(
                edmProvider.getEntityType(new FullQualifiedName("Test.IllegalNamespace.entity")));
    }

    @Test
    public void getEntityType_IndexExistAndEntityTypeList_EntityTypeRetrived()
            throws ODataException {
        MultyElasticIndexCsdlEdmProvider edmProvider = spy(
                new MultyElasticIndexCsdlEdmProvider(metaDataProvider, indices));
        ElasticCsdlEntityType type1 = mock(ElasticCsdlEntityType.class);
        when(type1.getEType()).thenReturn(BOOK_TYPE);
        ElasticCsdlEntityType type2 = mock(ElasticCsdlEntityType.class);
        when(type2.getEType()).thenReturn(AUTHOR_TYPE);
        doReturn(Arrays.asList(type1, type2)).when(edmProvider).getEntityTypes(AUTHORS_INDEX);
        assertEquals(type2, edmProvider.getEntityType(AUTHOR_FQN));
    }

    @Test
    public void getEntityType_IndexExistAndEmptyEntityTypeList_EntityTypeRetrived()
            throws ODataException {
        MultyElasticIndexCsdlEdmProvider edmProvider = spy(
                new MultyElasticIndexCsdlEdmProvider(metaDataProvider, indices));
        doReturn(Collections.emptyList()).when(edmProvider).getEntityTypes(AUTHORS_INDEX);
        assertNull(edmProvider.getEntityType(AUTHOR_FQN));
    }

    @Test
    public void createEntityType_IndexAndType_EntityTypeRetrived()
            throws ODataException, IOException {
        MultyElasticIndexCsdlEdmProvider edmProvider = new MultyElasticIndexCsdlEdmProvider(
                metaDataProvider, indices);
        doReturn(getStubProperties()).when(metaDataProvider).getMappingForType(AUTHORS_INDEX,
                AUTHOR_TYPE);
        doReturn(getParentChildMappings()).when(metaDataProvider).getMappingsForField(AUTHORS_INDEX,
                ElasticConstants.PARENT_PROPERTY);
        ElasticCsdlEntityType entityType = edmProvider.createEntityType(AUTHORS_INDEX, AUTHOR_TYPE);
        assertTrue(entityType instanceof ElasticCsdlEntityType);
        assertEquals(AUTHORS_INDEX, ((ElasticCsdlEntityType) entityType).getEIndex());
        assertEquals(AUTHOR_TYPE, ((ElasticCsdlEntityType) entityType).getEType());
        List<CsdlProperty> properties = entityType.getProperties();
        assertEquals(3, properties.size());
        CsdlProperty idProperty = properties.get(1);
        assertEquals(ElasticConstants.ID_FIELD_NAME, idProperty.getName());
        List<CsdlPropertyRef> keys = entityType.getKey();
        assertEquals(1, keys.size());
        CsdlPropertyRef idRef = keys.get(0);
        assertEquals(ElasticConstants.ID_FIELD_NAME, idRef.getName());
        List<CsdlNavigationProperty> navigationProperties = entityType.getNavigationProperties();
        assertEquals(1, navigationProperties.size());
        ElasticCsdlNavigationProperty bookProperty = (ElasticCsdlNavigationProperty) navigationProperties
                .get(0);
        assertEquals(BOOK_TYPE, bookProperty.getName());
        assertEquals(BOOK_TYPE, bookProperty.getEType());
        assertEquals(AUTHORS_INDEX, bookProperty.getEIndex());
        assertEquals(BOOK_FQN, bookProperty.getTypeFQN());
        assertEquals(AUTHOR_TYPE, bookProperty.getPartner());
    }

    @Test
    public void createEntityType_IndexAndTypeWithCustomIdProperty_EntityTypeRetrived()
            throws ODataException, IOException {
        MultyElasticIndexCsdlEdmProvider edmProvider = spy(
                new MultyElasticIndexCsdlEdmProvider(metaDataProvider, indices));
        MappingMetaData metaData = mock(MappingMetaData.class);
        when(metaDataProvider.getMappingForType(AUTHORS_INDEX, AUTHOR_TYPE)).thenReturn(metaData);
        doReturn(new ArrayList<>()).when(edmProvider).getProperties(AUTHORS_INDEX, AUTHOR_TYPE,
                metaData);
        doReturn(new ArrayList<>()).when(edmProvider).getNavigationProperties(AUTHORS_INDEX,
                AUTHOR_TYPE);
        ElasticCsdlEntityType entityType = edmProvider.createEntityType(AUTHORS_INDEX, AUTHOR_TYPE);
        assertTrue(entityType instanceof ElasticCsdlEntityType);
        assertEquals(AUTHORS_INDEX, ((ElasticCsdlEntityType) entityType).getEIndex());
        assertEquals(AUTHOR_TYPE, ((ElasticCsdlEntityType) entityType).getEType());
        List<CsdlProperty> properties = entityType.getProperties();
        assertEquals(1, properties.size());
        CsdlProperty idProperty = properties.get(0);
        assertEquals(ElasticConstants.ID_FIELD_NAME, idProperty.getName());
        List<CsdlPropertyRef> keys = entityType.getKey();
        assertEquals(1, keys.size());
        CsdlPropertyRef idRef = keys.get(0);
        assertEquals(ElasticConstants.ID_FIELD_NAME, idRef.getName());
    }

    @Test(expected = ODataException.class)
    public void createEntityType_MappingsAreNull_ODataExceptionRetrieved()
            throws ODataException, IOException {
        MultyElasticIndexCsdlEdmProvider edmProvider = new MultyElasticIndexCsdlEdmProvider(
                metaDataProvider, indices);
        doReturn(null).when(metaDataProvider).getMappingForType(AUTHORS_INDEX, AUTHOR_TYPE);
        edmProvider.createEntityType(AUTHORS_INDEX, AUTHOR_TYPE);
    }

    @Test
    public void createEntitySet_IndexAndType_EntitySetRetrieved() {
        MultyElasticIndexCsdlEdmProvider edmProvider = new MultyElasticIndexCsdlEdmProvider(
                metaDataProvider, indices);
        doReturn(getParentChildMappings()).when(metaDataProvider).getMappingsForField(AUTHORS_INDEX,
                ElasticConstants.PARENT_PROPERTY);
        ElasticCsdlEntitySet entitySet = edmProvider.createEntitySet(AUTHORS_INDEX, AUTHOR_TYPE);
        assertEquals(AUTHORS_INDEX, entitySet.getEIndex());
        assertEquals(AUTHOR_TYPE, entitySet.getEType());
        assertEquals(AUTHOR_TYPE, entitySet.getName());
        List<CsdlNavigationPropertyBinding> propertyBindings = entitySet
                .getNavigationPropertyBindings();
        assertEquals(1, propertyBindings.size());
        CsdlNavigationPropertyBinding propertyBinding = propertyBindings.get(0);
        assertEquals(BOOK_TYPE, propertyBinding.getPath());
        assertEquals(BOOK_TYPE, propertyBinding.getTarget());
    }

    @Test
    public void getEntitySet_ContainerNameAndSetName_EntitySetRetrieved() throws ODataException {
        MultyElasticIndexCsdlEdmProvider edmProvider = spy(
                new MultyElasticIndexCsdlEdmProvider(metaDataProvider, indices));
        CsdlEntityContainer container = mock(CsdlEntityContainer.class);
        doReturn(container).when(edmProvider).getEntityContainer();
        when(container.getEntitySet(BOOK_TYPE))
                .thenAnswer(answer -> new ElasticCsdlEntitySet().setName(answer.getArgument(0)));
        ElasticCsdlEntitySet entitySet = edmProvider.getEntitySet(edmProvider.getContainerName(),
                BOOK_TYPE);
        assertEquals(BOOK_TYPE, entitySet.getName());
    }

    @Test(expected = ODataException.class)
    public void getEntitySet_OtherContainerNameAndSetName_ODataExceptionRetrieved()
            throws ODataException {
        MultyElasticIndexCsdlEdmProvider edmProvider = spy(
                new MultyElasticIndexCsdlEdmProvider(metaDataProvider, indices));
        edmProvider.getEntitySet(new FullQualifiedName("Other.Container"), BOOK_TYPE);
    }

    @Test
    public void getEntityContainerInfo_ContainerNameNull_EntityContainerRetieved() {
        MultyElasticIndexCsdlEdmProvider edmProvider = new MultyElasticIndexCsdlEdmProvider(
                metaDataProvider, indices);
        CsdlEntityContainerInfo entityContainerInfo = edmProvider.getEntityContainerInfo(null);
        assertNotNull(entityContainerInfo);
        assertEquals(edmProvider.getContainerName(), entityContainerInfo.getContainerName());
    }

    @Test
    public void getEntityContainerInfo_ContainerName_NullRetieved() {
        MultyElasticIndexCsdlEdmProvider edmProvider = new MultyElasticIndexCsdlEdmProvider(
                metaDataProvider, indices);
        assertNull(edmProvider.getEntityContainerInfo(new FullQualifiedName("Test.ContainerName")));
    }

    @Test
    public void getSchemas_EmptyNamespaces_EmptySchemaListRetrieved() throws ODataException {
        MultyElasticIndexCsdlEdmProvider edmProvider = new MultyElasticIndexCsdlEdmProvider(
                metaDataProvider, new HashSet<String>());
        assertTrue(edmProvider.getSchemas().isEmpty());
    }

    @Test
    public void getSchemas_Namespaces_SchemaListRetrieved() throws ODataException {
        MultyElasticIndexCsdlEdmProvider edmProvider = spy(
                new MultyElasticIndexCsdlEdmProvider(metaDataProvider, indices));
        NestedTypeMapper nestedTypeMapper = mock(NestedTypeMapper.class);
        doReturn(new ArrayList<CsdlEntityType>()).when(edmProvider).getEntityTypes(anyString());
        doReturn(new ArrayList<CsdlComplexType>()).when(nestedTypeMapper)
                .getComplexTypes(anyString());
        doReturn(nestedTypeMapper).when(edmProvider).getNestedTypeMapper();
        doReturn(mock(CsdlEntityContainer.class)).when(edmProvider)
                .getEntityContainerForSchema(anyString());
        List<CsdlSchema> schemas = edmProvider.getSchemas();
        assertEquals(2, schemas.size());
    }

    @Test
    public void getEnityTypes_IndexWithEmptyMappings_EmptyListRetrieved() throws ODataException {
        MultyElasticIndexCsdlEdmProvider edmProvider = new MultyElasticIndexCsdlEdmProvider(
                metaDataProvider, indices);
        Builder<String, MappingMetaData> metadataBuilder = ImmutableOpenMap.builder();
        when(metaDataProvider.getAllMappings(WRITERS_INDEX)).thenReturn(metadataBuilder.build());
        assertTrue(edmProvider.getEntityTypes(WRITERS_INDEX).isEmpty());
    }

    @Test
    public void getEnityTypes_IndexWithMappings_ListEntityTypesRetrieved()
            throws ODataException, IOException {
        MultyElasticIndexCsdlEdmProvider edmProvider = spy(
                new MultyElasticIndexCsdlEdmProvider(metaDataProvider, indices));
        Builder<String, MappingMetaData> mappingsBuilder = ImmutableOpenMap.builder();
        mappingsBuilder.put(BOOK_TYPE, null);
        when(metaDataProvider.getAllMappings(AUTHORS_INDEX)).thenReturn(mappingsBuilder.build());
        doAnswer(answer -> new ElasticCsdlEntityType().setEIndex(answer.getArgument(0))
                .setName(answer.getArgument(1))).when(edmProvider).createEntityType(AUTHORS_INDEX,
                        BOOK_TYPE);
        List<ElasticCsdlEntityType> enityTypes = edmProvider.getEntityTypes(AUTHORS_INDEX);
        assertEquals(1, enityTypes.size());
        ElasticCsdlEntityType entityType = enityTypes.get(0);
        assertEquals(AUTHORS_INDEX, entityType.getEIndex());
        assertEquals(BOOK_TYPE, entityType.getEType());
        assertEquals(BOOK_TYPE, entityType.getName());
    }

    @Test
    public void getEntityContainerForSchema_Namespace_EntityContainerWithEntitySetsRetrieved()
            throws ODataException {
        MultyElasticIndexCsdlEdmProvider edmProvider = spy(
                new MultyElasticIndexCsdlEdmProvider(metaDataProvider, indices));
        Builder<String, MappingMetaData> mappingsBuilder = ImmutableOpenMap.builder();
        mappingsBuilder.put(AUTHOR_TYPE, null);
        mappingsBuilder.put(BOOK_TYPE, null);
        when(metaDataProvider.getAllMappings(AUTHORS_INDEX)).thenReturn(mappingsBuilder.build());
        doReturn(new ArrayList<>()).when(edmProvider).getNavigationProperties(AUTHORS_INDEX,
                AUTHOR_TYPE);
        doReturn(new ArrayList<>()).when(edmProvider).getNavigationProperties(AUTHORS_INDEX,
                BOOK_TYPE);
        CsdlEntityContainer entityContainer = edmProvider
                .getEntityContainerForSchema(AUTHORS_INDEX);
        assertEquals(edmProvider.getContainerName().getName(), entityContainer.getName());
        assertEquals(2, entityContainer.getEntitySets().size());
    }

    @Test
    public void getEntityContainerForSchema_NamespaceAndEmptyMetadata_EntityContainerWithEmptyEntitySetsRetrieved()
            throws ODataException {
        MultyElasticIndexCsdlEdmProvider edmProvider = new MultyElasticIndexCsdlEdmProvider(
                metaDataProvider, indices);
        Builder<String, MappingMetaData> mappingsBuilder = ImmutableOpenMap.builder();
        when(metaDataProvider.getAllMappings(AUTHORS_INDEX)).thenReturn(mappingsBuilder.build());
        CsdlEntityContainer entityContainer = edmProvider
                .getEntityContainerForSchema(AUTHORS_INDEX);
        assertEquals(edmProvider.getContainerName().getName(), entityContainer.getName());
        assertTrue(entityContainer.getEntitySets().isEmpty());
    }

    @Test
    public void getEntityContainer_ContainerWithEntitySetsRetrieved() throws ODataException {
        MultyElasticIndexCsdlEdmProvider edmProvider = spy(
                new MultyElasticIndexCsdlEdmProvider(metaDataProvider, indices));
        List<CsdlSchema> schemas = new ArrayList<>();
        CsdlSchema schema1 = mock(CsdlSchema.class);
        CsdlEntityContainer container = mock(CsdlEntityContainer.class);
        List<CsdlEntitySet> entitySets = new ArrayList<>();
        CsdlEntitySet set1 = mock(CsdlEntitySet.class);
        when(set1.isIncludeInServiceDocument()).thenReturn(true);
        entitySets.add(set1);
        entitySets.add(mock(CsdlEntitySet.class));
        when(container.getEntitySets()).thenReturn(entitySets);
        when(schema1.getEntityContainer()).thenReturn(container);
        schemas.add(schema1);
        CsdlSchema schema2 = mock(CsdlSchema.class);
        when(schema2.getEntityContainer()).thenReturn(mock(CsdlEntityContainer.class));
        schemas.add(schema2);
        doReturn(schemas).when(edmProvider).getSchemas();
        CsdlEntityContainer entityContainer = edmProvider.getEntityContainer();
        assertEquals(edmProvider.getContainerName().getName(), entityContainer.getName());
        assertEquals(1, entityContainer.getEntitySets().size());
    }

    @Test
    public void getComplexType_DifferenetNames_ExpectedValuesRetrieved() throws ODataException {
        MultyElasticIndexCsdlEdmProvider edmProvider = spy(
                new MultyElasticIndexCsdlEdmProvider(metaDataProvider, indices));
        List<CsdlSchema> schemas = new ArrayList<>();
        CsdlSchema schema = mock(CsdlSchema.class);
        String namespace = "OData";
        when(schema.getNamespace()).thenReturn(namespace);
        ElasticCsdlComplexType expectedComplexType = mock(ElasticCsdlComplexType.class);
        String complexTypeName = "dimension";
        when(schema.getComplexType(complexTypeName)).thenReturn(expectedComplexType);
        schemas.add(schema);
        doReturn(schemas).when(edmProvider).getSchemas();
        ElasticCsdlComplexType actualComplexType = edmProvider
                .getComplexType(new FullQualifiedName(namespace, complexTypeName));
        assertEquals(expectedComplexType, actualComplexType);
        assertNull(edmProvider.getComplexType(new FullQualifiedName("Test", "complex")));
    }

    private static MappingMetaData getStubProperties() throws IOException {
        Map<String, Object> dimension = new HashMap<>();
        dimension.put("type", "nested");
        HashMap<Object, Object> dimensionProperties = new HashMap<>();
        dimensionProperties.put("name", "string");
        dimensionProperties.put("state", "boolean");
        dimension.put("properties", dimensionProperties);
        Map<String, Object> properties = new HashMap<>();
        HashMap<Object, Object> idProperties = new HashMap<>();
        idProperties.put("type", "string");
        HashMap<Object, Object> currentProperties = new HashMap<>();
        currentProperties.put("type", "boolean");
        properties.put("id", idProperties);
        properties.put("dimension", dimension);
        properties.put("current", currentProperties);
        Map<String, Object> metadata = new HashMap<>();
        metadata.put("properties", properties);
        MappingMetaData mappingMetaData = mock(MappingMetaData.class);
        when(mappingMetaData.sourceAsMap()).thenReturn(metadata);
        return mappingMetaData;
    }

    private static ImmutableOpenMap<String, FieldMappingMetaData> getParentChildMappings() {
        Builder<String, FieldMappingMetaData> mappingsBuilder = ImmutableOpenMap.builder();
        FieldMappingMetaData mappingMetaData = mock(FieldMappingMetaData.class);
        HashMap<Object, Object> parentProperties = new HashMap<>();
        parentProperties.put("type", AUTHOR_TYPE);
        mappingsBuilder.put(BOOK_TYPE, mappingMetaData);
        HashMap<String, Object> parent = new HashMap<String, Object>();
        parent.put(ElasticConstants.PARENT_PROPERTY, parentProperties);
        when(mappingMetaData.sourceAsMap()).thenReturn(parent);
        return mappingsBuilder.build();
    }

}
