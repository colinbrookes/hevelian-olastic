package com.hevelian.olastic.core.stub;

import com.hevelian.olastic.core.api.edm.annotations.AnnotationProvider;
import com.hevelian.olastic.core.api.edm.provider.*;
import com.hevelian.olastic.core.elastic.mappings.MappingMetaDataProvider;
import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind;
import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.apache.olingo.commons.api.edm.provider.*;
import org.apache.olingo.commons.api.ex.ODataException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Initializes stub provider for testing purposes.
 */
public class TestProvider extends ElasticCsdlEdmProvider {

    private static List<CsdlAnnotation> analyzedAnnotations = Arrays
            .asList(new AnnotationProvider().getAnnotation(AnnotationProvider.ANALYZED_TERM_NAME));
    public static final String NAMESPACE = "OData.Test";
    public static final String CONTAINER_NAME = "Container";
    public static final FullQualifiedName CONTAINER = new FullQualifiedName(NAMESPACE,
            CONTAINER_NAME);

    public static final String DIMENSION_TYPE = "_dimension";
    public static final String BOOK_INFO_TYPE = "info";
    public static final String BOOK_PAGES_TYPE = "pages";
    public static final String BOOK_WORDS_TYPE = "words";

    public static final String AUTHORS_INDEX = "authors";
    public static final String AUTHOR_TYPE = "author";
    public static final String ADDRESS_TYPE = "address";
    public static final String BOOK_TYPE = "book";
    public static final String CHARACTER_TYPE = "character";

    public static final FullQualifiedName DIMENSION_FQN = new FullQualifiedName(NAMESPACE,
            DIMENSION_TYPE);

    public static final FullQualifiedName BOOK_INFO_FQN = new FullQualifiedName(NAMESPACE,
            BOOK_INFO_TYPE);

    public static final FullQualifiedName BOOK_PAGES_FQN = new FullQualifiedName(NAMESPACE,
            BOOK_PAGES_TYPE);

    public static final FullQualifiedName BOOK_WORDS_FQN = new FullQualifiedName(NAMESPACE,
            BOOK_WORDS_TYPE);

    public static final FullQualifiedName ADDRESS_FQN = new FullQualifiedName(NAMESPACE,
            ADDRESS_TYPE);
    public static final FullQualifiedName BOOK_FQN = new FullQualifiedName(NAMESPACE, BOOK_TYPE);
    public static final FullQualifiedName CHARACTER_FQN = new FullQualifiedName(NAMESPACE,
            CHARACTER_TYPE);
    public static final FullQualifiedName AUTHOR_FQN = new FullQualifiedName(NAMESPACE,
            AUTHOR_TYPE);

    public static final String ADDRESSES = "address";
    public static final String BOOKS = "book";
    public static final String CHARACTERS = "character";

    private ElasticCsdlNavigationProperty booksCollection;
    private ElasticCsdlNavigationProperty charactersCollection;
    private ElasticCsdlNavigationProperty addressesCollection;
    private ElasticCsdlNavigationProperty authorBookParent;
    private ElasticCsdlNavigationProperty authorAddressesParent;
    private ElasticCsdlNavigationProperty bookParent;

    public TestProvider(MappingMetaDataProvider metaDataProvider) {
        super(metaDataProvider);
        booksCollection = new ElasticCsdlNavigationProperty();
        booksCollection.setCollection(true);
        booksCollection.setName(BOOKS);
        booksCollection.setPartner(AUTHOR_TYPE);
        booksCollection.setType(BOOK_FQN);

        charactersCollection = new ElasticCsdlNavigationProperty();
        charactersCollection.setCollection(true);
        charactersCollection.setName(CHARACTERS);
        charactersCollection.setPartner(BOOK_TYPE);
        charactersCollection.setType(CHARACTER_FQN);

        addressesCollection = new ElasticCsdlNavigationProperty();
        addressesCollection.setCollection(true);
        addressesCollection.setName(ADDRESSES);
        addressesCollection.setPartner(AUTHOR_TYPE);
        addressesCollection.setType(ADDRESS_FQN);

        authorBookParent = new ElasticCsdlNavigationProperty();
        authorBookParent.setName(AUTHOR_TYPE);
        authorBookParent.setPartner(BOOK_TYPE);
        authorBookParent.setType(AUTHOR_FQN);

        authorAddressesParent = new ElasticCsdlNavigationProperty();
        authorAddressesParent.setName(AUTHOR_TYPE);
        authorAddressesParent.setPartner(ADDRESS_TYPE);
        authorAddressesParent.setType(AUTHOR_FQN);

        bookParent = new ElasticCsdlNavigationProperty();
        bookParent.setName(BOOK_TYPE);
        bookParent.setPartner(CHARACTER_TYPE);
        bookParent.setType(BOOK_FQN);
    }

    @Override
    public List<CsdlSchema> getSchemas() throws ODataException {
        CsdlSchema schema = new CsdlSchema();
        schema.setNamespace(NAMESPACE);

        List<CsdlEntityType> entityTypes = new ArrayList<>();
        entityTypes.add(getEntityType(AUTHOR_FQN));
        entityTypes.add(getEntityType(BOOK_FQN));
        entityTypes.add(getEntityType(CHARACTER_FQN));
        entityTypes.add(getEntityType(ADDRESS_FQN));

        schema.setEntityTypes(entityTypes);
        List<CsdlComplexType> complexTypes = new ArrayList<>();
        CsdlComplexType complexType = new CsdlComplexType();
        List<CsdlProperty> complexTypeProperties = new ArrayList<>();
        CsdlProperty name = new ElasticCsdlProperty().setName("name")
                .setType(EdmPrimitiveTypeKind.String.getFullQualifiedName());
        CsdlProperty state = new ElasticCsdlProperty().setName("state")
                .setType(EdmPrimitiveTypeKind.String.getFullQualifiedName());
        complexTypeProperties.add(name);
        complexTypeProperties.add(state);
        complexType.setName(DIMENSION_TYPE);
        complexType.setProperties(complexTypeProperties);
        complexTypes.add(complexType);
        schema.setComplexTypes(complexTypes);
        schema.setEntityContainer(getEntityContainer());

        List<CsdlSchema> schemas = new ArrayList<>();
        schemas.add(schema);

        return schemas;
    }

    @Override
    public CsdlEntityContainer getEntityContainer() throws ODataException {
        List<CsdlEntitySet> entitySets = new ArrayList<>();
        entitySets.add(getEntitySet(CONTAINER, AUTHOR_TYPE));
        entitySets.add(getEntitySet(CONTAINER, ADDRESS_TYPE));
        entitySets.add(getEntitySet(CONTAINER, BOOK_TYPE));
        entitySets.add(getEntitySet(CONTAINER, CHARACTER_TYPE));

        CsdlEntityContainer entityContainer = new CsdlEntityContainer();
        entityContainer.setName(CONTAINER_NAME);
        entityContainer.setEntitySets(entitySets);

        return entityContainer;
    }

    @Override
    public CsdlEntityContainerInfo getEntityContainerInfo(FullQualifiedName entityContainerName) {
        if (entityContainerName == null || entityContainerName.equals(CONTAINER)) {
            CsdlEntityContainerInfo entityContainerInfo = new CsdlEntityContainerInfo();
            entityContainerInfo.setContainerName(CONTAINER);
            return entityContainerInfo;
        }
        return null;
    }

    @Override
    public ElasticCsdlEntityType getEntityType(FullQualifiedName entityTypeName)
            throws ODataException {
        CsdlProperty id = new ElasticCsdlProperty().setName("_id")
                .setType(EdmPrimitiveTypeKind.String.getFullQualifiedName());
        CsdlProperty dimensionProperty = new ElasticCsdlProperty().setName(DIMENSION_TYPE)
                .setType(DIMENSION_FQN).setCollection(true);
        CsdlPropertyRef propertyRef = new CsdlPropertyRef();
        propertyRef.setName("_id");

        ElasticCsdlEntityType entityType = new ElasticCsdlEntityType();
        entityType.setESIndex(AUTHORS_INDEX);
        if (entityTypeName.equals(AUTHOR_FQN)) {

            CsdlProperty age = new ElasticCsdlProperty().setName("age")
                    .setType(EdmPrimitiveTypeKind.Int64.getFullQualifiedName());
            CsdlProperty birthDate = new ElasticCsdlProperty().setName("birthDate")
                    .setType(EdmPrimitiveTypeKind.DateTimeOffset.getFullQualifiedName());
            CsdlProperty name = new ElasticCsdlProperty().setName("name")
                    .setAnnotations(analyzedAnnotations)
                    .setType(EdmPrimitiveTypeKind.String.getFullQualifiedName());

            entityType.setName(AUTHOR_TYPE);
            entityType.setESType(AUTHOR_TYPE);
            entityType.setProperties(Arrays.asList(id, birthDate, age, name, dimensionProperty));
            entityType.setKey(Collections.singletonList(propertyRef));

            entityType.setNavigationProperties(Arrays.asList(addressesCollection, booksCollection));

            return entityType;
        } else if (entityTypeName.equals(ADDRESS_FQN)) {

            CsdlProperty address = new ElasticCsdlProperty().setName("address")
                    .setAnnotations(analyzedAnnotations)
                    .setType(EdmPrimitiveTypeKind.String.getFullQualifiedName());
            CsdlProperty city = new ElasticCsdlProperty().setName("_city")
                    .setAnnotations(analyzedAnnotations)
                    .setType(EdmPrimitiveTypeKind.String.getFullQualifiedName());

            CsdlPropertyRef cityPropertyRef = new CsdlPropertyRef();
            cityPropertyRef.setName("_city");

            List<CsdlPropertyRef> addressCompositeKey = new ArrayList<>();
            addressCompositeKey.add(propertyRef);
            addressCompositeKey.add(cityPropertyRef);

            entityType.setName(ADDRESS_TYPE);
            entityType.setESType(ADDRESS_TYPE);
            entityType.setProperties(Arrays.asList(id, address, city, dimensionProperty));

            entityType.setKey(addressCompositeKey);

            entityType.setNavigationProperties(Arrays.asList(authorAddressesParent));

            return entityType;
        } else if (entityTypeName.equals(BOOK_FQN)) {

            CsdlProperty title = new ElasticCsdlProperty().setName("title")
                    .setAnnotations(analyzedAnnotations)
                    .setType(EdmPrimitiveTypeKind.String.getFullQualifiedName());
            CsdlProperty info = new ElasticCsdlProperty().setName("info").setType(BOOK_INFO_FQN);

            entityType.setName(BOOK_TYPE);
            entityType.setESType(BOOK_TYPE);
            entityType.setProperties(Arrays.asList(id, title, dimensionProperty, info));
            entityType.setKey(Collections.singletonList(propertyRef));

            entityType
                    .setNavigationProperties(Arrays.asList(authorBookParent, charactersCollection));

            return entityType;
        } else if (entityTypeName.equals(CHARACTER_FQN)) {

            CsdlProperty name = new ElasticCsdlProperty().setName("name")
                    .setAnnotations(analyzedAnnotations)
                    .setType(EdmPrimitiveTypeKind.String.getFullQualifiedName());

            entityType.setName(CHARACTER_TYPE);
            entityType.setESType(CHARACTER_TYPE);
            entityType.setProperties(Arrays.asList(id, name, dimensionProperty));
            entityType.setKey(Collections.singletonList(propertyRef));

            entityType.setNavigationProperties(Arrays.asList(bookParent));

            return entityType;
        }

        return null;
    }

    @Override
    public ElasticCsdlEntitySet getEntitySet(FullQualifiedName entityContainer,
            String entitySetName) throws ODataException {
        ElasticCsdlEntitySet entitySet = new ElasticCsdlEntitySet();
        entitySet.setESIndex(AUTHORS_INDEX);
        if (entityContainer.equals(CONTAINER)) {
            if (entitySetName.equals(AUTHOR_TYPE)) {
                entitySet.setESType(AUTHOR_TYPE).setName(AUTHOR_TYPE).setType(AUTHOR_FQN);
                entitySet.setNavigationPropertyBindings(Arrays.asList(
                        new CsdlNavigationPropertyBinding().setPath(ADDRESS_TYPE)
                                .setTarget(ADDRESS_TYPE),
                        new CsdlNavigationPropertyBinding().setPath(BOOK_TYPE)
                                .setTarget(BOOK_TYPE)));
                return entitySet;
            } else if (entitySetName.equals(BOOK_TYPE)) {
                entitySet.setESType(BOOK_TYPE).setName(BOOK_TYPE).setType(BOOK_FQN);
                entitySet.setNavigationPropertyBindings(Arrays.asList(
                        new CsdlNavigationPropertyBinding().setPath(CHARACTER_TYPE)
                                .setTarget(CHARACTER_TYPE),
                        new CsdlNavigationPropertyBinding().setPath(AUTHOR_TYPE)
                                .setTarget(AUTHOR_TYPE)));
                return entitySet;
            } else if (entitySetName.equals(ADDRESS_TYPE)) {
                entitySet.setESType(ADDRESS_TYPE).setName(ADDRESS_TYPE).setType(ADDRESS_FQN);
                entitySet.setNavigationPropertyBindings(
                        Arrays.asList(new CsdlNavigationPropertyBinding().setPath(AUTHOR_TYPE)
                                .setTarget(AUTHOR_TYPE)));
                return entitySet;
            } else if (entitySetName.equals(CHARACTER_TYPE)) {
                entitySet.setESType(CHARACTER_TYPE).setName(CHARACTER_TYPE).setType(CHARACTER_FQN);
                entitySet.setNavigationPropertyBindings(
                        Arrays.asList(new CsdlNavigationPropertyBinding().setPath(BOOK_TYPE)
                                .setTarget(BOOK_TYPE)));
                return entitySet;
            }
        }
        return null;
    }

    @Override
    public ElasticCsdlComplexType getComplexType(FullQualifiedName complexTypeName)
            throws ODataException {
        if (complexTypeName.equals(DIMENSION_FQN)) {
            ElasticCsdlComplexType complexType = new ElasticCsdlComplexType();
            List<CsdlProperty> complexTypeProperties = new ArrayList<>();
            CsdlProperty name = new ElasticCsdlProperty().setName("name")
                    .setType(EdmPrimitiveTypeKind.String.getFullQualifiedName());
            CsdlProperty state = new ElasticCsdlProperty().setName("state")
                    .setType(EdmPrimitiveTypeKind.String.getFullQualifiedName());
            complexTypeProperties.add(name);
            complexTypeProperties.add(state);
            complexType.setName(DIMENSION_TYPE);
            complexType.setENestedType(DIMENSION_TYPE);
            complexType.setProperties(complexTypeProperties);
            return complexType;
        }
        if (complexTypeName.equals(BOOK_INFO_FQN)) {
            ElasticCsdlComplexType complexType = new ElasticCsdlComplexType();
            List<CsdlProperty> complexTypeProperties = new ArrayList<>();
            CsdlProperty pages = new ElasticCsdlProperty().setName("pages").setType(BOOK_PAGES_FQN)
                    .setCollection(true);
            complexTypeProperties.add(pages);
            complexType.setName(BOOK_INFO_TYPE);
            complexType.setENestedType(BOOK_INFO_TYPE);
            complexType.setProperties(complexTypeProperties);
            return complexType;
        }

        if (complexTypeName.equals(BOOK_PAGES_FQN)) {
            ElasticCsdlComplexType complexType = new ElasticCsdlComplexType();
            List<CsdlProperty> complexTypeProperties = new ArrayList<>();
            CsdlProperty pageNumber = new ElasticCsdlProperty().setName("pageNumber")
                    .setType(EdmPrimitiveTypeKind.Int32.getFullQualifiedName());
            CsdlProperty pageName = new ElasticCsdlProperty().setName("pageName")
                    .setType(EdmPrimitiveTypeKind.String.getFullQualifiedName());
            CsdlProperty analyzedPageName = new ElasticCsdlProperty().setName("analyzedPageName")
                    .setAnnotations(analyzedAnnotations)
                    .setType(EdmPrimitiveTypeKind.String.getFullQualifiedName());
            CsdlProperty words = new ElasticCsdlProperty().setName("words")
                    .setType(EdmPrimitiveTypeKind.String.getFullQualifiedName())
                    .setCollection(true);
            CsdlProperty analyzedWords = new ElasticCsdlProperty().setName("analyzedWords")
                    .setType(EdmPrimitiveTypeKind.String.getFullQualifiedName())
                    .setAnnotations(analyzedAnnotations).setCollection(true);
            complexTypeProperties.add(pageNumber);
            complexTypeProperties.add(pageName);
            complexTypeProperties.add(analyzedPageName);
            complexTypeProperties.add(words);
            complexTypeProperties.add(analyzedWords);
            complexType.setName(BOOK_PAGES_TYPE);
            complexType.setENestedType(BOOK_PAGES_TYPE);
            complexType.setProperties(complexTypeProperties);
            return complexType;
        }

        return null;
    }

    @Override
    protected List<String> getSchemaNamespaces() {
        return null;
    }

    @Override
    protected String namespaceToIndex(String namespace) {
        return null;
    }
}