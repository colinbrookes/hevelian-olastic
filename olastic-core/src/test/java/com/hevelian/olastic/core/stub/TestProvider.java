package com.hevelian.olastic.core.stub;

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
public class TestProvider extends CsdlAbstractEdmProvider {
    public static final String NAMESPACE = "OData.Test";
    public static final String CONTAINER_NAME = "Container";
    public static final FullQualifiedName CONTAINER = new FullQualifiedName(NAMESPACE, CONTAINER_NAME);

    public static final String DIMENSION_TYPE = "_dimension";

    public static final String AUTHOR_TYPE = "author";
    public static final String ADDRESS_TYPE = "address";
    public static final String BOOK_TYPE = "book";
    public static final String CHARACTER_TYPE = "character";

    public static final FullQualifiedName DIMENSION_FQN = new FullQualifiedName(NAMESPACE, DIMENSION_TYPE);

    public static final FullQualifiedName ADDRESS_FQN = new FullQualifiedName(NAMESPACE, ADDRESS_TYPE);
    public static final FullQualifiedName BOOK_FQN = new FullQualifiedName(NAMESPACE, BOOK_TYPE);
    public static final FullQualifiedName CHARACTER_FQN = new FullQualifiedName(NAMESPACE, CHARACTER_TYPE);
    public static final FullQualifiedName AUTHOR_FQN = new FullQualifiedName(NAMESPACE, AUTHOR_TYPE);


    public static final String ADDRESSES = "address";
    public static final String BOOKS = "book";
    public static final String CHARACTERS = "character";

    private CsdlNavigationProperty booksCollection;
    private CsdlNavigationProperty charactersCollection;
    private CsdlNavigationProperty addressesCollection;
    private CsdlNavigationProperty authorBookParent;
    private CsdlNavigationProperty authorAddressesParent;
    private CsdlNavigationProperty bookParent;


    public TestProvider() {
        booksCollection = new CsdlNavigationProperty();
        booksCollection.setCollection(true);
        booksCollection.setName(BOOKS);
        booksCollection.setPartner(AUTHOR_TYPE);
        booksCollection.setType(BOOK_FQN);

        charactersCollection = new CsdlNavigationProperty();
        charactersCollection.setCollection(true);
        charactersCollection.setName(CHARACTERS);
        charactersCollection.setPartner(BOOK_TYPE);
        charactersCollection.setType(CHARACTER_FQN);

        addressesCollection = new CsdlNavigationProperty();
        addressesCollection.setCollection(true);
        addressesCollection.setName(ADDRESSES);
        addressesCollection.setPartner(AUTHOR_TYPE);
        addressesCollection.setType(ADDRESS_FQN);

        authorBookParent = new CsdlNavigationProperty();
        authorBookParent.setName(AUTHOR_TYPE);
        authorBookParent.setPartner(BOOK_TYPE);
        authorBookParent.setType(AUTHOR_FQN);

        authorAddressesParent = new CsdlNavigationProperty();
        authorAddressesParent.setName(AUTHOR_TYPE);
        authorAddressesParent.setPartner(ADDRESS_TYPE);
        authorAddressesParent.setType(AUTHOR_FQN);

        bookParent = new CsdlNavigationProperty();
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
        CsdlProperty name = new CsdlProperty().setName("name").setType(EdmPrimitiveTypeKind.String.getFullQualifiedName());
        CsdlProperty state = new CsdlProperty().setName("state").setType(EdmPrimitiveTypeKind.String.getFullQualifiedName());
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
    public CsdlEntityContainerInfo getEntityContainerInfo(FullQualifiedName entityContainerName) throws ODataException {
        if (entityContainerName == null || entityContainerName.equals(CONTAINER)) {
            CsdlEntityContainerInfo entityContainerInfo = new CsdlEntityContainerInfo();
            entityContainerInfo.setContainerName(CONTAINER);
            return entityContainerInfo;
        }
        return null;
    }

    @Override
    public CsdlEntityType getEntityType(FullQualifiedName entityTypeName) throws ODataException {
        CsdlProperty id = new CsdlProperty().setName("_id").setType(EdmPrimitiveTypeKind.String.getFullQualifiedName());
        CsdlProperty dimensionProperty = new CsdlProperty().setName(DIMENSION_TYPE).setType(DIMENSION_FQN).setCollection(true);
        CsdlPropertyRef propertyRef = new CsdlPropertyRef();
        propertyRef.setName("_id");

        CsdlEntityType entityType = new CsdlEntityType();
        if (entityTypeName.equals(AUTHOR_FQN)) {

            CsdlProperty age = new CsdlProperty().setName("age").setType(EdmPrimitiveTypeKind.Int64.getFullQualifiedName());
            CsdlProperty name = new CsdlProperty().setName("name").setType(EdmPrimitiveTypeKind.String.getFullQualifiedName());

            entityType.setName(AUTHOR_TYPE);
            entityType.setProperties(Arrays.asList(id, age, name, dimensionProperty));
            entityType.setKey(Collections.singletonList(propertyRef));

            entityType.setNavigationProperties(Arrays.asList(addressesCollection, booksCollection));

            return entityType;
        } else if (entityTypeName.equals(ADDRESS_FQN)) {

            CsdlProperty address = new CsdlProperty().setName("address").setType(EdmPrimitiveTypeKind.String.getFullQualifiedName());
            CsdlProperty city = new CsdlProperty().setName("_city").setType(EdmPrimitiveTypeKind.String.getFullQualifiedName());

            CsdlPropertyRef cityPropertyRef = new CsdlPropertyRef();
            cityPropertyRef.setName("_city");

            List<CsdlPropertyRef> addressCompositeKey = new ArrayList<>();
            addressCompositeKey.add(propertyRef);
            addressCompositeKey.add(cityPropertyRef);

            entityType.setName(ADDRESS_TYPE);
            entityType.setProperties(Arrays.asList(id, address, city, dimensionProperty));

            entityType.setKey(addressCompositeKey);

            entityType.setNavigationProperties(Arrays.asList(authorAddressesParent));

            return entityType;
        } else if (entityTypeName.equals(BOOK_FQN)) {

            CsdlProperty title = new CsdlProperty().setName("title").setType(EdmPrimitiveTypeKind.String.getFullQualifiedName());

            entityType.setName(BOOK_TYPE);
            entityType.setProperties(Arrays.asList(id, title, dimensionProperty));
            entityType.setKey(Collections.singletonList(propertyRef));

            entityType.setNavigationProperties(Arrays.asList(authorBookParent, charactersCollection));

            return entityType;
        } else if (entityTypeName.equals(CHARACTER_FQN)) {

            CsdlProperty name = new CsdlProperty().setName("name").setType(EdmPrimitiveTypeKind.String.getFullQualifiedName());

            entityType.setName(CHARACTER_TYPE);
            entityType.setProperties(Arrays.asList(id, name, dimensionProperty));
            entityType.setKey(Collections.singletonList(propertyRef));

            entityType.setNavigationProperties(Arrays.asList(bookParent));

            return entityType;
        }

        return null;
    }

    @Override
    public CsdlEntitySet getEntitySet(FullQualifiedName entityContainer, String entitySetName) throws ODataException {
        CsdlEntitySet entitySet = new CsdlEntitySet();
        if (entityContainer.equals(CONTAINER)) {
            if (entitySetName.equals(AUTHOR_TYPE)) {
                entitySet.setName(AUTHOR_TYPE);
                entitySet.setType(AUTHOR_FQN);
                entitySet.setNavigationPropertyBindings(Arrays.asList(
                        new CsdlNavigationPropertyBinding().setPath(ADDRESS_TYPE).setTarget(ADDRESS_TYPE),
                        new CsdlNavigationPropertyBinding().setPath(BOOK_TYPE).setTarget(BOOK_TYPE)
                ));
                return entitySet;
            } else if (entitySetName.equals(BOOK_TYPE)) {
                entitySet.setName(BOOK_TYPE);
                entitySet.setType(BOOK_FQN);
                entitySet.setNavigationPropertyBindings(Arrays.asList(
                        new CsdlNavigationPropertyBinding().setPath(CHARACTER_TYPE).setTarget(CHARACTER_TYPE),
                        new CsdlNavigationPropertyBinding().setPath(AUTHOR_TYPE).setTarget(AUTHOR_TYPE)
                ));
                return entitySet;
            } else if (entitySetName.equals(ADDRESS_TYPE)) {
                entitySet.setName(ADDRESS_TYPE);
                entitySet.setType(ADDRESS_FQN);
                entitySet.setNavigationPropertyBindings(Arrays.asList(
                        new CsdlNavigationPropertyBinding().setPath(AUTHOR_TYPE).setTarget(AUTHOR_TYPE)
                ));
                return entitySet;
            } else if (entitySetName.equals(CHARACTER_TYPE)) {
                entitySet.setName(CHARACTER_TYPE);
                entitySet.setType(CHARACTER_FQN);
                entitySet.setNavigationPropertyBindings(Arrays.asList(
                        new CsdlNavigationPropertyBinding().setPath(BOOK_TYPE).setTarget(BOOK_TYPE)
                ));
                return entitySet;
            }
        }
        return null;
    }
}