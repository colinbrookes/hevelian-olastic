package com.hevelian.olastic.core.utils;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Locale;

import org.apache.olingo.commons.api.edm.EdmBindingTarget;
import org.apache.olingo.commons.api.edm.EdmNavigationProperty;
import org.apache.olingo.commons.api.ex.ODataRuntimeException;
import org.apache.olingo.commons.api.http.HttpStatusCode;
import org.apache.olingo.server.api.ODataApplicationException;

import com.hevelian.olastic.core.edm.ElasticEdmEntitySet;

/**
 * Contains utility methods.
 */
public class ProcessorUtils {
    /**
     * Example: For the following navigation:
     * DemoService.svc/Categories(1)/Products we need the EdmEntitySet for the
     * navigation property "Products"
     *
     * This is defined as follows in the metadata: <code>
     * 
     * <EntitySet Name="Categories" EntityType="OData.Demo.Category">
     * <NavigationPropertyBinding Path="Products" Target="Products"/>
     * </EntitySet>
     * </code> The "Target" attribute specifies the target EntitySet Therefore
     * we need the startEntitySet "Categories" in order to retrieve the target
     * EntitySet "Products"
     */
    public static ElasticEdmEntitySet getNavigationTargetEntitySet(
            ElasticEdmEntitySet startEdmEntitySet, EdmNavigationProperty edmNavigationProperty)
            throws ODataApplicationException {

        ElasticEdmEntitySet navigationTargetEntitySet = null;

        String navPropName = edmNavigationProperty.getName();
        EdmBindingTarget edmBindingTarget = startEdmEntitySet.getRelatedBindingTarget(navPropName);
        if (edmBindingTarget == null) {
            throw new ODataApplicationException("Not supported.",
                    HttpStatusCode.NOT_IMPLEMENTED.getStatusCode(), Locale.ROOT);
        }

        if (edmBindingTarget instanceof ElasticEdmEntitySet) {
            navigationTargetEntitySet = (ElasticEdmEntitySet) edmBindingTarget;
        } else {
            throw new ODataApplicationException("Not supported.",
                    HttpStatusCode.NOT_IMPLEMENTED.getStatusCode(), Locale.ROOT);
        }
        return navigationTargetEntitySet;
    }

    /**
     * Generates id string, for example: record(2)
     * @param entitySetName name of entity set
     * @param id odata id string
     * @return
     */
    public static URI createId(String entitySetName, Object id) {
        try {
            return new URI(entitySetName + "(" + String.valueOf(id) + ")");
        } catch (URISyntaxException e) {
            throw new ODataRuntimeException("Unable to create id for entity: " + entitySetName, e);
        }
    }

}