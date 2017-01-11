package com.hevelian.olastic.core.utils;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Locale;

import org.apache.olingo.commons.api.edm.EdmBindingTarget;
import org.apache.olingo.commons.api.edm.EdmNavigationProperty;
import org.apache.olingo.commons.api.ex.ODataRuntimeException;
import org.apache.olingo.commons.api.http.HttpStatusCode;
import org.apache.olingo.server.api.ODataApplicationException;
import org.apache.olingo.server.api.uri.UriInfo;
import org.apache.olingo.server.api.uri.UriResource;
import org.apache.olingo.server.api.uri.UriResourceEntitySet;

import com.hevelian.olastic.core.edm.ElasticEdmEntitySet;

/**
 * Contains utility methods.
 * 
 * @author Taras Kohut
 * @contributor rdidyk
 */
public final class ProcessorUtils {
    private ProcessorUtils() {
    }

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
    public static ElasticEdmEntitySet getNavigationTargetEntitySet(ElasticEdmEntitySet entitySet,
            EdmNavigationProperty navProperty) throws ODataApplicationException {
        ElasticEdmEntitySet navigationTargetEntitySet = null;
        EdmBindingTarget edmBindingTarget = entitySet
                .getRelatedBindingTarget(navProperty.getName());
        if (edmBindingTarget == null) {
            throwNotImplemented("Not supported.");
        }
        if (edmBindingTarget instanceof ElasticEdmEntitySet) {
            navigationTargetEntitySet = (ElasticEdmEntitySet) edmBindingTarget;
        } else {
            throwNotImplemented("Not supported.");
        }
        return navigationTargetEntitySet;
    }

    /**
     * Gets first resource entity set from URI info.
     * 
     * @param uriInfo
     *            URI info
     * @return first entity set
     * @throws ODataApplicationException
     */
    public static UriResourceEntitySet getFirstResourceEntitySet(UriInfo uriInfo)
            throws ODataApplicationException {
        List<UriResource> resourceParts = uriInfo.getUriResourceParts();
        UriResource uriResource = resourceParts.get(0);
        if (!(uriResource instanceof UriResourceEntitySet)) {
            throwNotImplemented("Only EntitySet is supported");
        }
        return (UriResourceEntitySet) uriResource;
    }

    /**
     * Generates id string, for example: record(2)
     * 
     * @param entitySetName
     *            name of entity set
     * @param id
     *            odata id string
     * @return
     */
    public static URI createId(String entitySetName, Object id) {
        try {
            return new URI(entitySetName + "(" + id + ")");
        } catch (URISyntaxException e) {
            throw new ODataRuntimeException("Unable to create id for entity: " + entitySetName, e);
        }
    }

    /**
     * Method throws exception with HTTP.501 status code and with appropriate
     * message.
     * 
     * @param msg
     *            message to show
     * @throws ODataApplicationException
     *             created exception
     */
    public static <T> T throwNotImplemented(String msg) throws ODataApplicationException {
        throw new ODataApplicationException(msg, HttpStatusCode.NOT_IMPLEMENTED.getStatusCode(),
                Locale.ROOT);
    }

}