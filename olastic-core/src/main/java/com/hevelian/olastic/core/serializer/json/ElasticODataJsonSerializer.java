package com.hevelian.olastic.core.serializer.json;

import static com.hevelian.olastic.core.serializer.utils.SerializeUtils.getPropertyType;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.olingo.commons.api.data.Property;
import org.apache.olingo.commons.api.edm.EdmProperty;
import org.apache.olingo.commons.api.edm.EdmStructuredType;
import org.apache.olingo.commons.api.edm.provider.CsdlProperty;
import org.apache.olingo.commons.api.format.ContentType;
import org.apache.olingo.commons.core.edm.EdmPropertyImpl;
import org.apache.olingo.server.api.ServiceMetadata;
import org.apache.olingo.server.api.serializer.SerializerException;
import org.apache.olingo.server.api.uri.queryoption.SelectOption;
import org.apache.olingo.server.core.serializer.json.ODataJsonSerializer;
import org.apache.olingo.server.core.serializer.utils.ExpandSelectHelper;

import com.fasterxml.jackson.core.JsonGenerator;

/**
 * Custom implementation of {@link ODataJsonSerializer} to override some default
 * behavior.
 * 
 * @author rdidyk
 */
public class ElasticODataJsonSerializer extends ODataJsonSerializer {

    /**
     * Constructor to initialize content type.
     * 
     * @param contentType
     *            content type
     */
    public ElasticODataJsonSerializer(ContentType contentType) {
        super(contentType);
    }

    @Override
    protected void writeProperties(ServiceMetadata metadata, EdmStructuredType type,
            List<Property> properties, SelectOption select, JsonGenerator json)
            throws IOException, SerializerException {
        boolean all = ExpandSelectHelper.isAll(select);
        Set<String> selected = all ? new HashSet<String>()
                : ExpandSelectHelper.getSelectedPropertyNames(select.getSelectItems());
        for (Property property : properties) {
            String propertyName = property.getName();
            if (all || selected.contains(propertyName)) {
                EdmProperty edmProperty = type.getStructuralProperty(propertyName);
                if (edmProperty == null) {
                    edmProperty = new EdmPropertyImpl(metadata.getEdm(), new CsdlProperty()
                            .setType(getPropertyType(property.getValue())).setName(propertyName));
                }
                Set<List<String>> selectedPaths = all || edmProperty.isPrimitive() ? null
                        : ExpandSelectHelper.getSelectedPaths(select.getSelectItems(),
                                propertyName);
                writeProperty(metadata, edmProperty, property, selectedPaths, json);
            }
        }
    }

}
