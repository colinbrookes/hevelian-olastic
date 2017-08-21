package com.hevelian.olastic.core.serializer.xml;

import static com.hevelian.olastic.core.serializer.utils.SerializeUtils.getPropertyType;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;

import org.apache.olingo.commons.api.data.Property;
import org.apache.olingo.commons.api.edm.EdmProperty;
import org.apache.olingo.commons.api.edm.EdmStructuredType;
import org.apache.olingo.commons.api.edm.provider.CsdlProperty;
import org.apache.olingo.commons.core.edm.EdmPropertyImpl;
import org.apache.olingo.server.api.ServiceMetadata;
import org.apache.olingo.server.api.serializer.SerializerException;
import org.apache.olingo.server.api.uri.queryoption.SelectOption;
import org.apache.olingo.server.core.serializer.utils.ExpandSelectHelper;
import org.apache.olingo.server.core.serializer.xml.ODataXmlSerializer;

/**
 * Custom implementation of {@link ODataXmlSerializer} to override some default
 * behavior.
 * 
 * @author rdidyk
 */
public class ElasticODataXmlSerializer extends ODataXmlSerializer {

    @Override
    protected void writeProperties(ServiceMetadata metadata, EdmStructuredType type,
            List<Property> properties, SelectOption select, String xml10InvalidCharReplacement,
            XMLStreamWriter writer) throws XMLStreamException, SerializerException {
        boolean all = ExpandSelectHelper.isAll(select);
        Set<String> selected = all ? new HashSet<>()
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
                writeProperty(metadata, edmProperty, property, selectedPaths,
                        xml10InvalidCharReplacement, writer);
            }
        }
    }

}
