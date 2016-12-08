package com.hevelian.olastic.core;

import static com.hevelian.olastic.core.utils.MetaDataUtils.castToType;

import java.util.List;

import org.apache.olingo.commons.api.edm.provider.CsdlEdmProvider;
import org.apache.olingo.commons.api.edmx.EdmxReference;
import org.apache.olingo.commons.api.format.ContentType;
import org.apache.olingo.server.api.OData;
import org.apache.olingo.server.api.etag.ServiceMetadataETagSupport;
import org.apache.olingo.server.api.serializer.ODataSerializer;
import org.apache.olingo.server.api.serializer.SerializerException;
import org.apache.olingo.server.core.ODataImpl;

import com.hevelian.olastic.core.api.edm.provider.ElasticCsdlEdmProvider;
import com.hevelian.olastic.core.serializer.json.ElasticODataJsonSerializer;
import com.hevelian.olastic.core.serializer.xml.ElasticODataXmlSerializer;

/**
 * Custom implementation of {@link OData} to override behavior of creating
 * entities and provide other side mappings from CSDL to ELasticsearch.
 * 
 * @author rdidyk
 */
public class ElasticOData extends ODataImpl {

    private ElasticOData() {
    }

    /**
     * @return a new OData instance
     */
    public static ElasticOData newInstance() {
        return new ElasticOData();
    }

    @Override
    public ElasticServiceMetadata createServiceMetadata(CsdlEdmProvider edmProvider,
            List<EdmxReference> references) {
        return createServiceMetadata(edmProvider, references, null);
    }

    @Override
    public ElasticServiceMetadata createServiceMetadata(CsdlEdmProvider edmProvider,
            List<EdmxReference> references, ServiceMetadataETagSupport serviceMetadataETagSupport) {
        return new ElasticServiceMetadata(castToType(edmProvider, ElasticCsdlEdmProvider.class),
                references, serviceMetadataETagSupport);
    }

    @Override
    public ODataSerializer createSerializer(ContentType contentType) throws SerializerException {
        ODataSerializer serializer = null;
        if (contentType.isCompatible(ContentType.APPLICATION_JSON)) {
            String metadata = contentType.getParameter(ContentType.PARAMETER_ODATA_METADATA);
            if (metadata == null
                    || ContentType.VALUE_ODATA_METADATA_MINIMAL.equalsIgnoreCase(metadata)
                    || ContentType.VALUE_ODATA_METADATA_NONE.equalsIgnoreCase(metadata)
                    || ContentType.VALUE_ODATA_METADATA_FULL.equalsIgnoreCase(metadata)) {
                serializer = new ElasticODataJsonSerializer(contentType);
            }
        } else if (contentType.isCompatible(ContentType.APPLICATION_XML)
                || contentType.isCompatible(ContentType.APPLICATION_ATOM_XML)) {
            serializer = new ElasticODataXmlSerializer();
        }
        if (serializer == null) {
            throw new SerializerException(
                    "Unsupported format: " + contentType.toContentTypeString(),
                    SerializerException.MessageKeys.UNSUPPORTED_FORMAT,
                    contentType.toContentTypeString());
        }
        return serializer;
    }

}
