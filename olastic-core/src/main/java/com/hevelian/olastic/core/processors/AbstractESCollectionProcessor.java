package com.hevelian.olastic.core.processors;

import org.apache.olingo.commons.api.data.AbstractEntityCollection;
import org.apache.olingo.commons.api.edm.EdmEntityType;
import org.apache.olingo.server.api.serializer.EntityCollectionSerializerOptions;
import org.apache.olingo.server.api.serializer.ODataSerializer;
import org.apache.olingo.server.api.serializer.SerializerException;
import org.apache.olingo.server.api.serializer.SerializerResult;
import org.apache.olingo.server.api.uri.UriInfo;
import org.apache.olingo.server.api.uri.queryoption.CountOption;
import org.apache.olingo.server.api.uri.queryoption.ExpandOption;
import org.apache.olingo.server.api.uri.queryoption.SelectOption;

import com.hevelian.olastic.core.edm.ElasticEdmEntitySet;
import com.hevelian.olastic.core.processors.data.InstanceData;

/**
 * Abstract class with common logic for all collection processors.
 * 
 * @author rdidyk
 */
public abstract class AbstractESCollectionProcessor
        extends AbstractESReadProcessor<EdmEntityType, AbstractEntityCollection> {

    @Override
    protected SerializerResult serialize(ODataSerializer serializer,
            InstanceData<EdmEntityType, AbstractEntityCollection> data,
            ElasticEdmEntitySet entitySet, UriInfo uriInfo) throws SerializerException {
        String id = request.getRawBaseUri() + "/" + entitySet.getEntityType();
        ExpandOption expand = uriInfo.getExpandOption();
        SelectOption select = uriInfo.getSelectOption();
        CountOption count = uriInfo.getCountOption();
        return serializer.entityCollection(serviceMetadata, data.getType(), data.getValue(),
                EntityCollectionSerializerOptions.with()
                        .contextURL(createContextUrl(entitySet, false, expand, select, null)).id(id)
                        .count(count).select(select).expand(expand).build());
    }

}
