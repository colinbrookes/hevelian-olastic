package com.hevelian.olastic.core.processors;

import com.hevelian.olastic.core.edm.ElasticEdmEntitySet;
import com.hevelian.olastic.core.processors.data.InstanceData;
import org.apache.olingo.commons.api.data.Entity;
import org.apache.olingo.commons.api.edm.EdmEntityType;
import org.apache.olingo.server.api.serializer.EntitySerializerOptions;
import org.apache.olingo.server.api.serializer.ODataSerializer;
import org.apache.olingo.server.api.serializer.SerializerException;
import org.apache.olingo.server.api.serializer.SerializerResult;
import org.apache.olingo.server.api.uri.UriInfo;
import org.apache.olingo.server.api.uri.queryoption.ExpandOption;
import org.apache.olingo.server.api.uri.queryoption.SelectOption;

/**
 * @author Taras Kohut
 */
public abstract class AbstractESEntityProcessor extends AbstractESReadProcessor<EdmEntityType, Entity> {

    @Override
    protected SerializerResult serialize(ODataSerializer serializer,
                                         InstanceData<EdmEntityType, Entity> data, ElasticEdmEntitySet entitySet,
                                         UriInfo uriInfo) throws SerializerException {
        ExpandOption expand = uriInfo.getExpandOption();
        SelectOption select = uriInfo.getSelectOption();
        return serializer.entity(serviceMetadata, data.getType(), data.getValue(),
                EntitySerializerOptions.with()
                        .contextURL(createContextUrl(entitySet, true, expand, select, null))
                        .select(select).expand(expand).build());
    }


}
