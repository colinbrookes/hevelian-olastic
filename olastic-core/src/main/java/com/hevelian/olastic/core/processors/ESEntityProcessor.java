package com.hevelian.olastic.core.processors;

import com.hevelian.olastic.core.ElasticOData;
import com.hevelian.olastic.core.ElasticServiceMetadata;
import org.apache.olingo.server.api.OData;
import org.apache.olingo.server.api.ServiceMetadata;
import org.apache.olingo.server.api.processor.EntityProcessor;

import static com.hevelian.olastic.core.utils.MetaDataUtils.castToType;

/**
 * Custom Elastic Processor for handling a single instance of an Entity Type.
 *
 * @author rdidyk
 */
public abstract class ESEntityProcessor implements ESProcessor, EntityProcessor {

    @Override
    public void init(OData odata, ServiceMetadata serviceMetadata) {
        init(castToType(odata, ElasticOData.class),
                castToType(serviceMetadata, ElasticServiceMetadata.class));
    }
}
