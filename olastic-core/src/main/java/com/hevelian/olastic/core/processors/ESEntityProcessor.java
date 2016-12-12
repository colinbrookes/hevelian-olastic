package com.hevelian.olastic.core.processors;

import static com.hevelian.olastic.core.utils.MetaDataUtils.castToType;

import org.apache.olingo.server.api.OData;
import org.apache.olingo.server.api.ServiceMetadata;
import org.apache.olingo.server.api.processor.EntityProcessor;

import com.hevelian.olastic.core.ElasticOData;
import com.hevelian.olastic.core.ElasticServiceMetadata;

/**
 * Custom Elastic Processor for handling a single instance of an Entity Type.
 * 
 * @author rdidyk
 */
public abstract class ESEntityProcessor implements EntityProcessor, ESProcessor {

    @Override
    public void init(OData odata, ServiceMetadata serviceMetadata) {
        init(castToType(odata, ElasticOData.class),
                castToType(serviceMetadata, ElasticServiceMetadata.class));
    }
}
