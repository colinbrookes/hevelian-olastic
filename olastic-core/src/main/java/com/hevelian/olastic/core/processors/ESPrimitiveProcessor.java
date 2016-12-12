package com.hevelian.olastic.core.processors;

import static com.hevelian.olastic.core.utils.MetaDataUtils.castToType;

import org.apache.olingo.server.api.OData;
import org.apache.olingo.server.api.ServiceMetadata;
import org.apache.olingo.server.api.processor.PrimitiveProcessor;

import com.hevelian.olastic.core.ElasticOData;
import com.hevelian.olastic.core.ElasticServiceMetadata;

/**
 * Custom elastic Processor for handling an instance of a primitive type, e.g.,
 * a primitive property of an entity.
 * 
 * @author rdidyk
 */
public abstract class ESPrimitiveProcessor implements PrimitiveProcessor, ESProcessor {

    @Override
    public void init(OData odata, ServiceMetadata serviceMetadata) {
        init(castToType(odata, ElasticOData.class),
                castToType(serviceMetadata, ElasticServiceMetadata.class));
    }
}
