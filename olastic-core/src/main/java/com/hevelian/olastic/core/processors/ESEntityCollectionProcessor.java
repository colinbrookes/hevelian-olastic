package com.hevelian.olastic.core.processors;

import org.apache.olingo.server.api.OData;
import org.apache.olingo.server.api.ServiceMetadata;
import org.apache.olingo.server.api.processor.EntityCollectionProcessor;

import com.hevelian.olastic.core.ElasticOData;
import com.hevelian.olastic.core.ElasticServiceMetadata;
import static com.hevelian.olastic.core.utils.MetaDataUtils.castToType;

/**
 * Custom Elastic processor for handling a collection of entities, e.g., an
 * Entity Set.
 * 
 * @author rdidyk
 */
public abstract class ESEntityCollectionProcessor
        implements EntityCollectionProcessor, ESProcessor {

    @Override
    public void init(OData odata, ServiceMetadata serviceMetadata) {
        init(castToType(odata, ElasticOData.class),
                castToType(serviceMetadata, ElasticServiceMetadata.class));
    }

}
