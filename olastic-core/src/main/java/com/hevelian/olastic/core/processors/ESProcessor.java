package com.hevelian.olastic.core.processors;

import org.apache.olingo.server.api.OData;
import org.apache.olingo.server.api.ServiceMetadata;
import org.apache.olingo.server.api.processor.Processor;

import com.hevelian.olastic.core.ElasticOData;
import com.hevelian.olastic.core.ElasticServiceMetadata;

/**
 * Base interface for all Elastic processor types.
 */
public interface ESProcessor extends Processor {

    /**
     * Initializes the processor for each HTTP request - response cycle. This
     * method is called by inner {@link #init(OData, ServiceMetadata)}.
     * 
     * @see #init(OData, ServiceMetadata)
     * @param odata
     *            Elastic OData instance
     * @param serviceMetadata
     *            Elastic Service metadata instance
     */
    public void init(ElasticOData odata, ElasticServiceMetadata serviceMetadata);

}
