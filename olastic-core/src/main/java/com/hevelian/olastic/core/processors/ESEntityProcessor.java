package com.hevelian.olastic.core.processors;

import org.apache.olingo.commons.api.data.Entity;
import org.apache.olingo.commons.api.edm.EdmEntityType;
import org.apache.olingo.server.api.processor.EntityProcessor;

/**
 * Custom Elastic Processor for handling a single instance of an Entity Type.
 * 
 * @author rdidyk
 */
public abstract class ESEntityProcessor extends AbstractESReadProcessor<EdmEntityType, Entity>
        implements EntityProcessor {

}
