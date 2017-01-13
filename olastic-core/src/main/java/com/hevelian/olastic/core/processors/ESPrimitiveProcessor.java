package com.hevelian.olastic.core.processors;

import org.apache.olingo.commons.api.data.Property;
import org.apache.olingo.commons.api.edm.EdmPrimitiveType;
import org.apache.olingo.server.api.processor.PrimitiveProcessor;

/**
 * Custom elastic Processor for handling an instance of a primitive type, e.g.,
 * a primitive property of an entity.
 * 
 * @author rdidyk
 */
public abstract class ESPrimitiveProcessor
        extends AbstractESReadProcessor<EdmPrimitiveType, Property> implements PrimitiveProcessor {
}
