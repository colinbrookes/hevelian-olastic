package com.hevelian.olastic.core.edm;

import org.apache.olingo.commons.api.edm.Edm;
import org.apache.olingo.commons.api.edm.EdmComplexType;
import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.apache.olingo.commons.core.edm.EdmComplexTypeImpl;

import com.hevelian.olastic.core.api.edm.provider.ElasticCsdlComplexType;

/**
 * Custom implementation of {@link EdmComplexType}.
 * 
 * @author rdidyk
 */
public class ElasticEdmComplexType extends EdmComplexTypeImpl {

	private ElasticCsdlComplexType csdlComplexType;

	public ElasticEdmComplexType(Edm edm, FullQualifiedName name, ElasticCsdlComplexType complexType) {
		super(edm, name, complexType);
		this.csdlComplexType = complexType;
	}

	/**
	 * Get's index name in Elasticsearch.
	 * 
	 * @return index name
	 */
	public String getEIndex() {
		return csdlComplexType.getEIndex();
	}

	/**
	 * Get's type name in Elasticsearch.
	 * 
	 * @return type name
	 */
	public String getEType() {
		return csdlComplexType.getEType();
	}

	/**
	 * Get's type name in Elasticsearch.
	 * 
	 * @return type name
	 */
	public String getENestedType() {
		return csdlComplexType.geteNestedType();
	}

}
