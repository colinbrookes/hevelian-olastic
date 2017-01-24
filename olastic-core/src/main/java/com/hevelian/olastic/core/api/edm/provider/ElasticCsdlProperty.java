package com.hevelian.olastic.core.api.edm.provider;

import org.apache.olingo.commons.api.edm.provider.CsdlProperty;

import com.google.common.base.Objects;

/**
 * Elasticsearch CSDL property implementation.
 * 
 * @author rdidyk
 */
public class ElasticCsdlProperty extends CsdlProperty implements ElasticCsdlEdmItem<ElasticCsdlProperty> {

	private String eIndex;
	private String eType;
	private String eField;

	public String getEField() {
		return eField;
	}

	public ElasticCsdlProperty setEField(String eField) {
		this.eField = eField;
		return this;
	}

	@Override
	public String getEType() {
		return eType;
	}

	@Override
	public String getEIndex() {
		return eIndex;
	}

	@Override
	public ElasticCsdlProperty setEIndex(String eIndex) {
		this.eIndex = eIndex;
		return this;
	}

	@Override
	public ElasticCsdlProperty setEType(String eType) {
		this.eType = eType;
		return this;
	}

	@Override
	public CsdlProperty setName(String name) {
		// To avoid call setEField() in case names are the same.
		if (eField == null) {
			setEField(name);
		}
		return super.setName(name);
	}

	@Override
	public int hashCode() {
		return Objects.hashCode(eIndex, eType, eField, getName());
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		if (obj == null) {
			return false;
		}
		if (getClass() != obj.getClass()) {
			return false;
		}
		ElasticCsdlProperty other = (ElasticCsdlProperty) obj;
		return Objects.equal(this.eIndex, other.eIndex) && Objects.equal(this.eType, other.eType)
				&& Objects.equal(this.eField, other.eField) && Objects.equal(this.getName(), other.getName());
	}
}
