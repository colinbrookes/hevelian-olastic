package com.hevelian.olastic.core.api.uri.queryoption.expression.member.impl;

import org.apache.olingo.commons.api.edm.EdmType;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.experimental.FieldDefaults;

/**
 * Represents expression member with type.
 *
 * @author Taras Kohut
 */
@AllArgsConstructor
@FieldDefaults(makeFinal = true, level = AccessLevel.PRIVATE)
@Getter
public abstract class TypedMember extends BaseMember {

	String field;
	EdmType edmType;

}
