package com.hevelian.olastic.core.api.uri.queryoption.expression.member.impl;

import com.hevelian.olastic.core.api.uri.queryoption.expression.member.ExpressionMember;
import org.apache.olingo.server.api.ODataApplicationException;

import static com.hevelian.olastic.core.utils.ProcessorUtils.throwNotImplemented;

/**
 * Base common class for any expression member.
 *
 * @author Taras Kohut
 */
public abstract class BaseMember implements ExpressionMember {

	@Override
	public ExpressionMember any() throws ODataApplicationException {
		return throwNotImplemented();
	}

	@Override
	public ExpressionMember all() throws ODataApplicationException {
		return throwNotImplemented();
	}

	@Override
	public ExpressionMember and(ExpressionMember expressionMember) throws ODataApplicationException {
		return throwNotImplemented();
	}

	@Override
	public ExpressionMember or(ExpressionMember expressionMember) throws ODataApplicationException {
		return throwNotImplemented();
	}

	@Override
	public ExpressionMember not() throws ODataApplicationException {
		return throwNotImplemented();
	}

	@Override
	public ExpressionMember eq(ExpressionMember expressionMember) throws ODataApplicationException {
		return throwNotImplemented();
	}

	@Override
	public ExpressionMember ne(ExpressionMember expressionMember) throws ODataApplicationException {
		return throwNotImplemented();
	}

	@Override
	public ExpressionMember ge(ExpressionMember expressionMember) throws ODataApplicationException {
		return throwNotImplemented();
	}

	@Override
	public ExpressionMember gt(ExpressionMember expressionMember) throws ODataApplicationException {
		return throwNotImplemented();
	}

	@Override
	public ExpressionMember le(ExpressionMember expressionMember) throws ODataApplicationException {
		return throwNotImplemented();
	}

	@Override
	public ExpressionMember lt(ExpressionMember expressionMember) throws ODataApplicationException {
		return throwNotImplemented();
	}

	@Override
	public ExpressionMember contains(ExpressionMember left, ExpressionMember right) throws ODataApplicationException {
		return throwNotImplemented();
	}

	@Override
	public ExpressionMember startsWith(ExpressionMember left, ExpressionMember right) throws ODataApplicationException {
		return throwNotImplemented();
	}

	@Override
	public ExpressionMember endsWith(ExpressionMember left, ExpressionMember right) throws ODataApplicationException {
		return throwNotImplemented();
	}

	@Override
	public ExpressionMember date(ExpressionMember expressionMember) throws ODataApplicationException {
		return throwNotImplemented();
	}

}
