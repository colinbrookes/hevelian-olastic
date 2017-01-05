package com.hevelian.olastic.core.api.uri.queryoption.expression.member.impl;

import org.apache.olingo.commons.api.edm.EdmType;
import org.apache.olingo.commons.core.edm.primitivetype.EdmInt32;
import org.apache.olingo.commons.core.edm.primitivetype.EdmString;
import org.apache.olingo.server.api.ODataApplicationException;
import org.junit.Test;

import static com.hevelian.olastic.core.TestUtils.checkFilterEqualsQuery;
import static com.hevelian.olastic.core.TestUtils.checkFilterNotEqualsQuery;
import static com.hevelian.olastic.core.TestUtils.checkFilterRangeQuery;
import static org.junit.Assert.assertEquals;

/**
 * Tests for {@link PrimitiveMember} class.
 * @author Taras Kohut
 */
public class PrimitiveMemberTest {
    String field = "someField";
    String value = "'value'";
    String intValue = "10";
    EdmType edmString = new EdmString();
    EdmType edmInt = new EdmInt32();
    @Test
    public void eq_PrimitiveAndLiteral_CorrectESQuery() throws Exception {
        PrimitiveMember left = new PrimitiveMember(field, edmString);
        LiteralMember right = new LiteralMember(value, edmString);
        ExpressionResult result = left.eq(right);
        checkFilterEqualsQuery(result.getQueryBuilder().toString(), field, value);
    }

    @Test
    public void ne_PrimitiveAndLiteral_CorrectESQuery() throws Exception {
        PrimitiveMember left = new PrimitiveMember(field, edmString);
        LiteralMember right = new LiteralMember(value, edmString);
        ExpressionResult result = left.ne(right);
        checkFilterNotEqualsQuery(result.getQueryBuilder().toString(), field, value);

    }

    @Test
    public void ge_PrimitiveAndLiteral_CorrectESQuery() throws Exception {
        PrimitiveMember left = new PrimitiveMember(field, edmString);
        LiteralMember right = new LiteralMember(intValue, edmInt);
        ExpressionResult result = left.ge(right);
        checkFilterRangeQuery(result.getQueryBuilder().toString(), "ge", field, intValue);
    }

    @Test
    public void gtPrimitiveLiteralCorrectESQuery() throws Exception {
        PrimitiveMember left = new PrimitiveMember(field, edmString);
        LiteralMember right = new LiteralMember(intValue, edmInt);
        ExpressionResult result = left.gt(right);
        checkFilterRangeQuery(result.getQueryBuilder().toString(), "gt", field, intValue);
    }

    @Test
    public void le_PrimitiveAndLiteral_CorrectESQuery() throws Exception {
        PrimitiveMember left = new PrimitiveMember(field, edmString);
        LiteralMember right = new LiteralMember(intValue, edmInt);
        ExpressionResult result = left.le(right);
        checkFilterRangeQuery(result.getQueryBuilder().toString(), "le", field, intValue);
    }

    @Test
    public void lt_PrimitiveAndLiteral_CorrectESQuery() throws Exception {
        PrimitiveMember left = new PrimitiveMember(field, edmString);
        LiteralMember right = new LiteralMember(intValue, edmInt);
        ExpressionResult result = left.lt(right);
        checkFilterRangeQuery(result.getQueryBuilder().toString(), "lt", field, intValue);
    }

    @Test
    public void getField() throws Exception {
        PrimitiveMember primitive = new PrimitiveMember(field, edmString);
        assertEquals(field, primitive.getField());
    }

    @Test
    public void getEdmType() throws Exception {
        PrimitiveMember primitive = new PrimitiveMember(field, edmString);
        assertEquals(edmString, primitive.getEdmType());
    }

    @Test(expected = ODataApplicationException.class)
    public void any_ExceptionIsThrown() throws ODataApplicationException {
        new PrimitiveMember(field, edmString).any();
    }

    @Test(expected = ODataApplicationException.class)
    public void all_ExceptionIsThrown() throws ODataApplicationException {
        new PrimitiveMember(field, edmString).all();
    }

    @Test(expected = ODataApplicationException.class)
    public void contains_ExceptionIsThrown() throws ODataApplicationException {
        new PrimitiveMember(field, edmString).contains(null, null);
    }

    @Test(expected = ODataApplicationException.class)
    public void startsWith_ExceptionIsThrown() throws ODataApplicationException {
        new PrimitiveMember(field, edmString).startsWith(null, null);
    }

    @Test(expected = ODataApplicationException.class)
    public void endsWith_ExceptionIsThrown() throws ODataApplicationException {
        new PrimitiveMember(field, edmString).endsWith(null, null);
    }

    @Test(expected = ODataApplicationException.class)
    public void date_ExceptionIsThrown() throws ODataApplicationException {
        new PrimitiveMember(field, edmString).date(null);
    }

    @Test(expected = ODataApplicationException.class)
    public void and_ExceptionIsThrown() throws ODataApplicationException {
        new PrimitiveMember(field, edmString).and(null);
    }

    @Test(expected = ODataApplicationException.class)
    public void or_ExceptionIsThrown() throws ODataApplicationException {
        new PrimitiveMember(field, edmString).or(null);
    }

    @Test(expected = ODataApplicationException.class)
    public void not_ExceptionIsThrown() throws ODataApplicationException {
        new PrimitiveMember(field, edmString).not();
    }

}