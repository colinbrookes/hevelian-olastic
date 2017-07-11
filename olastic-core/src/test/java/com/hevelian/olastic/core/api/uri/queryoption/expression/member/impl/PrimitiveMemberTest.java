package com.hevelian.olastic.core.api.uri.queryoption.expression.member.impl;

import static com.hevelian.olastic.core.TestUtils.checkFilterEqualsQuery;
import static com.hevelian.olastic.core.TestUtils.checkFilterNotEqualsQuery;
import static com.hevelian.olastic.core.TestUtils.checkFilterRangeQuery;
import static com.hevelian.olastic.core.TestUtils.getAnalyzedAnnotation;
import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.List;

import org.apache.olingo.commons.api.edm.EdmAnnotation;
import org.apache.olingo.commons.api.edm.EdmType;
import org.apache.olingo.commons.core.edm.primitivetype.EdmInt32;
import org.apache.olingo.commons.core.edm.primitivetype.EdmString;
import org.apache.olingo.server.api.ODataApplicationException;
import org.junit.Test;

/**
 * Tests for {@link PrimitiveMember} class.
 * 
 * @author Taras Kohut
 */
public class PrimitiveMemberTest {
    private String field = "someField";
    private String value = "'value'";
    private String intValue = "10";

    private EdmType edmString = new EdmString();
    private EdmType edmInt = new EdmInt32();
    private List<EdmAnnotation> annotations = Arrays.asList(getAnalyzedAnnotation());

    @Test
    public void eq_PrimitiveAndLiteral_CorrectESQuery() throws Exception {
        PrimitiveMember left = new PrimitiveMember(field, annotations);
        LiteralMember right = new LiteralMember(value, edmString);
        ExpressionResult result = left.eq(right);
        checkFilterEqualsQuery(result.getQueryBuilder().toString(), field, value);
    }

    @Test
    public void ne_PrimitiveAndLiteral_CorrectESQuery() throws Exception {
        PrimitiveMember left = new PrimitiveMember(field, annotations);
        LiteralMember right = new LiteralMember(value, edmString);
        ExpressionResult result = left.ne(right);
        checkFilterNotEqualsQuery(result.getQueryBuilder().toString(), field, value);

    }

    @Test
    public void ge_PrimitiveAndLiteral_CorrectESQuery() throws Exception {
        PrimitiveMember left = new PrimitiveMember(field, annotations);
        LiteralMember right = new LiteralMember(intValue, edmInt);
        ExpressionResult result = left.ge(right);
        checkFilterRangeQuery(result.getQueryBuilder().toString(), "ge", field, intValue);
    }

    @Test
    public void gtPrimitiveLiteralCorrectESQuery() throws Exception {
        PrimitiveMember left = new PrimitiveMember(field, annotations);
        LiteralMember right = new LiteralMember(intValue, edmInt);
        ExpressionResult result = left.gt(right);
        checkFilterRangeQuery(result.getQueryBuilder().toString(), "gt", field, intValue);
    }

    @Test
    public void le_PrimitiveAndLiteral_CorrectESQuery() throws Exception {
        PrimitiveMember left = new PrimitiveMember(field, annotations);
        LiteralMember right = new LiteralMember(intValue, edmInt);
        ExpressionResult result = left.le(right);
        checkFilterRangeQuery(result.getQueryBuilder().toString(), "le", field, intValue);
    }

    @Test
    public void lt_PrimitiveAndLiteral_CorrectESQuery() throws Exception {
        PrimitiveMember left = new PrimitiveMember(field, annotations);
        LiteralMember right = new LiteralMember(intValue, edmInt);
        ExpressionResult result = left.lt(right);
        checkFilterRangeQuery(result.getQueryBuilder().toString(), "lt", field, intValue);
    }

    @Test
    public void getField() throws Exception {
        PrimitiveMember primitive = new PrimitiveMember(field, annotations);
        assertEquals(field, primitive.getField());
    }

    @Test
    public void getAnnotations() throws Exception {
        PrimitiveMember primitive = new PrimitiveMember(field, annotations);
        assertEquals(annotations, primitive.getAnnotations());
    }

    @Test(expected = ODataApplicationException.class)
    public void any_ExceptionIsThrown() throws ODataApplicationException {
        new PrimitiveMember(field, annotations).any();
    }

    @Test(expected = ODataApplicationException.class)
    public void all_ExceptionIsThrown() throws ODataApplicationException {
        new PrimitiveMember(field, annotations).all();
    }

    @Test(expected = ODataApplicationException.class)
    public void and_ExceptionIsThrown() throws ODataApplicationException {
        new PrimitiveMember(field, annotations).and(null);
    }

    @Test(expected = ODataApplicationException.class)
    public void or_ExceptionIsThrown() throws ODataApplicationException {
        new PrimitiveMember(field, annotations).or(null);
    }

    @Test(expected = ODataApplicationException.class)
    public void not_ExceptionIsThrown() throws ODataApplicationException {
        new PrimitiveMember(field, annotations).not();
    }

}