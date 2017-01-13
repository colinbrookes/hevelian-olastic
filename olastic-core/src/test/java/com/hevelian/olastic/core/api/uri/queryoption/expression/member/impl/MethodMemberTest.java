package com.hevelian.olastic.core.api.uri.queryoption.expression.member.impl;

import com.hevelian.olastic.core.api.uri.queryoption.expression.member.ExpressionMember;
import org.apache.olingo.commons.api.edm.EdmType;
import org.apache.olingo.commons.core.edm.primitivetype.EdmString;
import org.apache.olingo.server.api.ODataApplicationException;
import org.json.JSONObject;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Tests for {@link MethodMember} class.
 * @author Taras Kohut
 */
public class MethodMemberTest {
    String field = "someField";
    String value = "'value'";
    EdmType edmString = new EdmString();

    @Test
    public void contains_PrimitiveAndLiteral_QueryIsCorrect() throws Exception {
        MethodMember methodMember = new MethodMember();
        PrimitiveMember left = new PrimitiveMember(field, edmString);
        LiteralMember right = new LiteralMember(value, edmString);
        ExpressionResult result = methodMember.contains(left, right);

        JSONObject queryObj = new JSONObject(result.getQueryBuilder().toString());
        JSONObject rootObj = queryObj.getJSONObject("match");
        String fieldName = field;
        JSONObject valueObject = rootObj.getJSONObject(fieldName);
        String actualValue = (String)valueObject.get("query");
        assertEquals(value.substring(1,value.length()-1), actualValue);
    }

    @Test
    public void startsWith_PrimitiveAndLiteral_QueryIsCorrect() throws Exception {
        MethodMember methodMember = new MethodMember();
        PrimitiveMember left = new PrimitiveMember(field, edmString);
        LiteralMember right = new LiteralMember(value, edmString);
        ExpressionResult result = methodMember.startsWith(left, right);

        JSONObject queryObj = new JSONObject(result.getQueryBuilder().toString());
        JSONObject rootObj = queryObj.getJSONObject("prefix");
        String fieldName = field + ".keyword";
        JSONObject valueObject = rootObj.getJSONObject(fieldName);
        String actualValue = (String)valueObject.get("value");

        assertEquals(value.substring(1,value.length()-1), actualValue);
    }

    @Test
    public void endsWith_PrimitiveAndLiteral_QueryIsCorrect() throws Exception {
        MethodMember methodMember = new MethodMember();
        PrimitiveMember left = new PrimitiveMember(field, edmString);
        LiteralMember right = new LiteralMember(value, edmString);
        ExpressionResult result = methodMember.endsWith(left, right);

        JSONObject queryObj = new JSONObject(result.getQueryBuilder().toString());
        JSONObject rootObj = queryObj.getJSONObject("wildcard");
        String fieldName = field + ".keyword";
        JSONObject valueObject = rootObj.getJSONObject(fieldName);
        String actualValue = (String)valueObject.get("wildcard");
        assertEquals('*' + value.substring(1,value.length()-1), actualValue);
    }

    @Test
    public void date_Literal_QueryIsCorrect() throws Exception {
        LiteralMember literal = new LiteralMember("'value'", new EdmString());
        ExpressionMember member = new MethodMember().date(literal);
        assertEquals(member, literal);
    }

    @Test(expected = ODataApplicationException.class)
    public void eq_ExceptionIsThrown() throws ODataApplicationException {
        new MethodMember().eq(null);
    }

    @Test(expected = ODataApplicationException.class)
    public void ne_ExceptionIsThrown() throws ODataApplicationException {
        new MethodMember().ne(null);
    }

    @Test(expected = ODataApplicationException.class)
    public void le_ExceptionIsThrown() throws ODataApplicationException {
        new MethodMember().le(null);
    }

    @Test(expected = ODataApplicationException.class)
    public void lt_ExceptionIsThrown() throws ODataApplicationException {
        new MethodMember().lt(null);
    }

    @Test(expected = ODataApplicationException.class)
    public void ge_ExceptionIsThrown() throws ODataApplicationException {
        new MethodMember().ge(null);
    }

    @Test(expected = ODataApplicationException.class)
    public void gt_ExceptionIsThrown() throws ODataApplicationException {
        new MethodMember().gt(null);
    }

    @Test(expected = ODataApplicationException.class)
    public void any_ExceptionIsThrown() throws ODataApplicationException {
        new MethodMember().any();
    }

    @Test(expected = ODataApplicationException.class)
    public void all_ExceptionIsThrown() throws ODataApplicationException {
        new MethodMember().all();
    }

    @Test(expected = ODataApplicationException.class)
    public void and_ExceptionIsThrown() throws ODataApplicationException {
        new MethodMember().and(null);
    }

    @Test(expected = ODataApplicationException.class)
    public void or_ExceptionIsThrown() throws ODataApplicationException {
        new MethodMember().or(null);
    }

    @Test(expected = ODataApplicationException.class)
    public void not_ExceptionIsThrown() throws ODataApplicationException {
        new MethodMember().not();
    }

}