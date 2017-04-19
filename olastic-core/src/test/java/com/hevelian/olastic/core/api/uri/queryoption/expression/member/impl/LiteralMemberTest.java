package com.hevelian.olastic.core.api.uri.queryoption.expression.member.impl;

import org.apache.olingo.commons.api.edm.EdmAnnotation;
import org.apache.olingo.commons.api.edm.EdmType;
import org.apache.olingo.commons.core.edm.primitivetype.EdmInt32;
import org.apache.olingo.commons.core.edm.primitivetype.EdmString;
import org.apache.olingo.server.api.ODataApplicationException;
import org.json.JSONObject;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static com.hevelian.olastic.core.TestUtils.*;
import static org.junit.Assert.assertEquals;

/**
 * Tests for  {@link LiteralMember} class.
 * @author Taras Kohut
 */
public class LiteralMemberTest {

    String field = "someField";
    String value = "'value'";

    String intValue = "10";
    EdmType edmString = new EdmString();
    List<EdmAnnotation> annotations = Arrays.asList(getAnalyzedAnnotation());
    List<EdmAnnotation> emptyAnnotations = Collections.emptyList();
    EdmType edmInt = new EdmInt32();
    List<String> parentTypes = Arrays.asList("parentType");

    @Test
    public void getValue_String_CorrectValue() throws Exception {
        LiteralMember member = new LiteralMember("'value'", edmString);
        assertEquals(member.getValue(), "value");
    }

    @Test(expected = IllegalArgumentException.class)
    public void getValue_WrongString_ExceptionIsThrown() throws Exception {
        LiteralMember member = new LiteralMember("value", edmString);
        assertEquals(member.getValue(), "value");
    }

    @Test
    public void eq_LiteralAndParent_CorrectESQuery() throws Exception {
        LiteralMember left = new LiteralMember(value, edmString);
        ParentMember right = new ParentMember(parentTypes, field, annotations);
        ExpressionResult result = (ExpressionResult)left.eq(right);

        checkFilterParentEqualsQuery(result.getQueryBuilder().toString(), parentTypes.get(0), field, value);
    }

    @Test
    public void eq_LiteralAndPrimitive_CorrectESQuery() throws Exception {
        LiteralMember left = new LiteralMember(value, edmString);
        PrimitiveMember right = new PrimitiveMember(field, annotations);
        ExpressionResult result = (ExpressionResult)left.eq(right);

        checkFilterEqualsQuery(result.getQueryBuilder().toString(), field, value);
    }

    @Test
    public void eq_SeveralTypes_CorrectParentQuery() throws Exception {
        List<String> severalTypes = Arrays.asList("Author", "Book", "Character");
        LiteralMember left = new LiteralMember(value, edmString);
        ParentMember right = new ParentMember(severalTypes, field, annotations);
        ExpressionResult result = (ExpressionResult)left.eq(right);
        JSONObject firstChild = new JSONObject(result.getQueryBuilder().toString()).getJSONObject("has_parent");
        String firstActualType = (String)firstChild.get("parent_type");
        assertEquals(severalTypes.get(0), firstActualType);

        JSONObject secondChild = firstChild.getJSONObject("query").getJSONObject("has_parent");
        String secondActualType = (String)secondChild.get("parent_type");
        assertEquals(severalTypes.get(1), secondActualType);

        JSONObject thirdChild = secondChild.getJSONObject("query").getJSONObject("has_parent");
        String thirdActualType = (String)thirdChild.get("parent_type");
        assertEquals(severalTypes.get(2), thirdActualType);

        JSONObject rootObj = thirdChild.getJSONObject("query").getJSONObject("term");
        String fieldName = field + ".keyword";
        JSONObject valueObject = rootObj.getJSONObject(fieldName);
        String actualValue = (String)valueObject.get("value");
        assertEquals(value.substring(1,value.length()-1), actualValue);
    }


    @Test
    public void ne_LiteralAndParent_CorrectESQuery() throws Exception {
        LiteralMember left = new LiteralMember(value, edmString);
        ParentMember right = new ParentMember(parentTypes, field, annotations);
        ExpressionResult result = (ExpressionResult)left.ne(right);

        checkFilterParentNotEqualsQuery(result.getQueryBuilder().toString(), parentTypes.get(0), field, value);
    }

    @Test
    public void ne_LiteralAndPrimitive_CorrectESQuery() throws Exception {
        LiteralMember left = new LiteralMember(value, edmString);
        PrimitiveMember right = new PrimitiveMember(field, annotations);
        ExpressionResult result = (ExpressionResult)left.ne(right);

        checkFilterNotEqualsQuery(result.getQueryBuilder().toString(), field, value);
    }

    @Test
    public void ge_LiteralAndParent_CorrectESQuery() throws Exception {
        LiteralMember left = new LiteralMember(intValue, edmInt);
        ParentMember right = new ParentMember(parentTypes, field, emptyAnnotations);
        ExpressionResult result = (ExpressionResult)left.ge(right);
        checkParentRangeQuery(result.getQueryBuilder().toString(), true, true, "range", field, "to", intValue);
    }

    @Test
    public void ge_LiteralAndPrimitive_CorrectESQuery() throws Exception {
        LiteralMember left = new LiteralMember(intValue, edmInt);
        PrimitiveMember right = new PrimitiveMember(field, emptyAnnotations);
        ExpressionResult result = (ExpressionResult)left.ge(right);
        checkFilterRangeQuery(result.getQueryBuilder().toString(), "le", field,  intValue);
    }

    @Test
    public void gtLiteralAndParentCorrectESQuery() throws Exception {
        LiteralMember left = new LiteralMember(intValue, edmInt);
        ParentMember right = new ParentMember(parentTypes, field, emptyAnnotations);
        ExpressionResult result = (ExpressionResult)left.gt(right);
        checkParentRangeQuery(result.getQueryBuilder().toString(), true, false, "range", field, "to", intValue);
    }

    @Test
    public void gtLiteralAndPrimitiveCorrectESQuery() throws Exception {
        LiteralMember left = new LiteralMember(intValue, edmInt);
        PrimitiveMember right = new PrimitiveMember(field, emptyAnnotations);
        ExpressionResult result = (ExpressionResult)left.gt(right);
        checkFilterRangeQuery(result.getQueryBuilder().toString(), "lt", field, intValue);
    }

    @Test
    public void le_LiteralAndParent_CorrectESQuery() throws Exception {
        LiteralMember left = new LiteralMember(intValue, edmInt);
        ParentMember right = new ParentMember(parentTypes, field, emptyAnnotations);
        ExpressionResult result = (ExpressionResult)left.le(right);
        checkParentRangeQuery(result.getQueryBuilder().toString(), true, true, "range", field, "from", intValue);
    }

    @Test
    public void le_LiteralAndPrimitive_CorrectESQuery() throws Exception {
        LiteralMember left = new LiteralMember(intValue, edmInt);
        PrimitiveMember right = new PrimitiveMember(field, emptyAnnotations);
        ExpressionResult result = (ExpressionResult)left.le(right);
        checkFilterRangeQuery(result.getQueryBuilder().toString(), "ge", field, intValue);
    }

    @Test
    public void lt_LiteralAndParent_CorrectESQuery() throws Exception {
        LiteralMember left = new LiteralMember(intValue, edmInt);
        ParentMember right = new ParentMember(parentTypes, field, emptyAnnotations);
        ExpressionResult result = (ExpressionResult)left.lt(right);
        checkParentRangeQuery(result.getQueryBuilder().toString(), false, true, "range", field, "from", intValue);
    }

    @Test
    public void lt_LiteralAndPrimitive_CorrectESQuery() throws Exception {
        LiteralMember left = new LiteralMember(intValue, edmInt);
        PrimitiveMember right = new PrimitiveMember(field, emptyAnnotations);
        ExpressionResult result = (ExpressionResult)left.lt(right);
        checkFilterRangeQuery(result.getQueryBuilder().toString(), "gt", field, intValue);
    }

    @Test(expected = ODataApplicationException.class)
    public void any_ExceptionIsThrown() throws ODataApplicationException {
        new LiteralMember(value, edmString).any();
    }

    @Test(expected = ODataApplicationException.class)
    public void all_ExceptionIsThrown() throws ODataApplicationException {
        new LiteralMember(value, edmString).all();
    }

    @Test(expected = ODataApplicationException.class)
    public void contains_ExceptionIsThrown() throws ODataApplicationException {
        new LiteralMember(value, edmString).contains(null, null);
    }

    @Test(expected = ODataApplicationException.class)
    public void startsWith_ExceptionIsThrown() throws ODataApplicationException {
        new LiteralMember(value, edmString).startsWith(null, null);
    }

    @Test(expected = ODataApplicationException.class)
    public void endsWith_ExceptionIsThrown() throws ODataApplicationException {
        new LiteralMember(value, edmString).endsWith(null, null);
    }

    @Test(expected = ODataApplicationException.class)
    public void date_ExceptionIsThrown() throws ODataApplicationException {
        new LiteralMember(value, edmString).date(null);
    }

    @Test(expected = ODataApplicationException.class)
    public void and_ExceptionIsThrown() throws ODataApplicationException {
        new LiteralMember(value, edmString).and(null);
    }

    @Test(expected = ODataApplicationException.class)
    public void or_ExceptionIsThrown() throws ODataApplicationException {
        new LiteralMember(value, edmString).or(null);
    }

    @Test(expected = ODataApplicationException.class)
    public void not_ExceptionIsThrown() throws ODataApplicationException {
        new LiteralMember(value, edmString).not();
    }

    private void checkParentRangeQuery(String query, boolean includeLower, boolean includeUpper,
                                 String rootKey, String field, String valueKey, Object expValue) throws ODataApplicationException {
        JSONObject parentObj = new JSONObject(query).getJSONObject("has_parent");
        String actualType = (String)parentObj.get("parent_type");
        assertEquals(parentTypes.get(0), actualType);
        JSONObject rootObj = parentObj.getJSONObject("query").getJSONObject(rootKey);
        JSONObject valueObject = rootObj.getJSONObject(field);
        String actualValue = (String)valueObject.get(valueKey);
        assertEquals(expValue, actualValue);
        assertEquals(valueObject.get("include_lower"), includeLower);
        assertEquals(valueObject.get("include_upper"), includeUpper);
    }

}