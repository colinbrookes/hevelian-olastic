package com.hevelian.olastic.core.api.uri.queryoption.expression.member.impl;

import static com.hevelian.olastic.core.TestUtils.checkFilterParentEqualsQuery;
import static com.hevelian.olastic.core.TestUtils.checkFilterParentNotEqualsQuery;
import static com.hevelian.olastic.core.TestUtils.getAnalyzedAnnotation;
import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.List;

import org.apache.olingo.commons.api.edm.EdmAnnotation;
import org.apache.olingo.commons.api.edm.EdmType;
import org.apache.olingo.commons.core.edm.primitivetype.EdmInt32;
import org.apache.olingo.commons.core.edm.primitivetype.EdmString;
import org.apache.olingo.server.api.ODataApplicationException;
import org.json.JSONObject;
import org.junit.Test;

/**
 * Tests for {@link ParentPrimitiveMember} class.
 * 
 * @author Taras Kohut
 */
public class ParentPrimitiveMemberTest {
    private String field = "someField";
    private String value = "'value'";
    private String intValue = "10";

    private List<EdmAnnotation> annotations = Arrays.asList(getAnalyzedAnnotation());
    private EdmType edmString = new EdmString();
    private EdmType edmInt = new EdmInt32();
    private List<String> parentTypes = Arrays.asList("parentType");
    private PrimitiveMember primitiveMember = new PrimitiveMember(field, annotations);

    @Test
    public void eq_ParentAndLiteral_CorrectESQuery() throws Exception {
        ParentPrimitiveMember left = new ParentPrimitiveMember(parentTypes, primitiveMember);
        LiteralMember right = new LiteralMember(value, edmString);
        ExpressionResult result = left.eq(right);

        checkFilterParentEqualsQuery(result.getQueryBuilder().toString(), parentTypes.get(0), field,
                value);
    }

    @Test
    public void eq_SeveralTypes_CorrectParentQuery() throws Exception {
        List<String> severalTypes = Arrays.asList("Author", "Book", "Character");
        ParentPrimitiveMember left = new ParentPrimitiveMember(severalTypes, primitiveMember);
        LiteralMember right = new LiteralMember(value, edmString);
        ExpressionResult result = left.eq(right);
        JSONObject firstChild = new JSONObject(result.getQueryBuilder().toString())
                .getJSONObject("has_parent");
        String firstActualType = (String) firstChild.get("parent_type");
        assertEquals(severalTypes.get(0), firstActualType);

        JSONObject secondChild = firstChild.getJSONObject("query").getJSONObject("has_parent");
        String secondActualType = (String) secondChild.get("parent_type");
        assertEquals(severalTypes.get(1), secondActualType);

        JSONObject thirdChild = secondChild.getJSONObject("query").getJSONObject("has_parent");
        String thirdActualType = (String) thirdChild.get("parent_type");
        assertEquals(severalTypes.get(2), thirdActualType);

        JSONObject rootObj = thirdChild.getJSONObject("query").getJSONObject("term");
        String fieldName = field + ".keyword";
        JSONObject valueObject = rootObj.getJSONObject(fieldName);
        String actualValue = (String) valueObject.get("value");
        assertEquals(value.substring(1, value.length() - 1), actualValue);
    }

    @Test
    public void ne_ParentAndLiteral_CorrectESQuery() throws Exception {
        ParentPrimitiveMember left = new ParentPrimitiveMember(parentTypes, primitiveMember);
        LiteralMember right = new LiteralMember(value, edmString);
        ExpressionResult result = left.ne(right);

        checkFilterParentNotEqualsQuery(result.getQueryBuilder().toString(), parentTypes.get(0),
                field, value);
    }

    @Test
    public void ge_ParentAndLiteral_CorrectESQuery() throws Exception {
        ParentPrimitiveMember left = new ParentPrimitiveMember(parentTypes, primitiveMember);
        LiteralMember right = new LiteralMember(intValue, edmInt);
        ExpressionResult result = left.ge(right);
        checkRangeQuery(result.getQueryBuilder().toString(), true, true, "range", field, "from",
                intValue);
    }

    @Test
    public void gtParentLiteralCorrectESQuery() throws Exception {
        ParentPrimitiveMember left = new ParentPrimitiveMember(parentTypes, primitiveMember);
        LiteralMember right = new LiteralMember(intValue, edmInt);
        ExpressionResult result = left.gt(right);
        checkRangeQuery(result.getQueryBuilder().toString(), false, true, "range", field, "from",
                intValue);
    }

    @Test
    public void le_ParentAndLiteral_CorrectESQuery() throws Exception {
        ParentPrimitiveMember left = new ParentPrimitiveMember(parentTypes, primitiveMember);
        LiteralMember right = new LiteralMember(intValue, edmInt);
        ExpressionResult result = left.le(right);
        checkRangeQuery(result.getQueryBuilder().toString(), true, true, "range", field, "to",
                intValue);
    }

    @Test
    public void lt_ParentAndLiteral_CorrectESQuery() throws Exception {
        ParentPrimitiveMember left = new ParentPrimitiveMember(parentTypes, primitiveMember);
        LiteralMember right = new LiteralMember(intValue, edmInt);
        ExpressionResult result = left.lt(right);
        checkRangeQuery(result.getQueryBuilder().toString(), true, false, "range", field, "to",
                intValue);
    }

    @Test
    public void getField() throws Exception {
        ParentPrimitiveMember primitive = new ParentPrimitiveMember(parentTypes, primitiveMember);
        assertEquals(field, primitive.getPrimitiveMember().getField());
    }

    @Test
    public void getAnnotations() throws Exception {
        ParentPrimitiveMember primitive = new ParentPrimitiveMember(parentTypes, primitiveMember);
        assertEquals(annotations, primitive.getPrimitiveMember().getAnnotations());
    }

    private void checkRangeQuery(String query, boolean includeLower, boolean includeUpper,
            String rootKey, String field, String valueKey, Object expValue)
            throws ODataApplicationException {
        JSONObject parentObj = new JSONObject(query).getJSONObject("has_parent");
        String actualType = (String) parentObj.get("parent_type");
        assertEquals(parentTypes.get(0), actualType);
        JSONObject rootObj = parentObj.getJSONObject("query").getJSONObject(rootKey);
        JSONObject valueObject = rootObj.getJSONObject(field);
        String actualValue = (String) valueObject.get(valueKey);
        assertEquals(expValue, actualValue);
        assertEquals(valueObject.get("include_lower"), includeLower);
        assertEquals(valueObject.get("include_upper"), includeUpper);
    }

    @Test(expected = ODataApplicationException.class)
    public void any_ExceptionIsThrown() throws ODataApplicationException {
        new ParentPrimitiveMember(parentTypes, primitiveMember).any();
    }

    @Test(expected = ODataApplicationException.class)
    public void all_ExceptionIsThrown() throws ODataApplicationException {
        new ParentPrimitiveMember(parentTypes, primitiveMember).all();
    }

    @Test(expected = ODataApplicationException.class)
    public void and_ExceptionIsThrown() throws ODataApplicationException {
        new ParentPrimitiveMember(parentTypes, primitiveMember).and(null);
    }

    @Test(expected = ODataApplicationException.class)
    public void or_ExceptionIsThrown() throws ODataApplicationException {
        new ParentPrimitiveMember(parentTypes, primitiveMember).or(null);
    }

    @Test(expected = ODataApplicationException.class)
    public void not_ExceptionIsThrown() throws ODataApplicationException {
        new ParentPrimitiveMember(parentTypes, primitiveMember).not();
    }
}