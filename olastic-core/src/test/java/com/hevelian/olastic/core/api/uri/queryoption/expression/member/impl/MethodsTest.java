package com.hevelian.olastic.core.api.uri.queryoption.expression.member.impl;

import com.hevelian.olastic.core.elastic.ElasticConstants;
import org.apache.olingo.commons.api.edm.EdmAnnotation;
import org.apache.olingo.commons.api.edm.EdmType;
import org.apache.olingo.commons.core.edm.primitivetype.EdmString;
import org.json.JSONObject;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static com.hevelian.olastic.core.TestUtils.getAnalyzedAnnotation;
import static org.junit.Assert.assertEquals;

/**
 * Tests for Built-in odata Query Functions.
 * 
 * @author Taras Kohut
 */
public class MethodsTest {
    String field = "someField";
    String value = "'value'";
    EdmType edmString = new EdmString();
    List<EdmAnnotation> annotations = Arrays.asList(getAnalyzedAnnotation());

    @Test
    public void contains_PrimitiveAndLiteral_QueryIsCorrect() throws Exception {
        PrimitiveMember left = new PrimitiveMember(field, annotations);
        LiteralMember right = new LiteralMember(value, edmString);
        ExpressionResult result = left.contains(right);

        JSONObject queryObj = new JSONObject(result.getQueryBuilder().toString());
        JSONObject rootObj = queryObj.getJSONObject("wildcard");
        String fieldName = field + ".keyword";
        JSONObject valueObject = rootObj.getJSONObject(fieldName);
        String actualValue = (String) valueObject.get("wildcard");
        assertEquals(ElasticConstants.WILDCARD_CHAR + value.substring(1, value.length() - 1)
                + ElasticConstants.WILDCARD_CHAR, actualValue);
    }

    @Test
    public void startsWith_PrimitiveAndLiteral_QueryIsCorrect() throws Exception {
        PrimitiveMember left = new PrimitiveMember(field, annotations);
        LiteralMember right = new LiteralMember(value, edmString);
        ExpressionResult result = left.startsWith(right);

        JSONObject queryObj = new JSONObject(result.getQueryBuilder().toString());
        JSONObject rootObj = queryObj.getJSONObject("prefix");
        String fieldName = field + ".keyword";
        JSONObject valueObject = rootObj.getJSONObject(fieldName);
        String actualValue = (String) valueObject.get("value");

        assertEquals(value.substring(1, value.length() - 1), actualValue);
    }

    @Test
    public void endsWith_PrimitiveAndLiteral_QueryIsCorrect() throws Exception {
        PrimitiveMember left = new PrimitiveMember(field, annotations);
        LiteralMember right = new LiteralMember(value, edmString);
        ExpressionResult result = left.endsWith(right);

        JSONObject queryObj = new JSONObject(result.getQueryBuilder().toString());
        JSONObject rootObj = queryObj.getJSONObject("wildcard");
        String fieldName = field + ".keyword";
        JSONObject valueObject = rootObj.getJSONObject(fieldName);
        String actualValue = (String) valueObject.get("wildcard");
        assertEquals('*' + value.substring(1, value.length() - 1), actualValue);
    }

}