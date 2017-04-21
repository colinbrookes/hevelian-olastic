package com.hevelian.olastic.core.api.uri.queryoption.expression;

import static com.hevelian.olastic.core.TestUtils.checkFilterEqualsQuery;
import static com.hevelian.olastic.core.TestUtils.checkFilterNotEqualsQuery;
import static com.hevelian.olastic.core.TestUtils.checkFilterParentEqualsQuery;
import static com.hevelian.olastic.core.TestUtils.checkFilterRangeQuery;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.mock;

import java.util.ArrayList;

import org.apache.olingo.server.api.OData;
import org.apache.olingo.server.api.ODataApplicationException;
import org.apache.olingo.server.api.ServiceMetadata;
import org.apache.olingo.server.api.uri.UriInfo;
import org.apache.olingo.server.api.uri.queryoption.expression.BinaryOperatorKind;
import org.apache.olingo.server.core.uri.parser.Parser;
import org.apache.olingo.server.core.uri.parser.UriParserException;
import org.apache.olingo.server.core.uri.validator.UriValidationException;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.Before;
import org.junit.Test;

import com.hevelian.olastic.core.ElasticOData;
import com.hevelian.olastic.core.ElasticServiceMetadata;
import com.hevelian.olastic.core.api.uri.queryoption.expression.member.ExpressionMember;
import com.hevelian.olastic.core.api.uri.queryoption.expression.member.impl.ExpressionResult;
import com.hevelian.olastic.core.elastic.ElasticConstants;
import com.hevelian.olastic.core.elastic.mappings.MappingMetaDataProvider;
import com.hevelian.olastic.core.stub.TestProvider;

/**
 * Tests for {@link ElasticSearchExpressionVisitor} class. These tests look more
 * like integration ones, this is required because in order to perform regular
 * unit testing several olingo objects should be created manually, and this is
 * relatively hard.
 * 
 * @author Taras Kohut
 */
public class ElasticSearchExpressionVisitorTest {
    protected ElasticServiceMetadata defaultMetadata;
    protected ElasticOData defaultOData;

    @Before
    public void setUp() throws UriParserException, UriValidationException {
        defaultOData = ElasticOData.newInstance();
        defaultMetadata = defaultOData.createServiceMetadata(
                new TestProvider(mock(MappingMetaDataProvider.class)), new ArrayList<>());
    }

    public static UriInfo buildUriInfo(ServiceMetadata metadata, OData odata, String rawODataPath,
            String rawQueryPath) throws UriParserException, UriValidationException {
        return new Parser(metadata.getEdm(), odata).parseUri(rawODataPath, rawQueryPath, null);
    }

    @Test
    public void visitBinaryOperator_gt_CorrectESQuery() throws Exception {
        String rawODataPath = "/author";
        String rawQueryPath = "$filter=age gt 30";
        UriInfo uriInfo = buildUriInfo(defaultMetadata, defaultOData, rawODataPath, rawQueryPath);
        ExpressionMember result = uriInfo.getFilterOption().getExpression()
                .accept(new ElasticSearchExpressionVisitor());
        String query = ((ExpressionResult) result).getQueryBuilder().toString();
        checkFilterRangeQuery(query, "gt", "age", "30");
    }

    @Test
    public void visitBinaryOperator_ge_CorrectESQuery() throws Exception {
        String rawODataPath = "/author";
        String rawQueryPath = "$filter=age ge 30";
        UriInfo uriInfo = buildUriInfo(defaultMetadata, defaultOData, rawODataPath, rawQueryPath);
        ExpressionMember result = uriInfo.getFilterOption().getExpression()
                .accept(new ElasticSearchExpressionVisitor());
        String query = ((ExpressionResult) result).getQueryBuilder().toString();
        checkFilterRangeQuery(query, "ge", "age", "30");
    }

    @Test
    public void visitBinaryOperator_lt_CorrectESQuery() throws Exception {
        String rawODataPath = "/author";
        String rawQueryPath = "$filter=age lt 30";
        UriInfo uriInfo = buildUriInfo(defaultMetadata, defaultOData, rawODataPath, rawQueryPath);
        ExpressionMember result = uriInfo.getFilterOption().getExpression()
                .accept(new ElasticSearchExpressionVisitor());
        String query = ((ExpressionResult) result).getQueryBuilder().toString();
        checkFilterRangeQuery(query, "lt", "age", "30");
    }

    @Test
    public void visitBinaryOperator_le_CorrectESQuery() throws Exception {
        String rawODataPath = "/author";
        String rawQueryPath = "$filter=age le 30";
        UriInfo uriInfo = buildUriInfo(defaultMetadata, defaultOData, rawODataPath, rawQueryPath);
        ExpressionMember result = uriInfo.getFilterOption().getExpression()
                .accept(new ElasticSearchExpressionVisitor());
        String query = ((ExpressionResult) result).getQueryBuilder().toString();
        checkFilterRangeQuery(query, "le", "age", "30");
    }

    @Test
    public void visitBinaryOperator_eq_CorrectESQuery() throws Exception {
        String rawODataPath = "/author";
        String rawQueryPath = "$filter=name eq '30'";
        UriInfo uriInfo = buildUriInfo(defaultMetadata, defaultOData, rawODataPath, rawQueryPath);
        ExpressionMember result = uriInfo.getFilterOption().getExpression()
                .accept(new ElasticSearchExpressionVisitor());
        String query = ((ExpressionResult) result).getQueryBuilder().toString();
        checkFilterEqualsQuery(query, "name", "'30'");
    }

    @Test
    public void visitBinaryOperator_ne_CorrectESQuery() throws Exception {
        String rawODataPath = "/author";
        String rawQueryPath = "$filter=name ne '30'";
        UriInfo uriInfo = buildUriInfo(defaultMetadata, defaultOData, rawODataPath, rawQueryPath);
        ExpressionMember result = uriInfo.getFilterOption().getExpression()
                .accept(new ElasticSearchExpressionVisitor());
        String query = ((ExpressionResult) result).getQueryBuilder().toString();
        checkFilterNotEqualsQuery(query, "name", "'30'");
    }

    @Test
    public void visitBinaryOperator_and_CorrectESQuery() throws Exception {
        String rawODataPath = "/author";
        String rawQueryPath = "$filter=name eq '30' and age gt 30";
        UriInfo uriInfo = buildUriInfo(defaultMetadata, defaultOData, rawODataPath, rawQueryPath);
        ExpressionMember result = uriInfo.getFilterOption().getExpression()
                .accept(new ElasticSearchExpressionVisitor());
        String query = ((ExpressionResult) result).getQueryBuilder().toString();

        JSONObject queryObj = new JSONObject(query);
        JSONObject rootObj = queryObj.getJSONObject("bool");
        JSONArray mustArr = rootObj.getJSONArray("must");
        JSONObject leftMatchAll = ((JSONObject) mustArr.get(0)).getJSONObject("term");
        JSONObject rightMatchAll = ((JSONObject) mustArr.get(1)).getJSONObject("range");
        assertNotNull(leftMatchAll);
        assertNotNull(rightMatchAll);
    }

    @Test(expected = ODataApplicationException.class)
    public void visitBinaryOperator_has_notImplementedException() throws Exception {
        new ElasticSearchExpressionVisitor().visitBinaryOperator(BinaryOperatorKind.HAS, null,
                null);
    }

    @Test
    public void visitBinaryOperator_or_CorrectESQuery() throws Exception {
        String rawODataPath = "/author";
        String rawQueryPath = "$filter=name eq '30' or age gt 30";
        UriInfo uriInfo = buildUriInfo(defaultMetadata, defaultOData, rawODataPath, rawQueryPath);
        ExpressionMember result = uriInfo.getFilterOption().getExpression()
                .accept(new ElasticSearchExpressionVisitor());
        String query = ((ExpressionResult) result).getQueryBuilder().toString();

        JSONObject queryObj = new JSONObject(query);
        JSONObject rootObj = queryObj.getJSONObject("bool");
        JSONArray mustArr = rootObj.getJSONArray("should");
        JSONObject leftMatchAll = ((JSONObject) mustArr.get(0)).getJSONObject("term");
        JSONObject rightMatchAll = ((JSONObject) mustArr.get(1)).getJSONObject("range");
        assertNotNull(leftMatchAll);
        assertNotNull(rightMatchAll);
    }

    @Test
    public void visitUnaryOperator_simpleNot_correctESQuery() throws Exception {
        String rawODataPath = "/author";
        String rawQueryPath = "$filter=not (name eq '30')";
        UriInfo uriInfo = buildUriInfo(defaultMetadata, defaultOData, rawODataPath, rawQueryPath);
        ExpressionMember result = uriInfo.getFilterOption().getExpression()
                .accept(new ElasticSearchExpressionVisitor());
        String query = ((ExpressionResult) result).getQueryBuilder().toString();

        JSONObject queryObj = new JSONObject(query);
        JSONObject rootObj = queryObj.getJSONObject("bool");
        JSONArray mustArr = rootObj.getJSONArray("must_not");
        JSONObject matchAllObj = ((JSONObject) mustArr.get(0)).getJSONObject("term");
        assertNotNull(matchAllObj);
    }

    @Test
    public void visitMember_lambdaAny_correctESQuery() throws Exception {
        String rawODataPath = "/author";
        String rawQueryPath = "$filter=book/any(b:b/title eq 'name')";
        UriInfo uriInfo = buildUriInfo(defaultMetadata, defaultOData, rawODataPath, rawQueryPath);
        ExpressionMember result = uriInfo.getFilterOption().getExpression()
                .accept(new ElasticSearchExpressionVisitor());
        String query = ((ExpressionResult) result).getQueryBuilder().toString();

        JSONObject queryObj = new JSONObject(query);
        JSONObject rootObj = queryObj.getJSONObject("has_child");
        String actualType = (String) rootObj.get("type");
        JSONObject queryObject = rootObj.getJSONObject("query");
        JSONObject matchAll = queryObject.getJSONObject("term");
        assertNotNull(matchAll);
        assertEquals("book", actualType);
    }

    @Test(expected = ODataApplicationException.class)
    public void visitMember_lambdaAll_notImplementedException() throws Exception {
        String rawODataPath = "/author";
        String rawQueryPath = "$filter=book/all(b:b/title eq 'name')";
        UriInfo uriInfo = buildUriInfo(defaultMetadata, defaultOData, rawODataPath, rawQueryPath);
        uriInfo.getFilterOption().getExpression().accept(new ElasticSearchExpressionVisitor());
    }

    @Test
    public void visitMember_lambdaAnyByComplexType_correctESQuery() throws Exception {
        String rawODataPath = "/author";
        String rawQueryPath = "$filter=_dimension/any(d:d/name eq 'validity')";
        UriInfo uriInfo = buildUriInfo(defaultMetadata, defaultOData, rawODataPath, rawQueryPath);
        ExpressionMember result = uriInfo.getFilterOption().getExpression()
                .accept(new ElasticSearchExpressionVisitor());
        String query = ((ExpressionResult) result).getQueryBuilder().toString();

        JSONObject queryObj = new JSONObject(query);
        JSONObject rootObj = queryObj.getJSONObject("nested");
        String actualType = (String) rootObj.get("path");
        JSONObject queryObject = rootObj.getJSONObject("query");
        JSONObject term = queryObject.getJSONObject("term");
        JSONObject termValue = term.getJSONObject("_dimension.name");
        assertNotNull(termValue);
        assertEquals("_dimension", actualType);
    }

    @Test
    public void visitMethodCall_endsWith_CorrectEsQuery() throws Exception {
        String rawODataPath = "/author";
        String rawQueryPath = "$filter=endswith(name,'on')";
        UriInfo uriInfo = buildUriInfo(defaultMetadata, defaultOData, rawODataPath, rawQueryPath);
        ExpressionMember result = uriInfo.getFilterOption().getExpression()
                .accept(new ElasticSearchExpressionVisitor());
        String query = ((ExpressionResult) result).getQueryBuilder().toString();

        JSONObject queryObj = new JSONObject(query);
        JSONObject rootObj = queryObj.getJSONObject("wildcard");
        JSONObject queryObject = rootObj.getJSONObject("name.keyword");
        String value = (String) queryObject.get("wildcard");
        assertEquals("*on", value);
    }

    @Test
    public void visitMethodCall_startsWith_CorrectEsQuery() throws Exception {
        String rawODataPath = "/author";
        String rawQueryPath = "$filter=startswith(name,'j')";
        UriInfo uriInfo = buildUriInfo(defaultMetadata, defaultOData, rawODataPath, rawQueryPath);
        ExpressionMember result = uriInfo.getFilterOption().getExpression()
                .accept(new ElasticSearchExpressionVisitor());
        String query = ((ExpressionResult) result).getQueryBuilder().toString();

        JSONObject queryObj = new JSONObject(query);
        JSONObject rootObj = queryObj.getJSONObject("prefix");
        JSONObject queryObject = rootObj.getJSONObject("name.keyword");
        String value = (String) queryObject.get("value");
        assertEquals("j", value);
    }

    @Test
    public void visitMethodCall_contains_CorrectEsQuery() throws Exception {
        String rawODataPath = "/author";
        String rawQueryPath = "$filter=contains(name,'j')";
        UriInfo uriInfo = buildUriInfo(defaultMetadata, defaultOData, rawODataPath, rawQueryPath);
        ExpressionMember result = uriInfo.getFilterOption().getExpression()
                .accept(new ElasticSearchExpressionVisitor());
        String query = ((ExpressionResult) result).getQueryBuilder().toString();

        JSONObject queryObj = new JSONObject(query);
        JSONObject rootObj = queryObj.getJSONObject("wildcard");
        JSONObject queryObject = rootObj.getJSONObject("name.keyword");
        String value = (String) queryObject.get("wildcard");
        assertEquals(ElasticConstants.WILDCARD_CHAR + "j" + ElasticConstants.WILDCARD_CHAR, value);
    }

    @Test(expected = ODataApplicationException.class)
    public void visitMethodCall_length_NotImplemented() throws Exception {
        String rawODataPath = "/author";
        String rawQueryPath = "$filter=length(name) eq 19";
        UriInfo uriInfo = buildUriInfo(defaultMetadata, defaultOData, rawODataPath, rawQueryPath);
        uriInfo.getFilterOption().getExpression().accept(new ElasticSearchExpressionVisitor());
    }

    @Test
    public void visitMethodCall_date_CorrectEsQuery() throws Exception {
        String rawODataPath = "/author";
        String rawQueryPath = "$filter=date(birthDate) eq 2016-02-14";
        UriInfo uriInfo = buildUriInfo(defaultMetadata, defaultOData, rawODataPath, rawQueryPath);
        ExpressionMember result = uriInfo.getFilterOption().getExpression()
                .accept(new ElasticSearchExpressionVisitor());
        String query = ((ExpressionResult) result).getQueryBuilder().toString();

        JSONObject queryObj = new JSONObject(query);
        JSONObject rootObj = queryObj.getJSONObject("term");
        JSONObject queryObject = rootObj.getJSONObject("birthDate");
        String value = (String) queryObject.get("value");
        assertEquals("2016-02-14", value);
    }

    @Test
    public void visitMember_ParentsProperty_correctESQuery() throws Exception {
        String rawODataPath = "/book";
        String rawQueryPath = "$filter=author/name eq 'Dawkins'";
        UriInfo uriInfo = buildUriInfo(defaultMetadata, defaultOData, rawODataPath, rawQueryPath);
        ExpressionMember result = uriInfo.getFilterOption().getExpression()
                .accept(new ElasticSearchExpressionVisitor());
        String query = ((ExpressionResult) result).getQueryBuilder().toString();

        checkFilterParentEqualsQuery(query, "author", "name", "'Dawkins'");
    }

    @Test
    public void visitLambdaExpression_anyExpression_null() throws Exception {
        assertNull(new ElasticSearchExpressionVisitor().visitLambdaExpression(null, null, null));
    }

    @Test(expected = ODataApplicationException.class)
    public void visitAlias_anyAlias_notImplementedException() throws Exception {
        new ElasticSearchExpressionVisitor().visitAlias(null);
    }

    @Test(expected = ODataApplicationException.class)
    public void visitTypeLiteral_anyType_notImplementedException() throws Exception {
        new ElasticSearchExpressionVisitor().visitTypeLiteral(null);
    }

    @Test(expected = ODataApplicationException.class)
    public void visitLambdaReference_anyLambdaReference_notImplementedException() throws Exception {
        new ElasticSearchExpressionVisitor().visitLambdaReference(null);
    }

    @Test(expected = ODataApplicationException.class)
    public void visitEnum_anyEnum_notImplementedException() throws Exception {
        new ElasticSearchExpressionVisitor().visitEnum(null, null);
    }

}