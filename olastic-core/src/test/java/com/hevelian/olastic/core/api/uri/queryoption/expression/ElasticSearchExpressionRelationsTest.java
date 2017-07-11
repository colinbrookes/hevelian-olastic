package com.hevelian.olastic.core.api.uri.queryoption.expression;

import com.hevelian.olastic.core.api.uri.queryoption.expression.member.ExpressionMember;
import com.hevelian.olastic.core.api.uri.queryoption.expression.member.impl.ExpressionResult;
import org.apache.olingo.server.api.ODataApplicationException;
import org.apache.olingo.server.api.uri.UriInfo;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.Test;

import static com.hevelian.olastic.core.TestUtils.checkFilterGrandParentEqualsQuery;
import static com.hevelian.olastic.core.TestUtils.checkFilterParentEqualsQuery;
import static com.hevelian.olastic.core.TestUtils.checkFilterGrandChildEqualsQuery;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * @author Taras Kohut
 */
public class ElasticSearchExpressionRelationsTest extends ElasticSearchExpressionVisitorTest {
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

    @Test
    public void visitMember_lambdaAnyContains_correctESQuery() throws Exception {
        String rawODataPath = "/author";
        String rawQueryPath = "$filter=book/any(b:b/character/any(c:contains(c/name,'name')))";
        UriInfo uriInfo = buildUriInfo(defaultMetadata, defaultOData, rawODataPath, rawQueryPath);
        ExpressionMember result = uriInfo.getFilterOption().getExpression()
                .accept(new ElasticSearchExpressionVisitor());
        String query = ((ExpressionResult) result).getQueryBuilder().toString();

        JSONObject queryObj = new JSONObject(query);
        JSONObject rootObj = queryObj.getJSONObject("has_child");
        String bookType = (String) rootObj.get("type");
        assertEquals("book", bookType);
        JSONObject characterObj = rootObj.getJSONObject("query").getJSONObject("has_child");
        String characterType = (String) characterObj.get("type");
        assertEquals("character", characterType);
        JSONObject queryObject = characterObj.getJSONObject("query");
        JSONObject wildcard = queryObject.getJSONObject("wildcard");
        assertEquals("*name*", wildcard.getJSONObject("name.keyword").getString("wildcard"));
        assertNotNull(wildcard);
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
    public void visitMember_lambdaAnyByNestedComplexType_correctESQuery() throws Exception {
        String rawODataPath = "/book";
        String rawQueryPath = "$filter=info/pages/any(p:p/words/any(w:w eq 'w') and p/pageName eq 'page name')";
        UriInfo uriInfo = buildUriInfo(defaultMetadata, defaultOData, rawODataPath, rawQueryPath);
        ExpressionMember result = uriInfo.getFilterOption().getExpression()
                .accept(new ElasticSearchExpressionVisitor());
        String query = ((ExpressionResult) result).getQueryBuilder().toString();

        JSONObject queryObj = new JSONObject(query);
        JSONObject rootObj = queryObj.getJSONObject("nested");
        String pagesPath = (String) rootObj.get("path");
        JSONObject pagesQueryObject = rootObj.getJSONObject("query");
        JSONArray mustQueryObj = pagesQueryObject.getJSONObject("bool").getJSONArray("must");
        JSONObject wordsTerm = ((JSONObject) mustQueryObj.get(0)).getJSONObject("term");
        JSONObject pageNameTerm = ((JSONObject) mustQueryObj.get(1)).getJSONObject("term");
        String wordsTermValue = wordsTerm.getJSONObject("info.pages.words").getString("value");
        String pageTermValue = pageNameTerm.getJSONObject("info.pages.pageName").getString("value");

        assertEquals("info.pages", pagesPath);
        assertEquals("w", wordsTermValue);
        assertEquals("page name", pageTermValue);
    }

    @Test
    public void visitMember_lambdaAnyByNestedComplexTypeAnalyzed_correctESQuery() throws Exception {
        String rawODataPath = "/book";
        String rawQueryPath = "$filter=info/pages/any(p:p/analyzedWords/any(w:w eq 'w') and p/analyzedPageName eq 'page name' or p/pageNumber eq 5)";
        UriInfo uriInfo = buildUriInfo(defaultMetadata, defaultOData, rawODataPath, rawQueryPath);
        ExpressionMember result = uriInfo.getFilterOption().getExpression()
                .accept(new ElasticSearchExpressionVisitor());
        String query = ((ExpressionResult) result).getQueryBuilder().toString();

        JSONObject queryObj = new JSONObject(query);
        JSONObject rootObj = queryObj.getJSONObject("nested");
        String pagesPath = (String) rootObj.get("path");
        JSONObject pagesQueryObject = rootObj.getJSONObject("query");
        JSONArray shouldQueryObj = pagesQueryObject.getJSONObject("bool").getJSONArray("should");
        JSONArray mustQueryObj = ((JSONObject) shouldQueryObj.get(0)).getJSONObject("bool")
                .getJSONArray("must");
        JSONObject wordsTerm = ((JSONObject) mustQueryObj.get(0)).getJSONObject("term");
        JSONObject pageNameTerm = ((JSONObject) mustQueryObj.get(1)).getJSONObject("term");
        JSONObject pageNumberTerm = ((JSONObject) shouldQueryObj.get(1)).getJSONObject("term");
        String wordsTermValue = wordsTerm.getJSONObject("info.pages.analyzedWords.keyword")
                .getString("value");
        String pageTermValue = pageNameTerm.getJSONObject("info.pages.analyzedPageName.keyword")
                .getString("value");
        int pageNumberValue = pageNumberTerm.getJSONObject("info.pages.pageNumber").getInt("value");

        assertEquals("info.pages", pagesPath);
        assertEquals("w", wordsTermValue);
        assertEquals("page name", pageTermValue);
        assertEquals(5, pageNumberValue);
    }

    @Test
    public void visitMember_lambdaAnyByNestedComplexTypeAnalyzedContains_correctESQuery()
            throws Exception {
        String rawODataPath = "/book";
        String rawQueryPath = "$filter=info/pages/any(p:p/analyzedWords/any(w:contains(w,'w')) and p/analyzedPageName eq 'page name' or p/pageNumber eq 5)";
        UriInfo uriInfo = buildUriInfo(defaultMetadata, defaultOData, rawODataPath, rawQueryPath);
        ExpressionMember result = uriInfo.getFilterOption().getExpression()
                .accept(new ElasticSearchExpressionVisitor());
        String query = ((ExpressionResult) result).getQueryBuilder().toString();

        JSONObject queryObj = new JSONObject(query);
        JSONObject rootObj = queryObj.getJSONObject("nested");
        String pagesPath = (String) rootObj.get("path");
        JSONObject pagesQueryObject = rootObj.getJSONObject("query");
        JSONArray shouldQueryObj = pagesQueryObject.getJSONObject("bool").getJSONArray("should");
        JSONArray mustQueryObj = ((JSONObject) shouldQueryObj.get(0)).getJSONObject("bool")
                .getJSONArray("must");
        JSONObject wordsTerm = ((JSONObject) mustQueryObj.get(0)).getJSONObject("wildcard");
        JSONObject pageNameTerm = ((JSONObject) mustQueryObj.get(1)).getJSONObject("term");
        JSONObject pageNumberTerm = ((JSONObject) shouldQueryObj.get(1)).getJSONObject("term");
        String wordsTermValue = wordsTerm.getJSONObject("info.pages.analyzedWords.keyword")
                .getString("wildcard");
        String pageTermValue = pageNameTerm.getJSONObject("info.pages.analyzedPageName.keyword")
                .getString("value");
        int pageNumberValue = pageNumberTerm.getJSONObject("info.pages.pageNumber").getInt("value");

        assertEquals("info.pages", pagesPath);
        assertEquals("*w*", wordsTermValue);
        assertEquals("page name", pageTermValue);
        assertEquals(5, pageNumberValue);
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
    public void visitMember_GrandChildProperty_correctESQuery() throws Exception {
        String rawODataPath = "/author";
        String rawQueryPath = "$filter=book/any(b:b/character/any(c:c/name eq 'Oliver Twist'))";
        UriInfo uriInfo = buildUriInfo(defaultMetadata, defaultOData, rawODataPath, rawQueryPath);
        ExpressionMember result = uriInfo.getFilterOption().getExpression()
                .accept(new ElasticSearchExpressionVisitor());
        String query = ((ExpressionResult) result).getQueryBuilder().toString();
        String value = "'Oliver Twist'";
        String field = "name";
        checkFilterGrandChildEqualsQuery(query, field, value, "book", "character");
    }

    @Test
    public void visitMember_GrandParentProperty_correctESQuery() throws Exception {
        String rawODataPath = "/character";
        String rawQueryPath = "$filter=book/author/name eq 'Duma'";
        UriInfo uriInfo = buildUriInfo(defaultMetadata, defaultOData, rawODataPath, rawQueryPath);
        ExpressionMember result = uriInfo.getFilterOption().getExpression()
                .accept(new ElasticSearchExpressionVisitor());
        String query = ((ExpressionResult) result).getQueryBuilder().toString();
        String value = "'Duma'";
        String field = "name";
        checkFilterGrandParentEqualsQuery(query, field, value, "book", "author", "term", "value");
    }

    @Test
    public void visitMember_GrandParentPropertyContains_correctESQuery() throws Exception {
        String rawODataPath = "/character";
        String rawQueryPath = "$filter=contains(book/author/name,'Du')";
        UriInfo uriInfo = buildUriInfo(defaultMetadata, defaultOData, rawODataPath, rawQueryPath);
        ExpressionMember result = uriInfo.getFilterOption().getExpression()
                .accept(new ElasticSearchExpressionVisitor());
        String query = ((ExpressionResult) result).getQueryBuilder().toString();
        String value = "'*Du*'";
        String field = "name";
        checkFilterGrandParentEqualsQuery(query, field, value, "book", "author", "wildcard",
                "wildcard");
    }

    @Test
    public void visitMember_GrandParentPropertyStartsWith_correctESQuery() throws Exception {
        String rawODataPath = "/character";
        String rawQueryPath = "$filter=startswith(book/author/name,'Du')";
        UriInfo uriInfo = buildUriInfo(defaultMetadata, defaultOData, rawODataPath, rawQueryPath);
        ExpressionMember result = uriInfo.getFilterOption().getExpression()
                .accept(new ElasticSearchExpressionVisitor());
        String query = ((ExpressionResult) result).getQueryBuilder().toString();
        String value = "'Du'";
        String field = "name";
        checkFilterGrandParentEqualsQuery(query, field, value, "book", "author", "prefix", "value");
    }

    @Test
    public void visitMember_GrandParentPropertyEndsWith_correctESQuery() throws Exception {
        String rawODataPath = "/character";
        String rawQueryPath = "$filter=endswith(book/author/name,'Du')";
        UriInfo uriInfo = buildUriInfo(defaultMetadata, defaultOData, rawODataPath, rawQueryPath);
        ExpressionMember result = uriInfo.getFilterOption().getExpression()
                .accept(new ElasticSearchExpressionVisitor());
        String query = ((ExpressionResult) result).getQueryBuilder().toString();
        String value = "'*Du'";
        String field = "name";
        checkFilterGrandParentEqualsQuery(query, field, value, "book", "author", "wildcard",
                "wildcard");
    }

    @Test
    public void visitMember_ParentLambdaAnyByComplexType_correctESQuery() throws Exception {
        String rawODataPath = "/book";
        String rawQueryPath = "$filter=author/_dimension/any(d:(d/name eq 'Validity' and d/state eq 'true'))";
        UriInfo uriInfo = buildUriInfo(defaultMetadata, defaultOData, rawODataPath, rawQueryPath);
        ExpressionMember result = uriInfo.getFilterOption().getExpression()
                .accept(new ElasticSearchExpressionVisitor());
        String query = ((ExpressionResult) result).getQueryBuilder().toString();

        JSONObject parentObj = new JSONObject(query).getJSONObject("has_parent");
        String actualType = (String) parentObj.get("parent_type");
        assertEquals("author", actualType);

        JSONObject rootObj = parentObj.getJSONObject("query").getJSONObject("nested");
        String actualNestedType = (String) rootObj.get("path");
        assertEquals("_dimension", actualNestedType);
        JSONObject queryObject = rootObj.getJSONObject("query");
        JSONArray must = queryObject.getJSONObject("bool").getJSONArray("must");
        JSONObject firstTermValue = must.getJSONObject(0).getJSONObject("term")
                .getJSONObject("_dimension.name");
        assertEquals("Validity", firstTermValue.get("value"));
        JSONObject secondTermValue = must.getJSONObject(1).getJSONObject("term")
                .getJSONObject("_dimension.state");
        assertEquals("true", secondTermValue.get("value"));
    }

    @Test
    public void visitMember_GrandLambdaAnyByComplexType_correctESQuery() throws Exception {
        String rawODataPath = "/character";
        String rawQueryPath = "$filter=book/author/_dimension/any(d:(d/name eq 'Validity' and d/state eq 'true'))";
        UriInfo uriInfo = buildUriInfo(defaultMetadata, defaultOData, rawODataPath, rawQueryPath);
        ExpressionMember result = uriInfo.getFilterOption().getExpression()
                .accept(new ElasticSearchExpressionVisitor());
        String query = ((ExpressionResult) result).getQueryBuilder().toString();

        JSONObject parentObj = new JSONObject(query).getJSONObject("has_parent");
        String actualParentType = (String) parentObj.get("parent_type");
        assertEquals("book", actualParentType);

        JSONObject grandParentObj = parentObj.getJSONObject("query").getJSONObject("has_parent");
        String actualGrandParentType = (String) grandParentObj.get("parent_type");
        assertEquals("author", actualGrandParentType);

        JSONObject rootObj = grandParentObj.getJSONObject("query").getJSONObject("nested");
        String actualNestedType = (String) rootObj.get("path");
        assertEquals("_dimension", actualNestedType);
        JSONObject queryObject = rootObj.getJSONObject("query");
        JSONArray must = queryObject.getJSONObject("bool").getJSONArray("must");
        JSONObject firstTermValue = must.getJSONObject(0).getJSONObject("term")
                .getJSONObject("_dimension.name");
        assertEquals("Validity", firstTermValue.get("value"));
        JSONObject secondTermValue = must.getJSONObject(1).getJSONObject("term")
                .getJSONObject("_dimension.state");
        assertEquals("true", secondTermValue.get("value"));
    }

    @Test
    public void visitMember_ChildLambdaAnyByComplexType_correctESQuery() throws Exception {
        String rawODataPath = "/author";
        String rawQueryPath = "$filter=book/any(b:b/_dimension/any(d:(d/name eq 'Validity' and d/state eq 'true')))";
        UriInfo uriInfo = buildUriInfo(defaultMetadata, defaultOData, rawODataPath, rawQueryPath);
        ExpressionMember result = uriInfo.getFilterOption().getExpression()
                .accept(new ElasticSearchExpressionVisitor());
        String query = ((ExpressionResult) result).getQueryBuilder().toString();

        JSONObject childObj = new JSONObject(query).getJSONObject("has_child");
        String actualType = (String) childObj.get("type");
        assertEquals("book", actualType);

        JSONObject rootObj = childObj.getJSONObject("query").getJSONObject("nested");
        String actualNestedType = (String) rootObj.get("path");
        assertEquals("_dimension", actualNestedType);
        JSONObject queryObject = rootObj.getJSONObject("query");
        JSONArray must = queryObject.getJSONObject("bool").getJSONArray("must");
        JSONObject firstTermValue = must.getJSONObject(0).getJSONObject("term")
                .getJSONObject("_dimension.name");
        assertEquals("Validity", firstTermValue.get("value"));
        JSONObject secondTermValue = must.getJSONObject(1).getJSONObject("term")
                .getJSONObject("_dimension.state");
        assertEquals("true", secondTermValue.get("value"));
    }

    @Test
    public void visitMember_GrandChildLambdaAnyByComplexType_correctESQuery() throws Exception {
        String rawODataPath = "/author";
        String rawQueryPath = "$filter=book/any(b:b/character/any(c:c/_dimension/any(d:(d/name eq 'Validity' and d/state eq 'true'))))";
        UriInfo uriInfo = buildUriInfo(defaultMetadata, defaultOData, rawODataPath, rawQueryPath);
        ExpressionMember result = uriInfo.getFilterOption().getExpression()
                .accept(new ElasticSearchExpressionVisitor());
        String query = ((ExpressionResult) result).getQueryBuilder().toString();

        JSONObject childObj = new JSONObject(query).getJSONObject("has_child");
        String actualParentType = (String) childObj.get("type");
        assertEquals("book", actualParentType);

        JSONObject grandChildObj = childObj.getJSONObject("query").getJSONObject("has_child");
        String actualGrandParentType = (String) grandChildObj.get("type");
        assertEquals("character", actualGrandParentType);

        JSONObject rootObj = grandChildObj.getJSONObject("query").getJSONObject("nested");
        String actualNestedType = (String) rootObj.get("path");
        assertEquals("_dimension", actualNestedType);
        JSONObject queryObject = rootObj.getJSONObject("query");
        JSONArray must = queryObject.getJSONObject("bool").getJSONArray("must");
        JSONObject firstTermValue = must.getJSONObject(0).getJSONObject("term")
                .getJSONObject("_dimension.name");
        assertEquals("Validity", firstTermValue.get("value"));
        JSONObject secondTermValue = must.getJSONObject(1).getJSONObject("term")
                .getJSONObject("_dimension.state");
        assertEquals("true", secondTermValue.get("value"));
    }

}
