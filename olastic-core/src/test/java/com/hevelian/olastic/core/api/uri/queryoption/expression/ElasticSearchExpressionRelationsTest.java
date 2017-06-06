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
                ExpressionMember result = uriInfo.getFilterOption().getExpression().accept(new ElasticSearchExpressionVisitor());
                String query = ((ExpressionResult)result).getQueryBuilder().toString();

                JSONObject queryObj = new JSONObject(query);
                JSONObject rootObj = queryObj.getJSONObject("nested");
                String pagesPath = (String)rootObj.get("path");
                JSONObject pagesQueryObject = rootObj.getJSONObject("query");
                JSONArray mustQueryObj = pagesQueryObject.getJSONObject("bool").getJSONArray("must");
                JSONObject wordsTerm = ((JSONObject)mustQueryObj.get(0)).getJSONObject("term");
                JSONObject pageNameTerm = ((JSONObject)mustQueryObj.get(1)).getJSONObject("term");
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
                ExpressionMember result = uriInfo.getFilterOption().getExpression().accept(new ElasticSearchExpressionVisitor());
                String query = ((ExpressionResult)result).getQueryBuilder().toString();

                JSONObject queryObj = new JSONObject(query);
                JSONObject rootObj = queryObj.getJSONObject("nested");
                String pagesPath = (String)rootObj.get("path");
                JSONObject pagesQueryObject = rootObj.getJSONObject("query");
                JSONArray shouldQueryObj = pagesQueryObject.getJSONObject("bool").getJSONArray("should");
                JSONArray mustQueryObj = ((JSONObject)shouldQueryObj.get(0)).getJSONObject("bool").getJSONArray("must");
                JSONObject wordsTerm = ((JSONObject)mustQueryObj.get(0)).getJSONObject("term");
                JSONObject pageNameTerm = ((JSONObject)mustQueryObj.get(1)).getJSONObject("term");
                JSONObject pageNumberTerm = ((JSONObject)shouldQueryObj.get(1)).getJSONObject("term");
                String wordsTermValue = wordsTerm.getJSONObject("info.pages.analyzedWords.keyword").getString("value");
                String pageTermValue = pageNameTerm.getJSONObject("info.pages.analyzedPageName.keyword").getString("value");
                int pageNumberValue = pageNumberTerm.getJSONObject("info.pages.pageNumber").getInt("value");

                assertEquals("info.pages", pagesPath);
                assertEquals("w", wordsTermValue);
                assertEquals("page name", pageTermValue);
                assertEquals(5,  pageNumberValue);
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
                checkFilterGrandParentEqualsQuery(query, field, value, "book", "author");
        }

}
