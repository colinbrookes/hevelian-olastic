package com.hevelian.olastic.core.api.uri.queryoption.expression.member.impl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import org.apache.olingo.server.api.ODataApplicationException;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.Test;

/**
 * Tests for {@link ExpressionResult} class.
 * 
 * @author Taras Kohut
 */
public class ExpressionResultTest {
    private QueryBuilder matchAllQuery = QueryBuilders.matchAllQuery();

    @Test
    public void getQueryBuilder() throws Exception {
        ExpressionResult result = new ExpressionResult(matchAllQuery);
        assertEquals(matchAllQuery, result.getQueryBuilder());
    }

    @Test
    public void and_ResultQuery_QueryIsCorrect() throws Exception {
        ExpressionResult left = new ExpressionResult(matchAllQuery);
        ExpressionResult right = new ExpressionResult(matchAllQuery);
        ExpressionResult result = left.and(right);

        JSONObject queryObj = new JSONObject(result.getQueryBuilder().toString());
        JSONObject rootObj = queryObj.getJSONObject("bool");
        JSONArray mustArr = rootObj.getJSONArray("must");
        JSONObject leftMatchAll = ((JSONObject) mustArr.get(0)).getJSONObject("match_all");
        JSONObject rightMatchAll = ((JSONObject) mustArr.get(1)).getJSONObject("match_all");
        assertNotNull(leftMatchAll);
        assertNotNull(rightMatchAll);
    }

    @Test
    public void or_ResultQuery_QueryIsCorrect() throws Exception {
        ExpressionResult left = new ExpressionResult(matchAllQuery);
        ExpressionResult right = new ExpressionResult(matchAllQuery);
        ExpressionResult result = left.or(right);

        JSONObject queryObj = new JSONObject(result.getQueryBuilder().toString());
        JSONObject rootObj = queryObj.getJSONObject("bool");
        JSONArray mustArr = rootObj.getJSONArray("should");
        JSONObject leftMatchAll = ((JSONObject) mustArr.get(0)).getJSONObject("match_all");
        JSONObject rightMatchAll = ((JSONObject) mustArr.get(1)).getJSONObject("match_all");
        assertNotNull(leftMatchAll);
        assertNotNull(rightMatchAll);
    }

    @Test
    public void not_ResultQuery_QueryIsCorrect() throws Exception {
        ExpressionResult matchAll = new ExpressionResult(matchAllQuery);
        ExpressionResult result = matchAll.not();

        JSONObject queryObj = new JSONObject(result.getQueryBuilder().toString());
        JSONObject rootObj = queryObj.getJSONObject("bool");
        JSONArray mustArr = rootObj.getJSONArray("must_not");
        JSONObject matchAllObj = ((JSONObject) mustArr.get(0)).getJSONObject("match_all");
        assertNotNull(matchAllObj);
    }

    @Test(expected = ODataApplicationException.class)
    public void eq_ExceptionIsThrown() throws ODataApplicationException {
        new ExpressionResult(matchAllQuery).eq(null);
    }

    @Test(expected = ODataApplicationException.class)
    public void ne_ExceptionIsThrown() throws ODataApplicationException {
        new ExpressionResult(matchAllQuery).ne(null);
    }

    @Test(expected = ODataApplicationException.class)
    public void le_ExceptionIsThrown() throws ODataApplicationException {
        new ExpressionResult(matchAllQuery).le(null);
    }

    @Test(expected = ODataApplicationException.class)
    public void lt_ExceptionIsThrown() throws ODataApplicationException {
        new ExpressionResult(matchAllQuery).lt(null);
    }

    @Test(expected = ODataApplicationException.class)
    public void ge_ExceptionIsThrown() throws ODataApplicationException {
        new ExpressionResult(matchAllQuery).ge(null);
    }

    @Test(expected = ODataApplicationException.class)
    public void gt_ExceptionIsThrown() throws ODataApplicationException {
        new ExpressionResult(matchAllQuery).gt(null);
    }

    @Test(expected = ODataApplicationException.class)
    public void all_ExceptionIsThrown() throws ODataApplicationException {
        new ExpressionResult(matchAllQuery).all();
    }

    @Test(expected = ODataApplicationException.class)
    public void any_ExceptionIsThrown() throws ODataApplicationException {
        new ExpressionResult(matchAllQuery).any();
    }

    @Test(expected = ODataApplicationException.class)
    public void contains_ExceptionIsThrown() throws ODataApplicationException {
        new ExpressionResult(matchAllQuery).contains(null);
    }

    @Test(expected = ODataApplicationException.class)
    public void startsWith_ExceptionIsThrown() throws ODataApplicationException {
        new ExpressionResult(matchAllQuery).startsWith(null);
    }

    @Test(expected = ODataApplicationException.class)
    public void endsWith_ExceptionIsThrown() throws ODataApplicationException {
        new ExpressionResult(matchAllQuery).endsWith(null);
    }

    @Test(expected = ODataApplicationException.class)
    public void date_ExceptionIsThrown() throws ODataApplicationException {
        new ExpressionResult(matchAllQuery).date();
    }
}