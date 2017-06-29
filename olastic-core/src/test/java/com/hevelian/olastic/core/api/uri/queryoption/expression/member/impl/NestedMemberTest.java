package com.hevelian.olastic.core.api.uri.queryoption.expression.member.impl;

import org.apache.olingo.server.api.ODataApplicationException;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.json.JSONObject;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Tests for {@link NestedMember} class.
 * @author Taras Kohut
 */
public class NestedMemberTest {

    String type = "someType";
    QueryBuilder query = QueryBuilders.matchAllQuery();

    @Test
    public void any_MatchAllQuery_TypeAndQueryAreCorrect() throws Exception {
        NestedMember nestedMember = new NestedMember(type, query);
        ExpressionResult result = nestedMember.any();

        JSONObject queryObj = new JSONObject(result.getQueryBuilder().toString());
        JSONObject rootObj = queryObj.getJSONObject("nested");
        String actualType = (String)rootObj.get("path");
        JSONObject queryObject = rootObj.getJSONObject("query");
        JSONObject matchAll = queryObject.getJSONObject("match_all");
        assertNotNull(matchAll);
        assertEquals(type, actualType);
    }

    @Test(expected = ODataApplicationException.class)
    public void eq_ExceptionIsThrown() throws ODataApplicationException {
        new NestedMember(type, query).eq(null);
    }

    @Test(expected = ODataApplicationException.class)
    public void ne_ExceptionIsThrown() throws ODataApplicationException {
        new NestedMember(type, query).ne(null);
    }

    @Test(expected = ODataApplicationException.class)
    public void le_ExceptionIsThrown() throws ODataApplicationException {
        new NestedMember(type, query).le(null);
    }

    @Test(expected = ODataApplicationException.class)
    public void lt_ExceptionIsThrown() throws ODataApplicationException {
        new NestedMember(type, query).lt(null);
    }

    @Test(expected = ODataApplicationException.class)
    public void ge_ExceptionIsThrown() throws ODataApplicationException {
        new NestedMember(type, query).ge(null);
    }

    @Test(expected = ODataApplicationException.class)
    public void gt_ExceptionIsThrown() throws ODataApplicationException {
        new NestedMember(type, query).gt(null);
    }

    @Test(expected = ODataApplicationException.class)
    public void all_ExceptionIsThrown() throws ODataApplicationException {
        new NestedMember(type, query).all();
    }

    @Test(expected = ODataApplicationException.class)
    public void contains_ExceptionIsThrown() throws ODataApplicationException {
        new NestedMember(type, query).contains(null);
    }

    @Test(expected = ODataApplicationException.class)
    public void startsWith_ExceptionIsThrown() throws ODataApplicationException {
        new NestedMember(type, query).startsWith(null);
    }

    @Test(expected = ODataApplicationException.class)
    public void endsWith_ExceptionIsThrown() throws ODataApplicationException {
        new NestedMember(type, query).endsWith(null);
    }

    @Test(expected = ODataApplicationException.class)
    public void date_ExceptionIsThrown() throws ODataApplicationException {
        new NestedMember(type, query).date();
    }

    @Test(expected = ODataApplicationException.class)
    public void and_ExceptionIsThrown() throws ODataApplicationException {
        new NestedMember(type, query).and(null);
    }

    @Test(expected = ODataApplicationException.class)
    public void or_ExceptionIsThrown() throws ODataApplicationException {
        new NestedMember(type, query).or(null);
    }

    @Test(expected = ODataApplicationException.class)
    public void not_ExceptionIsThrown() throws ODataApplicationException {
        new NestedMember(type, query).not();
    }
}