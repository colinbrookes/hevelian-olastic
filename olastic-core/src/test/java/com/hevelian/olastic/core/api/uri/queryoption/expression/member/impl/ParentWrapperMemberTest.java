package com.hevelian.olastic.core.api.uri.queryoption.expression.member.impl;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.List;

import org.apache.olingo.server.api.ODataApplicationException;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.json.JSONObject;
import org.junit.Test;

/**
 * Tests for {@link ParentWrapperMember} class.
 * 
 * @author Ruslan Didyk
 */
public class ParentWrapperMemberTest {

    private List<String> parentTypes;
    private QueryBuilder query;

    public ParentWrapperMemberTest() throws ODataApplicationException {
        this.parentTypes = Arrays.asList("author");
        this.query = new NestedMember("_dimension", QueryBuilders.termQuery("name", "Validity"))
                .any().getQueryBuilder();
    }

    @Test
    public void any_NestedQuery_TypeAndQueryAreCorrect() throws Exception {
        ParentWrapperMember nestedMember = new ParentWrapperMember(parentTypes, query);
        ExpressionResult result = nestedMember.any();
        String query = result.getQueryBuilder().toString();

        JSONObject parentObj = new JSONObject(query).getJSONObject("has_parent");
        String actualType = (String) parentObj.get("parent_type");
        assertEquals("author", actualType);

        JSONObject rootObj = parentObj.getJSONObject("query").getJSONObject("nested");
        String actualNestedType = (String) rootObj.get("path");
        assertEquals("_dimension", actualNestedType);
        JSONObject queryObject = rootObj.getJSONObject("query");
        JSONObject termValue = queryObject.getJSONObject("term").getJSONObject("name");
        assertEquals("Validity", termValue.get("value"));
    }

    @Test(expected = ODataApplicationException.class)
    public void eq_ExceptionIsThrown() throws ODataApplicationException {
        new ParentWrapperMember(parentTypes, query).eq(null);
    }

    @Test(expected = ODataApplicationException.class)
    public void ne_ExceptionIsThrown() throws ODataApplicationException {
        new ParentWrapperMember(parentTypes, query).ne(null);
    }

    @Test(expected = ODataApplicationException.class)
    public void le_ExceptionIsThrown() throws ODataApplicationException {
        new ParentWrapperMember(parentTypes, query).le(null);
    }

    @Test(expected = ODataApplicationException.class)
    public void lt_ExceptionIsThrown() throws ODataApplicationException {
        new ParentWrapperMember(parentTypes, query).lt(null);
    }

    @Test(expected = ODataApplicationException.class)
    public void ge_ExceptionIsThrown() throws ODataApplicationException {
        new ParentWrapperMember(parentTypes, query).ge(null);
    }

    @Test(expected = ODataApplicationException.class)
    public void gt_ExceptionIsThrown() throws ODataApplicationException {
        new ParentWrapperMember(parentTypes, query).gt(null);
    }

    @Test(expected = ODataApplicationException.class)
    public void all_ExceptionIsThrown() throws ODataApplicationException {
        new ParentWrapperMember(parentTypes, query).all();
    }

    @Test(expected = ODataApplicationException.class)
    public void contains_ExceptionIsThrown() throws ODataApplicationException {
        new ParentWrapperMember(parentTypes, query).contains(null);
    }

    @Test(expected = ODataApplicationException.class)
    public void startsWith_ExceptionIsThrown() throws ODataApplicationException {
        new ParentWrapperMember(parentTypes, query).startsWith(null);
    }

    @Test(expected = ODataApplicationException.class)
    public void endsWith_ExceptionIsThrown() throws ODataApplicationException {
        new ParentWrapperMember(parentTypes, query).endsWith(null);
    }

    @Test(expected = ODataApplicationException.class)
    public void date_ExceptionIsThrown() throws ODataApplicationException {
        new ParentWrapperMember(parentTypes, query).date();
    }

    @Test(expected = ODataApplicationException.class)
    public void and_ExceptionIsThrown() throws ODataApplicationException {
        new ParentWrapperMember(parentTypes, query).and(null);
    }

    @Test(expected = ODataApplicationException.class)
    public void or_ExceptionIsThrown() throws ODataApplicationException {
        new ParentWrapperMember(parentTypes, query).or(null);
    }

    @Test(expected = ODataApplicationException.class)
    public void not_ExceptionIsThrown() throws ODataApplicationException {
        new ParentWrapperMember(parentTypes, query).not();
    }

}
