package com.hevelian.olastic.core;

import org.apache.olingo.server.api.ODataApplicationException;
import org.json.JSONArray;
import org.json.JSONObject;

import static org.junit.Assert.assertEquals;

/**
 * Test routines.
 * @author Taras Kohut
 */
public class TestUtils {
    /**
     * Checks if ES query for range filter is correct
     * @param query ESQuery
     * @param field  field name
     * @param expValue expected range value
     * @throws ODataApplicationException
     */
    public static void checkFilterRangeQuery(String query, String operator, String field,  Object expValue) throws ODataApplicationException {
        String rootKey = "range";
        boolean includeLower = !operator.equals("gt");
        boolean includeUpper = !operator.equals("lt");
        String valueKey = operator.startsWith("g") ? "from" : "to";
        JSONObject queryObj = new JSONObject(query);
        JSONObject rootObj = queryObj.getJSONObject(rootKey);
        JSONObject valueObject = rootObj.getJSONObject(field);
        String actualValue = (String)valueObject.get(valueKey);
        assertEquals(expValue, actualValue);
        assertEquals(valueObject.get("include_lower"), includeLower);
        assertEquals(valueObject.get("include_upper"), includeUpper);
    }

    /**
     * Checks if ES query for equals filter is correct
     * @param query ESQuery
     * @param field field name
     * @param value Expected value enclosed in ''
     */
    public static void checkFilterEqualsQuery(String query, String field, String value) {
        checkFilterEqualsQueryInternal(query, field, value, false);
    }
    /**
     * Checks if ES query for not equals filter is correct
     * @param query ESQuery
     * @param field field name
     * @param value Expected value enclosed in ''
     */
    public static void checkFilterNotEqualsQuery(String query, String field, String value) {
        checkFilterEqualsQueryInternal(query, field, value, true);
    }

    private static void checkFilterEqualsQueryInternal(String query, String field, String value, boolean isNotQuery) {
        JSONObject queryObj = new JSONObject(query);
        JSONObject rootObj;

        if (isNotQuery) {
            JSONObject bool =  queryObj.getJSONObject("bool");
            JSONArray array;
            array =  bool.getJSONArray("must_not");
            rootObj = ((JSONObject)array.get(0)).getJSONObject("term");
        } else {
            rootObj = queryObj.getJSONObject("term");
        }

        String fieldName = field + ".keyword";
        JSONObject valueObject = rootObj.getJSONObject(fieldName);
        String actualValue = (String)valueObject.get("value");
        assertEquals(value.substring(1,value.length()-1), actualValue);
    }

    /**
     * Checks if ES query for equals filter by parent's property is correct
     * @param query ESQuery
     * @param parent parent's type
     * @param field field name
     * @param value Expected value enclosed in ''
     */
    public static void checkFilterParentEqualsQuery(String query, String parent, String field, String value) {
        checkFilterParentEqualsQueryInternal(query, parent, field, value, false);
    }

    /**
     * Checks if ES query for not equals filter by parent's property is correct
     * @param query ESQuery
     * @param parent parent's type
     * @param field field name
     * @param value Expected value enclosed in ''
     */
    public static void checkFilterParentNotEqualsQuery(String query, String parent, String field, String value) {
        checkFilterParentEqualsQueryInternal(query, parent, field, value, true);
    }

    private static void checkFilterParentEqualsQueryInternal(String query, String parent, String field, String value, boolean isNotQuery) {
        JSONObject parentObj = new JSONObject(query).getJSONObject("has_parent");
        String actualType = (String)parentObj.get("parent_type");
        assertEquals(parent, actualType);
        JSONObject rootObj;
        if (isNotQuery) {
            JSONObject bool =  parentObj.getJSONObject("query").getJSONObject("bool");
            JSONArray array;
            array =  bool.getJSONArray("must_not");
            rootObj = ((JSONObject)array.get(0)).getJSONObject("term");
        } else {
            rootObj = parentObj.getJSONObject("query").getJSONObject("term");
        }

        String fieldName = field + ".keyword";
        JSONObject valueObject = rootObj.getJSONObject(fieldName);
        String actualValue = (String)valueObject.get("value");
        assertEquals(value.substring(1,value.length()-1), actualValue);
    }
}
