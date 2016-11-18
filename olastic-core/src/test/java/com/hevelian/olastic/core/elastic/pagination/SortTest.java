package com.hevelian.olastic.core.elastic.pagination;

import static org.junit.Assert.*;

import org.junit.Test;

/**
 * JUnit test for {@link Sort} class.
 * 
 * @author rdidyk
 */
public class SortTest {

    @Test
    public void constructor_Property_SettedAndDefaultDir() {
        Sort sort = new Sort("name");
        assertEquals("name", sort.getProperty());
        assertEquals(Sort.Direction.ASC, sort.getDirection());
    }

    @Test
    public void constructor_PropertyAndDir_AllValuesSetted() {
        Sort sort = new Sort("name", Sort.Direction.DESC);
        assertEquals("name", sort.getProperty());
        assertEquals(Sort.Direction.DESC, sort.getDirection());
    }

    @Test
    public void setters_PropertyAndDir_AllValuesSetted() {
        Sort sort = new Sort(null);
        assertNull(sort.getProperty());
        sort.setProperty("id");
        assertEquals("id", sort.getProperty());
        sort.setDirection(Sort.Direction.DESC);
        assertEquals(Sort.Direction.DESC, sort.getDirection());
    }

    @Test(expected = IllegalArgumentException.class)
    public void fromString_Null_IAEThrown() {
        Sort.Direction.fromString(null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void fromString_EmptyValue_IAEThrown() {
        Sort.Direction.fromString("");
    }

    @Test(expected = IllegalArgumentException.class)
    public void fromString_IllegalValue_IAEThrown() {
        Sort.Direction.fromString("illegal");
    }

    @Test
    public void fromString_CorrectValuee_EnumRetrieved() {
        assertEquals(Sort.Direction.ASC, Sort.Direction.fromString("asc"));
        assertEquals(Sort.Direction.ASC, Sort.Direction.fromString("ASC"));
        assertEquals(Sort.Direction.ASC, Sort.Direction.fromString("aSc"));
        assertEquals(Sort.Direction.DESC, Sort.Direction.fromString("desc"));
        assertEquals(Sort.Direction.DESC, Sort.Direction.fromString("DESC"));
        assertEquals(Sort.Direction.DESC, Sort.Direction.fromString("dESc"));
    }
}