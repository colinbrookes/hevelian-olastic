package com.hevelian.olastic.core.edm;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import java.util.Calendar;
import java.util.GregorianCalendar;

import org.apache.olingo.commons.api.data.Property;
import org.apache.olingo.commons.api.data.ValueType;
import org.apache.olingo.commons.api.edm.EdmElement;
import org.apache.olingo.commons.api.edm.EdmType;
import org.apache.olingo.commons.core.edm.primitivetype.EdmDate;
import org.apache.olingo.commons.core.edm.primitivetype.EdmDateTimeOffset;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

/**
 * JUnit test for {@link PropertyCreator} class.
 * 
 * @author rdidyk
 */
@RunWith(MockitoJUnitRunner.class)
public class PropertyCreatorTest {

    private static final String DATE_OF_BIRTH_PROPERTY = "dateOfBirth";

    @Mock
    private ElasticEdmEntityType entityType;

    private PropertyCreator creator;

    @Before
    public void setUpBefore() {
        creator = new PropertyCreator();
    }

    @Test
    public void createProperty_DateFieldsAndNullValue_PropertyWithNullValueCreated() {
        doReturn(getTypedProperty(EdmDateTimeOffset.getInstance())).when(entityType)
                .getProperty(DATE_OF_BIRTH_PROPERTY);
        Property property = creator.createProperty(DATE_OF_BIRTH_PROPERTY, null, entityType);
        assertEquals(DATE_OF_BIRTH_PROPERTY, property.getName());
        assertEquals(ValueType.PRIMITIVE, property.getValueType());
        assertNull(property.getValue());

        doReturn(getTypedProperty(EdmDate.getInstance())).when(entityType)
                .getProperty(DATE_OF_BIRTH_PROPERTY);
        property = creator.createProperty(DATE_OF_BIRTH_PROPERTY, null, entityType);
        assertEquals(DATE_OF_BIRTH_PROPERTY, property.getName());
        assertEquals(ValueType.PRIMITIVE, property.getValueType());
        assertNull(property.getValue());
    }

    @Test
    public void createProperty_DateTimeOffsetFieldAndDateAsString_PropertyWithCalendarCreated() {
        doReturn(getTypedProperty(EdmDateTimeOffset.getInstance())).when(entityType)
                .getProperty(DATE_OF_BIRTH_PROPERTY);
        Property property = creator.createProperty(DATE_OF_BIRTH_PROPERTY,
                "1982-05-24T10:25:15.777Z", entityType);
        assertEquals(DATE_OF_BIRTH_PROPERTY, property.getName());
        assertEquals(ValueType.PRIMITIVE, property.getValueType());
        Object calendar = property.getValue();
        assertTrue(calendar instanceof GregorianCalendar);
        assertEquealsToCalendar(1982, 4, 24, 10, 25, 15, 777, (GregorianCalendar) calendar);

        // Case where date less than 1582 year
        property = creator.createProperty(DATE_OF_BIRTH_PROPERTY, "1420-07-09T16:55:01.102Z",
                entityType);
        calendar = property.getValue();
        assertTrue(calendar instanceof GregorianCalendar);
        assertEquealsToCalendar(1420, 6, 9, 16, 55, 1, 102, (GregorianCalendar) calendar);
    }

    @Test
    public void createProperty_DateFieldAndDateAsString_PropertyWithCalendarCreated() {
        doReturn(getTypedProperty(EdmDate.getInstance())).when(entityType)
                .getProperty(DATE_OF_BIRTH_PROPERTY);
        Property property = creator.createProperty(DATE_OF_BIRTH_PROPERTY,
                "1992-06-24T00:00:00.000Z", entityType);
        assertEquals(DATE_OF_BIRTH_PROPERTY, property.getName());
        assertEquals(ValueType.PRIMITIVE, property.getValueType());
        Object calendar = property.getValue();
        assertTrue(calendar instanceof GregorianCalendar);
        assertEquealsToCalendar(1992, 5, 24, 0, 0, 0, 0, (GregorianCalendar) calendar);

        // Case where date less than 1582 year
        property = creator.createProperty(DATE_OF_BIRTH_PROPERTY, "1256-05-01T00:00:00.000Z",
                entityType);
        calendar = property.getValue();
        assertTrue(calendar instanceof GregorianCalendar);
        assertEquealsToCalendar(1256, 4, 1, 0, 0, 0, 0, (GregorianCalendar) calendar);
    }

    @Test
    public void createProperty_DateTimeOffsetFieldAndDateAsTimestamp_PropertyWithCalendarCreated() {
        doReturn(getTypedProperty(EdmDateTimeOffset.getInstance())).when(entityType)
                .getProperty(DATE_OF_BIRTH_PROPERTY);
        Property property = creator.createProperty(DATE_OF_BIRTH_PROPERTY, 1064231665584L,
                entityType);
        assertEquals(DATE_OF_BIRTH_PROPERTY, property.getName());
        assertEquals(ValueType.PRIMITIVE, property.getValueType());
        Object calendar = property.getValue();
        assertTrue(calendar instanceof GregorianCalendar);
        assertEquealsToCalendar(2003, 8, 22, 11, 54, 25, 584, (GregorianCalendar) calendar);

        // Case where date less than 1582 year
        property = creator.createProperty(DATE_OF_BIRTH_PROPERTY, -19064231665584L, entityType);
        calendar = property.getValue();
        assertTrue(calendar instanceof GregorianCalendar);
        assertEquealsToCalendar(1365, 10, 17, 4, 5, 34, 416, (GregorianCalendar) calendar);
    }

    @Test
    public void createProperty_DateTimeFieldAndDateAsTimestamp_PropertyWithCalendarCreated() {
        doReturn(getTypedProperty(EdmDateTimeOffset.getInstance())).when(entityType)
                .getProperty(DATE_OF_BIRTH_PROPERTY);
        Property property = creator.createProperty(DATE_OF_BIRTH_PROPERTY, 683596800000L,
                entityType);
        assertEquals(DATE_OF_BIRTH_PROPERTY, property.getName());
        assertEquals(ValueType.PRIMITIVE, property.getValueType());
        Object calendar = property.getValue();
        assertTrue(calendar instanceof GregorianCalendar);
        assertEquealsToCalendar(1991, 7, 31, 0, 0, 0, 0, (GregorianCalendar) calendar);

        // Case where date less than 1582 year
        property = creator.createProperty(DATE_OF_BIRTH_PROPERTY, -27377049600000L, entityType);
        calendar = property.getValue();
        assertTrue(calendar instanceof GregorianCalendar);
        assertEquealsToCalendar(1102, 5, 17, 0, 0, 0, 0, (GregorianCalendar) calendar);
    }

    private static void assertEquealsToCalendar(int year, int mounth, int day, int hour, int minute,
            int second, int millisecond, GregorianCalendar calendar) {
        assertEquals(year, calendar.get(Calendar.YEAR));
        assertEquals(mounth, calendar.get(Calendar.MONTH));
        assertEquals(day, calendar.get(Calendar.DAY_OF_MONTH));
        assertEquals(hour, calendar.get(Calendar.HOUR_OF_DAY));
        assertEquals(minute, calendar.get(Calendar.MINUTE));
        assertEquals(second, calendar.get(Calendar.SECOND));
        assertEquals(millisecond, calendar.get(Calendar.MILLISECOND));
    }

    private static EdmElement getTypedProperty(EdmType type) {
        EdmElement property = mock(EdmElement.class);
        doReturn(type).when(property).getType();
        return property;
    }

}
