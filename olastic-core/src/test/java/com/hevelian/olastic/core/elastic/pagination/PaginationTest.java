package com.hevelian.olastic.core.elastic.pagination;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * JUnit test for {@link Pagination} class.
 * @author Taras Kohut
 */
public class PaginationTest {
    @Test
    public void constructor_topSkipOrder_allPropertiesAreSet() {
        int top = 0;
        int skip = 10;
        String sortColumn = "column";
        List<Sort> orderBy = new ArrayList<>();
        Sort sort = new Sort(sortColumn);
        orderBy.add(sort);
        Pagination pagination = new Pagination(top, skip, orderBy);
        assertEquals(top, pagination.getTop());
        assertEquals(skip, pagination.getSkip());
        assertEquals(orderBy, pagination.getOrderBy());
    }

    @Test
    public void setters_topSkipOrder_allPropertiesAreSet() {
        int top = 0;
        int newTop = 5;
        int skip = 10;
        int newSkip = 20;
        String sortColumn = "column";
        List<Sort> orderBy = new ArrayList<>();
        Sort sort = new Sort(sortColumn);
        orderBy.add(sort);

        Pagination pagination = new Pagination(top, skip, orderBy);

        String newSortColumn = "newcolumn";
        List<Sort> newOrderBy = new ArrayList<>();
        Sort newSort = new Sort(newSortColumn);
        newOrderBy.add(newSort);

        pagination.setSkip(newSkip);
        pagination.setTop(newTop);
        pagination.setOrderBy(newOrderBy);

        assertEquals(newTop, pagination.getTop());
        assertEquals(newSkip, pagination.getSkip());
        assertEquals(newOrderBy, pagination.getOrderBy());
    }
}
