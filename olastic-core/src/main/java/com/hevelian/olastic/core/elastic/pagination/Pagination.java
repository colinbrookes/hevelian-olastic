package com.hevelian.olastic.core.elastic.pagination;

import java.util.List;

/**
 * Encapsulates pagination data.
 */
public class Pagination {
    /** Default skip value. */
    public static final int SKIP_DEFAULT = 0;
    /** Default top value. */
    public static final int TOP_DEFAULT = 25;
    private int top;
    private int skip;
    private List<Sort> orderBy;
    /**
     * Initializes pagination with all the data
     * @param top top count
     * @param skip skip count
     * @param orderBy order by field
     */
    public Pagination(int top, int skip,  List<Sort> orderBy) {
        this.top = top;
        this.skip = skip;
        this.orderBy = orderBy;
    }

    public int getTop() {
        return top;
    }

    public void setTop(int top) {
        this.top = top;
    }

    public int getSkip() {
        return skip;
    }

    public void setSkip(int skip) {
        this.skip = skip;
    }

    public  List<Sort> getOrderBy() {
        return orderBy;
    }

    public void setOrderBy( List<Sort> orderBy) {
        this.orderBy = orderBy;
    }
}
