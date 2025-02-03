package com.github.wz2cool.localqueue.model.page;

import java.util.List;

public class PageInfo<T> {

    private final long start;

    private final long end;

    private final List<T> data;

    private final SortDirection sortDirection;

    private final int pageSize;

    public PageInfo(long start, long end, List<T> data, SortDirection sortDirection, int pageSize) {
        this.start = start;
        this.end = end;
        this.data = data;
        this.sortDirection = sortDirection;
        this.pageSize = pageSize;
    }

    public long getStart() {
        return start;
    }

    public long getEnd() {
        return end;
    }

    public List<T> getData() {
        return data;
    }

    public SortDirection getSortDirection() {
        return sortDirection;
    }


    public int getPageSize() {
        return pageSize;
    }

}
