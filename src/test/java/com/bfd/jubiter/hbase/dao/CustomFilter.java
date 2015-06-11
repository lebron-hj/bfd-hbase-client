package com.bfd.jubiter.hbase.dao;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.hbase.util.Bytes;

public class CustomFilter extends FilterBase {
    private byte[] value = null;
    private boolean filterRow = true;

    public CustomFilter() {
        super();
    }

    public CustomFilter(byte[] value) {
        this.value = value;
    }

    @Override
    public void reset() {
        this.filterRow = true;
    }

    @Override
    public ReturnCode filterKeyValue(Cell cell) {
        if (Bytes.compareTo(value, CellUtil.cloneValue(cell)) == 0) {
            filterRow = false;
        }
        return ReturnCode.INCLUDE;
    }

    @Override
    public boolean filterRow() {
        return filterRow;
    }

}