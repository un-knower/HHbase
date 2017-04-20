package com.ning.hhbase.bean;

import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;

public class TableJoinStruct extends TableJoin {
    
    private TableJoin outerTable;
    
    private String joinCol;
    
    private TableJoin innerTable;

    public TableJoin getOuterTable() {
        return outerTable;
    }

    public void setOuterTable(TableJoin outerTable) {
        this.outerTable = outerTable;
    }

    public String getJoinCol() {
        return joinCol;
    }

    public void setJoinCol(String joinCol) {
        this.joinCol = joinCol;
    }

    public TableJoin getInnerTable() {
        return innerTable;
    }

    public void setInnerTable(TableJoin innerTable) {
        this.innerTable = innerTable;
    }

    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this, ToStringStyle.MULTI_LINE_STYLE);
    }
}
