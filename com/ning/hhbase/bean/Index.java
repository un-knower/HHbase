package com.ning.hhbase.bean;

import java.util.ArrayList;
import java.util.List;

public class Index {
    
    private String indexName;
    
    private String indexType;
    
    private List<Column> indexColumnList = new ArrayList<Column>();

    public String getIndexName() {
        return indexName;
    }

    public void setIndexName(String indexName) {
        this.indexName = indexName;
    }

    public String getIndexType() {
        return indexType;
    }

    public void setIndexType(String indexType) {
        this.indexType = indexType;
    }

    public List<Column> getIndexColumnList() {
        return indexColumnList;
    }

    public void setIndexColumnList(List<Column> indexColumnList) {
        this.indexColumnList = indexColumnList;
    }
    
    
    
}
