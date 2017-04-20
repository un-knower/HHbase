package com.ning.hhbase.bean;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;

public class Column {

    public static final String type_number = "number";
    public static final String type_string = "string";
    public static final String type_boolean = "boolean";
    public static final String type_date = "date";

    private String name;
    private String type;
    private String familyName;
    private boolean rowKey = false;
    private int indexLength;

    private String hiveName;
    private String hiveType;
    private boolean struct = false;
    private List<Column> structCols = new ArrayList<Column>();

    public String getHiveName() {
        return name;
    }

    public void setHiveName(String hiveName) {
        this.hiveName = hiveName;
    }

    public String getHiveType() {
        return hiveType;
    }

    public void setHiveType(String hiveType) {
        this.hiveType = hiveType;
    }

    public boolean isStruct() {
        return struct;
    }

    public void setStruct(boolean struct) {
        this.struct = struct;
    }

    public List<Column> getStructCols() {
        return structCols;
    }

    public void setStructCols(List<Column> structCols) {
        this.structCols = structCols;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getFamilyName() {
        return familyName;
    }

    public void setFamilyName(String familyName) {
        this.familyName = familyName;
    }

    public boolean isRowKey() {
        return rowKey;
    }

    public void setRowKey(boolean rowKey) {
        this.rowKey = rowKey;
    }

    public int getIndexLength() {
        return indexLength;
    }

    public void setIndexLength(int indexLength) {
        this.indexLength = indexLength;
    }

    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this, ToStringStyle.MULTI_LINE_STYLE);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof Column) {
            Column col = (Column) obj;
            if (col.getHiveName().equals(this.getHiveName())) {
                return true;
            }
            return false;
        } else {
            return false;
        }

    }

    @Override
    public int hashCode() {
        return this.getHiveName().hashCode();
    }
    
    public static void main(String[] args) {
        
    }

}
