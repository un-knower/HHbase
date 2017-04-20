/**
 * Copyright: Copyright (c) 2016 
 * Company:东方网力科技股份有限公司
 * 
 * @author huangjinyan
 * @date 2016年8月15日 下午1:45:02
 * @version V1.0
 */
package com.ning.hhbase.bean;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;

/**
 * @ClassName: Table
 * @Description: TODO
 * @author huangjinyan
 * @date 2016年8月16日 下午3:12:58
 *
 **/
public class Table {

    private String name;

    private List<Column> columnList = new ArrayList<Column>();

    public String[] getColumnsStringArray() {
        return getColumnsString().split(",");
    }

    public String getColumnsString() {
        String str = "";
        for (Column col : getHiveColumnsDistinct()) {
            if(col != null){
                str += "," + col.getName();
            }
        }
        return str.length() > 1 ? str.substring(1) : str;
    }

    // public Map<String, Column> getColumnMap() {
    // Map<String, Column> map = new HashMap<>();
    // for (Column col : columnList) {
    // map.put(col.getName(), col);
    // }
    // return map;
    // }

    public Map<String, Column> getHiveColumnMap() {
        Map<String, Column> map = new HashMap<>();
        for (Column col : columnList) {
            if(col != null){
                map.put(col.getHiveName(), col);
            }
        }
        return map;
    }

    public List<String> getColumnsStringList() {
        List<String> list = new ArrayList<String>();
        for (Column col : columnList) {
            if(col != null){
                list.add(col.getName());
            }
        }
        return list;
    }

    public List<String> getHiveColumnsStringList() {
        List<String> list = new ArrayList<String>();
        for (Column col : columnList) {
            if(col != null){
                list.add(col.getName());
            }
            
        }
        return list;
    }

    public List<Column> getColumnList() {
        return columnList;
    }

    public void setColumnList(List<Column> columnList) {
        this.columnList = columnList;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void add(Column column) {
        this.columnList.add(column);
    }

    /**
     * 验证列数组
     * 
     * @param columns
     * @return
     */
    public boolean verifyColumns(String[] columns) {
        if (null == columns || columns.length == 0) {
            return false;
        }
        List<String> list = this.getColumnsStringList();
        for (String col : columns) {
            if (!list.contains(col)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this, ToStringStyle.MULTI_LINE_STYLE);
    }

    public Set<Column> getHiveColumnsDistinct() {
        Set<Column> colSet = new HashSet<Column>();
        Column col = null;
        for (int i = 0; i < columnList.size(); i++) {
            col = columnList.get(i);
            if (col.isStruct()) {
                List<Column> cols = col.getStructCols();
                for (int j = 0; j < cols.size(); j++) {
                    colSet.add(cols.get(j));
                }
            } else {
                colSet.add(col);
            }
        }

        return colSet;
    }

    public List<Column> getStructCols() {
        List<Column> resStructCols = new ArrayList<Column>();
        Column col = null;
        for (int i = 0; i < columnList.size(); i++) {
            col = columnList.get(i);
            if (col.isStruct()) {
                resStructCols.add(col);
            }
        }
        return resStructCols;
    }

    public List<Column> getNotStructCols() {
        List<Column> resStructCols = new ArrayList<Column>();
        Column col = null;
        for (int i = 0; i < columnList.size(); i++) {
            col = columnList.get(i);
            if (!col.isStruct()) {
                resStructCols.add(col);
            }
        }
        return resStructCols;
    }
    
    public enum NPBaseTableType {
        ES, 
        HBASE, 
        ORC,
        TXT
        ;
    }

    public NPBaseTableType getTableType(){
        if(this instanceof ESTable){
            return NPBaseTableType.ES;
        }else if(this instanceof HbaseTable){
            return NPBaseTableType.HBASE;
        }else if(this instanceof OrcTable){
            return NPBaseTableType.ORC;
        }else{
            return NPBaseTableType.TXT;
        }
    }
    
}