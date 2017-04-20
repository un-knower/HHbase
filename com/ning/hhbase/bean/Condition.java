/**
 * Copyright: Copyright (c) 2016 
 * Company:东方网力科技股份有限公司
 * 
 * @author huangjinyan
 * @date 2016年8月15日 下午1:45:02
 * @version V1.0
 */
package com.ning.hhbase.bean;

import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;

import com.alibaba.fastjson.JSONObject;

/**
 * @ClassName: Condition
 * @Description: TODO
 * @author huangjinyan
 * @date 2016年8月15日 下午4:41:28
 *
 **/
public class Condition {
    
    public static final int EQUAL = 0;
    
    public static final int GREATER = 1;
    
    public static final int LESS = 2;
    
    public static final int GREATER_AND = 3;
    
    public static final int LESS_AND = 4;
    
    public static final int IK_FUZZY = 5;
    
    public static final int LIKE = 6;
    
    //列的名称
	private String column;
	
	private String value;
	
	// 条件比较类型  0 等于 1 大于 2 小于 3 大于等于 4 小于等于 5 模糊
	private int compareType;
	
	public Condition(){
	}
	
	public Condition(String column,String value,int compareType){
		this.column=column;
		this.value=value;
		this.compareType=compareType;
		
	}

	public String getColumn() {
		return column;
	}

	public void setColumn(String column) {
		this.column = column;
	}

	public String getValue() {
		return value;
	}

	public void setValue(String value) {
		this.value = value;
	}

	public int getCompareType() {
		return compareType;
	}

	public void setCompareType(int compareType) {
		this.compareType = compareType;
	}
	
	public JSONObject toJSON(){
		JSONObject json=new JSONObject();
		json.put("column", column);
		json.put("value", value);
		json.put("compareType", compareType);
		return json;
	}
	
	public String toJsonString() {
		return toJSON().toJSONString();
	}
	
	@Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this,ToStringStyle.MULTI_LINE_STYLE);
    }
}
