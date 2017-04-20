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
import java.util.List;

import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

/**
 * @ClassName: Conditions
 * @Description: TODO
 * @author huangjinyan
 * @date 2016年8月15日 下午4:46:13
 *
 **/
public class Conditions extends Condition{
	
    public static final int NOT = 2;
    
    public static final int AND = 1;
    
    public static final int OR = 0;
    
    public Conditions(String column, String value, int compareType) {
		super(column, value, compareType);
	}

	public Conditions() {
	}

	private int isOr; //是否是Or操作，0 should; 1 must 2 must_not
	
	private List<Condition> cdList=new ArrayList<Condition>();//条件列表
	
	
	public int getIsOr() {
        return isOr;
    }

    public void setIsOr(int isOr) {
        this.isOr = isOr;
    }

    public void add(Condition condition){
		cdList.add(condition);
	}
	
	public void setCdList(List<Condition> cdList) {
        this.cdList = cdList;
    }
	
	public List<Condition> getCdList() {
		return cdList;
	}

	public JSONObject toJSON() {
		JSONObject json=new JSONObject();
		if(this.cdList.size()>0){
			JSONArray array=new JSONArray();
			for(Condition cd:cdList){
				array.add(cd.toJSON());
			}
			json.put("conditions", array);
			json.put("isOr", isOr);
		}else{
			json.put("column", this.getColumn());
			json.put("value", getValue());
			json.put("compareType", getCompareType());
		}
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
