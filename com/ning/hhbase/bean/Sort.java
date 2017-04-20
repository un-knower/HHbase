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

/**
 * @ClassName: Sort
 * @Description: TODO
 * @author huangjinyan
 * @date 2016年8月15日 下午4:46:54
 *
 **/
public class Sort {
	private String column;
	private boolean desc;
	
	public Sort(String column,boolean desc){
		this.column=column;
		this.desc=desc;
	}
	
	public String getColumn() {
		return column;
	}
	
	public void setColumn(String column) {
		this.column = column;
	}
	
	public boolean isDesc() {
		return desc;
	}
	
	public void setDesc(boolean desc) {
		this.desc = desc;
	}

	@Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this,ToStringStyle.MULTI_LINE_STYLE);
    }
}
