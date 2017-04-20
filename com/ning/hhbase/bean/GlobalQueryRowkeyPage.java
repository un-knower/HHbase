/**
 * Copyright: Copyright (c) 2016 
 * Company:东方网力科技股份有限公司
 * 
 * @author huangjinyan
 * @date 2016年8月15日 下午1:45:02
 * @version V1.0
 */
package com.ning.hhbase.bean;

import java.util.List;

import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;

/**
 * @ClassName: GlobalQueryRowkeyPage
 * @Description: TODO
 * @author huangjinyan
 * @date 2016年8月15日 下午5:11:50
 *
 **/
public class GlobalQueryRowkeyPage {
    
    private List<String> rowkeys = null;
    private String endKey = null;

    public List<String> getRowkeys() {
        return rowkeys;
    }

    public void setRowkeys(List<String> rowkeys) {
        this.rowkeys = rowkeys;
    }

    public String getEndKey() {
        return endKey;
    }

    public void setEndKey(String endKey) {
        this.endKey = endKey;
    }

    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this,ToStringStyle.MULTI_LINE_STYLE);
    }
    
}
