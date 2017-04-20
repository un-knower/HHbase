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
 * @ClassName: ResultCode
 * @Description: TODO
 * @author huangjinyan
 * @date 2016年8月15日 下午5:19:22
 *
 **/
public class ResultCode {
    
    public static final String TABLE_NOT_EXISTS = "101";
    
    public static final String COLUMNS_NO_DATA = "107";

    public static final String COLUMN_NOT_EXISTS = "103";

    public static final String NO_INDEX = "102";

    public static final String INTERNAL_ERROR = "100";

    public static final String INDEX_TYPE_NOT_MATCH = "110";

    public static final String VALUE_NO_DATA = "108";

    public static final String FILTER_DONT_EXSIST = "109";
    
    public static final String DATE_FORMAT_ERROR = "111";
    
    private String resultCode = "0";

    public String getResultCode() {
        return resultCode;
    }

    public void setResultCode(String resultCode) {
        this.resultCode = resultCode;
    }
    
    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this,ToStringStyle.MULTI_LINE_STYLE);
    }
}
