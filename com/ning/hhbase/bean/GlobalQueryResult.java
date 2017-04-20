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

import com.ning.hhbase.exception.BigDataWareHouseException;

/**
 * @ClassName: GlobalQueryResult
 * @Description: TODO
 * @author huangjinyan
 * @date 2016年8月15日 下午5:59:22
 *
 **/
public class GlobalQueryResult {

    private String resultString = null;
    
    private List<Object[]> result = new ArrayList<Object[]>();
    
    private String statusCode = null;
    
    private String endkey = null;
    
    private BigDataWareHouseException exception = null;

    public List<Object[]> getResult() {
        return result;
    }

    public void setResult(List<Object[]> result) {
        this.result = result;
    }
    
    public void addResult(List<Object[]> addResult) {
        if(null != addResult){
            for(Object[] str : addResult){
                this.result.add(str);
            }
        }
    }

    public String getStatusCode() {
        return statusCode;
    }

    public void setStatusCode(String statusCode) {
        this.statusCode = statusCode;
    }

    public String getEndkey() {
        return endkey;
    }

    public void setEndkey(String endkey) {
        this.endkey = endkey;
    }

    public String getResultString() {
        return resultString;
    }

    public void setResultString(String resultString) {
        this.resultString = resultString;
    }

    
    @Override
    public String toString() {
        StringBuffer buffer = new StringBuffer();
        buffer.append("resultString=");
        buffer.append(resultString);
        buffer.append("resultSize=");
        buffer.append(result.size());
        buffer.append("statusCode=");
        buffer.append(statusCode);
        buffer.append("endkey=");
        buffer.append(endkey);
        return buffer.toString();
    }

    public BigDataWareHouseException getException() {
        return exception;
    }

    public void setException(BigDataWareHouseException exception) {
        this.exception = exception;
    }
}
