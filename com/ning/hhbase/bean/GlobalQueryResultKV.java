package com.ning.hhbase.bean;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.ning.hhbase.exception.BigDataWareHouseException;

/**
 * 
  * @ClassName: GlobalQueryResultKV
  * @Description: TODO
  * @author abc
  * @date 2016年9月27日 下午2:08:00
  *
 */
public class GlobalQueryResultKV {
    
    private List<Map<String,Object>> result = new ArrayList<Map<String,Object>>();
    
    private String statusCode = null;
    
    private String endkey = null;
    
    private BigDataWareHouseException exception = null;

    public List<Map<String,Object>> getResult() {
        return result;
    }

    public void setResult( List<Map<String,Object>> result) {
        this.result = result;
    }
    
    public void addResult(List<Map<String,Object>> addResult) {
        if(null != addResult){
            for(Map<String,Object> str : addResult){
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

    
    @Override
    public String toString() {
        StringBuffer buffer = new StringBuffer();
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
