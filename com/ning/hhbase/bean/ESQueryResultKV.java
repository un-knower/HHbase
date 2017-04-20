package com.ning.hhbase.bean;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.ning.hhbase.exception.BigDataWareHouseException;

/**
 * 
  * @ClassName: ESQueryResultKV
  * @Description: TODO
  * @author nyx
  * @date 2016年9月27日 下午1:13:56
  *
 */
public class ESQueryResultKV {
    
    private List<Map<String,Object>> result = new ArrayList<Map<String,Object>>();
    
    private long total = 0l;
    
    private String statusCode = null;
    
    private BigDataWareHouseException exception = null;

    public List<Map<String,Object>> getResult() {
        return result;
    }

    public void setResult(List<Map<String,Object>> result) {
        this.result = result;
    }

    public long getTotal() {
        return total;
    }

    public void setTotal(long total) {
        this.total = total;
    }

    public String getStatusCode() {
        return statusCode;
    }

    public void setStatusCode(String statusCode) {
        this.statusCode = statusCode;
    }

    
    @Override
    public String toString() {
        StringBuffer buffer = new StringBuffer();
        buffer.append("resultSize=");
        buffer.append(result.size());
        buffer.append("total=");
        buffer.append(total);
        buffer.append("statusCode=");
        buffer.append(statusCode);
        return buffer.toString();
    }

    public BigDataWareHouseException getException() {
        return exception;
    }

    public void setException(BigDataWareHouseException exception) {
        this.exception = exception;
    }
}
