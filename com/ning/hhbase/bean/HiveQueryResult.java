package com.ning.hhbase.bean;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.ning.hhbase.exception.BigDataWareHouseException;

public class HiveQueryResult {


    private List<Map<String,Object>> result = new ArrayList<Map<String,Object>>();
    
    private String statusCode = null;
    
    private BigDataWareHouseException exception = null;

    public List<Map<String, Object>> getResult() {
        return result;
    }

    public void setResult(List<Map<String, Object>> result) {
        this.result = result;
    }

    public String getStatusCode() {
        return statusCode;
    }

    public void setStatusCode(String statusCode) {
        this.statusCode = statusCode;
    }

    public BigDataWareHouseException getException() {
        return exception;
    }

    public void setException(BigDataWareHouseException exception) {
        this.exception = exception;
    }
    
    
}
