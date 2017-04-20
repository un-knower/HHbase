package com.ning.hhbase.bean;

import com.ning.hhbase.exception.BigDataWareHouseException;

public class HbaseInsertResult {

    private String statusCode = null;
    
    private BigDataWareHouseException exception = null;

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
