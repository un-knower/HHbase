package com.ning.hhbase.bean;

import com.ning.hhbase.exception.BigDataWareHouseException;

/**
 * 
  * @ClassName: ESInsertResult
  * @Description: TODO
  * @author nyx
  * @date 2016年9月20日 下午1:48:07
  *
 */
public class ESInsertResult {

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
