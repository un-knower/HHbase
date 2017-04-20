/**
 * Copyright: Copyright (c) 2016 
 * Company:东方网力科技股份有限公司
 * 
 * @author huangjinyan
 * @date 2016年8月15日 下午4:58:58
 * @version V1.0
 */
package com.ning.hhbase.tools;

import org.apache.log4j.Logger;

import com.ning.hhbase.exception.BigDataWareHouseException;


/**
 * @ClassName: LogUtils
 * @Description: TODO
 * @author huangjinyan
 * @date 2016年8月15日 下午4:58:58
 *
 **/
public class LogUtils {

    /**
     * @Fields LOG : 日志
     **/
    private static Logger LOG = Logger.getLogger(LogUtils.class);
    
    public static void errorMsg(Throwable e,String msg, String... params) throws BigDataWareHouseException {
        LOG.error(BigDataWareHouseException.buildMsg(e,msg, params));
        throw BigDataWareHouseException.throwException(e,msg, params);
    }
    
    public static void errorMsg(String msg, String... params) throws BigDataWareHouseException {
        LOG.error(BigDataWareHouseException.buildMsg(msg, params));
        throw BigDataWareHouseException.throwException(msg, params);
    }
}
