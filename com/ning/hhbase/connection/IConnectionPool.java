/**
 * Copyright: Copyright (c) 2016 
 * Company:东方网力科技股份有限公司
 * 
 * @author huangjinyan
 * @date 2016年8月15日 下午2:15:44
 * @version V1.0
 */
package com.ning.hhbase.connection;

import com.ning.hhbase.exception.BigDataWareHouseException;

/**
 * @ClassName: IConnectionPool
 * @Description: 连接池接口
 * @author ningyexin
 * @date 2016年8月15日 下午2:15:44
 *
 **/
public interface IConnectionPool {
    
    /**
     * @Title: getConnection
     * @Description: 获得连接  
     * @return
     * @throws BigDataWareHouseException
     **/
    public ESHbaseConnection getConnection() throws BigDataWareHouseException;  
    
    /**
     * @Title: releaseConn
     * @Description: TODO
     * @param 回收连接
     * @throws BigDataWareHouseException
     **/
    public void releaseConn(ESHbaseConnection conn) throws BigDataWareHouseException;  
    
    /**
     * @Title: destroy
     * @Description: 销毁清空  
     * @throws BigDataWareHouseException
     **/
    public void destroy() throws BigDataWareHouseException;  
    
    /**
     * @Title: isActive
     * @Description: 连接池是活动状态  
     * @return
     **/
    public boolean isActive();  
}
