/**
 * Copyright: Copyright (c) 2016 
 * Company:东方网力科技股份有限公司
 * 
 * @author huangjinyan
 * @date 2016年8月15日 下午2:14:50
 * @version V1.0
 */
package com.ning.hhbase.connection;

import org.apache.log4j.Logger;

import com.ning.hhbase.exception.BigDataWareHouseException;


/**
 * @ClassName: ConnectionPoolManager
 * @Description: 数据仓库连接池
 * @author ningyexin
 * @date 2016年8月15日 下午2:14:50
 *
 **/
public class ConnectionPoolManager {

    /**
     * @Fields LOG : 日志
     **/
    private static final Logger LOG = Logger.getLogger(ConnectionPoolManager.class);
    
    /**
     * @Fields pool : 连接池存放
     **/
    private IConnectionPool pool = new ConnectionPool();

    /**
     * 创建一个新的实例 ConnectionPoolManager. 
     * <p>Title: </p>
     * <p>Description: </p>
     **/
    private ConnectionPoolManager() {
        LOG.info("======实例化连接池");
    }

    /**
     * @Title: getInstance
     * @Description: 单例实现
     * @return
     **/
    public static ConnectionPoolManager getInstance() {
        return Singleton.instance;
    }

    private static class Singleton {
        private static ConnectionPoolManager instance = new ConnectionPoolManager();
    }

    /**
     * @Title: getConnection
     * @Description: 获得连接,根据连接池名字 获得连接
     * @return
     * @throws BigDataWareHouseException
     **/
    public ESHbaseConnection getConnection() throws  BigDataWareHouseException {
        ESHbaseConnection conn = null;
        conn = this.pool.getConnection();
        return conn;
    }

    /**
     * @Title: close
     * @Description: 关闭，回收连接
     * @param conn
     * @throws BigDataWareHouseException
     **/
    public void close(ESHbaseConnection conn) throws BigDataWareHouseException {
        IConnectionPool pool = this.pool;

        if (pool != null) {
            pool.releaseConn(conn);
        }

    }

    /**
     * @Title: destroy
     * @Description: 清空连接池
     * @param poolName
     * @throws BigDataWareHouseException
     **/
    public void destroy(String poolName) throws BigDataWareHouseException {
        IConnectionPool pool = this.pool;
        if (pool != null) {
            pool.destroy();
        }
    }

    /**
     * @Title: getPool
     * @Description: 获取连接
     * @return
     **/
    public IConnectionPool getPool() {
        return pool;
    }
}
