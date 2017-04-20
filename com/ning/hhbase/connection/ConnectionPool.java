/**
 * Copyright: Copyright (c) 2016 
 * Company:东方网力科技股份有限公司
 * 
 * @author huangjinyan
 * @date 2016年8月15日 下午2:14:50
 * @version V1.0
 */
package com.ning.hhbase.connection;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

import javax.script.ScriptEngineManager;
import javax.script.ScriptException;

import org.apache.log4j.Logger;

import com.ning.hhbase.common.Constants;
import com.ning.hhbase.config.NPBaseConfiguration;
import com.ning.hhbase.engine.NPBaseEngine;
import com.ning.hhbase.exception.BigDataWareHouseException;
import com.ning.hhbase.tools.LogUtils;


/**
 * @ClassName: ConnectionPool
 * @Description: 连接池主类
 * @author ningyexin
 * @date 2016年8月15日 下午2:27:17
 *
 **/
public class ConnectionPool implements IConnectionPool {

    /**
     * @Fields LOG : 日志
     **/
    private static final Logger LOG = Logger.getLogger(ConnectionPool.class);
    
    /**
     * @Fields isActive : 连接池活动状态
     **/
    private boolean isActive = false;
    
    /**
     * @Fields contConnection : 记录创建的总的连接数
     **/
    private AtomicLong contConnection = new AtomicLong(0);
    
    private AtomicLong freeTime = new AtomicLong(-1);
    
    /**
     * @Fields freeConnection : 空闲连接
     **/
    private BlockingQueue<ESHbaseConnection> freeConnection = null;
    
    /**
     * @Fields activeConnection : 活动连接
     **/
    private BlockingQueue<ESHbaseConnection> activeConnection = null;

    private int retryTime = -1;

    private int connectionTimeout = -1;

    private static NPBaseConfiguration config = NPBaseEngine.getConfig();
    
    /**
     * 创建一个新的实例 ConnectionPool. 
     **/
    public ConnectionPool() {
        try {
            // 初始
            init();

            initConstants();
        } catch (BigDataWareHouseException e) {
            LOG.error("");
        }
    }

    private void initConstants() throws BigDataWareHouseException {
        try {
            double tmpDubl = (Double) new ScriptEngineManager().getEngineByName("JavaScript").eval(config.getValue(NPBaseConfiguration.CONNECTION_RETRY_TIME));
            retryTime = (int) tmpDubl;
            tmpDubl = (Double) new ScriptEngineManager().getEngineByName("JavaScript").eval(config.getValue(NPBaseConfiguration.CONNECTION_TIMEOUT));
            connectionTimeout = (int) tmpDubl;
        } catch (ScriptException e) {
            e.printStackTrace();
            LogUtils.errorMsg(e,BigDataWareHouseException.CONNECTION_POOL_CONFIG);
        }

    }

    class CloseConnThread extends Thread {

        public void run() {
            ESHbaseConnection conn;
            while (true) {
                if (freeTime.get() != -1 && (System.currentTimeMillis() - freeTime.get()) >= 1000 * 60 * 3) {
                    for (int i = 0; i < freeConnection.size() - Integer.parseInt(config.getValue(NPBaseConfiguration.INIT_CONNECTIONS)); i++) {
                        conn = freeConnection.poll();
                        try {
                            conn.close();
                        } catch (BigDataWareHouseException e) {
                            e.printStackTrace();
                        }
                    }
                }
                try {
                    CloseConnThread.sleep(1000 * 5);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }

        }
    }

    /**
     * @Title: init
     * @Description: 初始化
     **/
    public synchronized void init() {
        
        // 初始化活动和空闲连接池队列大小
        if (freeConnection == null && activeConnection == null) {
            freeConnection = new ArrayBlockingQueue<ESHbaseConnection>(Integer.parseInt(config.getValue(NPBaseConfiguration.MAX_CONNECTIONS)));
            activeConnection = new ArrayBlockingQueue<ESHbaseConnection>(Integer.parseInt(config.getValue(NPBaseConfiguration.MAX_CONNECTIONS)));
        }
        
        try {

            // 数据仓库连接池
            ESHbaseConnection conn;
            
            for (int i = 0; i < Integer.parseInt(config.getValue(NPBaseConfiguration.INIT_CONNECTIONS)); i++) {
                
                conn = newConnection(config);
                
                // 初始化最小连接数
                if (conn != null) {
                    freeConnection.add(conn);
                    contConnection.getAndIncrement();
                }
            }
            
            isActive = true;
        } catch (BigDataWareHouseException e) {
            e.printStackTrace();
        }
        new CloseConnThread().start();
    }

    
    /**
     * @Title: getConnection
     * @Description: 获取连接
     **/
    public synchronized ESHbaseConnection getConnection() throws BigDataWareHouseException {
        
        ESHbaseConnection conn = null;

        // 判断是否超过最大连接数限制
        long beginTry = System.currentTimeMillis();
        
        if (freeConnection.size() > 0) {
            conn = freeConnection.poll();
            activeConnection.add(conn);
            freeTime.set(-1);
        } else if (contConnection.get() < Integer.parseInt(config.getValue(NPBaseConfiguration.MAX_CONNECTIONS))) {
            conn = newConnection(config);
            activeConnection.add(conn);
            freeTime.set(-1);
            contConnection.getAndIncrement();
        } else {
            
            // 继续获得连接,直到重新获得连接
            try {
                wait(retryTime);
            } catch (InterruptedException e) {
                LOG.error(e.getMessage());
                LogUtils.errorMsg(e,BigDataWareHouseException.CONNECTION_POOL_INTERRUPTED);
            }
            
            conn = getConnection();
            if (System.currentTimeMillis() - beginTry >= connectionTimeout) {
                LogUtils.errorMsg(BigDataWareHouseException.CONNECTION_TIMEOUT);
            }
        }

        return conn;
    }

    /**
     * @Title: newConnection
     * @Description: 获得新连接
     * @return
     * @throws BigDataWareHouseException
     **/
    private ESHbaseConnection newConnection(NPBaseConfiguration config) throws BigDataWareHouseException {
        ESHbaseConnection conn = null;
        conn = new ESHbaseConnection(config);
        return conn;
    }

    /**
     * @Title: releaseConn
     * @Description: 释放连接
     * @return
     * @throws BigDataWareHouseException
     **/
    public synchronized void releaseConn(ESHbaseConnection conn) throws BigDataWareHouseException {
        
        if (isValid(conn) && !(freeConnection.size() > Integer.parseInt(config.getValue(NPBaseConfiguration.MAX_CONNECTIONS)))) {
            freeConnection.add(conn);
            activeConnection.remove(conn);

            if (activeConnection.size() == 0) {
                freeTime.set(System.currentTimeMillis());
            }
            
            // 唤醒所有正待等待的线程，去抢连接
            notifyAll();
        }
    }

    /**
     * @Title: isValid
     * @Description: 判断连接是否可用
     * @param conn
     * @return
     * @throws BigDataWareHouseException
     **/
    private boolean isValid(ESHbaseConnection conn) throws BigDataWareHouseException {
        if (conn == null || conn.isClosed()) {
            return false;
        }
        return true;
    }

    /**
     * @Title: destroy
     * @Description: 销毁连接池
     * @param conn
     * @return
     * @throws BigDataWareHouseException
     **/
    public synchronized void destroy() throws BigDataWareHouseException {
        for (ESHbaseConnection conn : freeConnection) {
            if (isValid(conn)) {
                conn.close();
            }
        }
        for (ESHbaseConnection conn : activeConnection) {
            if (isValid(conn)) {
                conn.close();
            }
        }
        isActive = false;
        contConnection.set(0);
    }

    /**
     * @Title: isActive
     * @Description: 连接池状态
     * @param conn
     * @return
     * @throws BigDataWareHouseException
     **/
    @Override
    public boolean isActive() {
        return isActive;
    }
}