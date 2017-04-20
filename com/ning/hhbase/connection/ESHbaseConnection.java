/**
 * Copyright: Copyright (c) 2016 
 * Company:东方网力科技股份有限公司
 * 
 * @author huangjinyan
 * @date 2016年8月15日 下午2:17:47
 * @version V1.0
 */
package com.ning.hhbase.connection;

import java.io.IOException;
import java.sql.DriverManager;
import java.sql.SQLException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.log4j.Logger;
import org.elasticsearch.client.transport.TransportClient;

import com.ning.hhbase.common.Constants;
import com.ning.hhbase.config.NPBaseConfiguration;
import com.ning.hhbase.engine.NPBaseEngine;
import com.ning.hhbase.exception.BigDataWareHouseException;
import com.ning.hhbase.tools.EsUtils;
import com.ning.hhbase.tools.LogUtils;


/**
 * @ClassName: ESHbaseConnection
 * @Description: 连接类
 * @author ningyexin
 * @date 2016年8月15日 下午2:17:47
 *
 **/
public class ESHbaseConnection {

    /**
     * @Fields LOG : 日志
     **/
    private static final Logger LOG = Logger.getLogger(ESHbaseConnection.class);
    
    /**
     * @Fields driverName : hive驱动名
     **/
    private static String driverName = "org.apache.hive.jdbc.HiveDriver";
    
    /**
     * @Fields hbaseConfig : hbaseconfig
     **/
    private static Configuration hbaseConfig = HBaseConfiguration.create();
    
    /**
     * @Fields hbaseConnection : hbase连接
     **/
    private Connection hbaseConnection;
    
    /**
     * @Fields client : es客户端
     **/
    private TransportClient client = acClient;
    
    /**
     * @Fields client : es客户端
     **/
    private static TransportClient acClient = null;
    
    /**
     * @Fields hiveConnection : hive连接
     **/
    private java.sql.Connection hiveConnection;
    
    private NPBaseConfiguration config = null;
    
    public void setConfig(NPBaseConfiguration config) {
        this.config = config;
    }
    
    public ESHbaseConnection(NPBaseConfiguration config) throws BigDataWareHouseException{
        this.config = config;
        
        // 初始化hbaseconfig
        hbaseConfig.set("hbase.zookeeper.property.clientPort", config.getValue(NPBaseConfiguration.HBASE_ZOOKEEPERPORT));
        hbaseConfig.set("hbase.zookeeper.quorum", config.getValue(NPBaseConfiguration.HBASE_ZOOKEEPERIP));
        hbaseConfig.set("hbase.master", config.getValue(NPBaseConfiguration.HBASE_HOST));
        
        try {
            hbaseConnection = ConnectionFactory.createConnection(hbaseConfig);
        } catch (IOException e) {
            LOG.error(e.getMessage());
            LogUtils.errorMsg(e,BigDataWareHouseException.HBSE_CONNECTION_FAIL);
        }
        
        try {
            Class.forName(driverName);
        } catch (ClassNotFoundException e) {
            LOG.error(e.getMessage());
            LogUtils.errorMsg(e,BigDataWareHouseException.HIVE_DRIVER_NOTFOUND);
        }
        
        try {
            hiveConnection = DriverManager.getConnection(config.getValue(NPBaseConfiguration.HIVE_URL), config.getValue(NPBaseConfiguration.HIVE_USERNAME), config.getValue(NPBaseConfiguration.HIVE_PASSWORD));
        } catch (SQLException e) {
            LOG.error(e.getMessage());
            LogUtils.errorMsg(e,BigDataWareHouseException.HIVE_CONNECT_FAIL);
            
        }
        if("true".equals(config.getValue(NPBaseConfiguration.ESSWITCH)) && acClient == null){
            try {
                acClient = new EsUtils(config).getClient();
                client = acClient;
            } catch (Exception e) {
                LOG.error(e.getMessage());
                LogUtils.errorMsg(e,BigDataWareHouseException.ES_CLIENT_ERROR);
            }
        }else{
            client = acClient;
        }
            
    }
    
    /**
     * @Title: gethbaseConfig
     * @Description: TODO
     * @return
     **/
    public static Configuration gethbaseConfig() {
        return hbaseConfig;
    }
    
    /**
     * @Title: isClosed
     * @Description: TODO
     * @return
     * @throws BigDataWareHouseException
     **/
    public boolean isClosed() throws BigDataWareHouseException{
        if(hbaseConnection == null ){
            return true;
        }
        return false;
    }
    
    /**
     * @Title: close
     * @Description: 关闭连接
     * @throws BigDataWareHouseException
     **/
    public void close() throws BigDataWareHouseException{
//        client = null;
//        if(hbaseConnection != null){
//            try {
//                hbaseConnection.close();
//            } catch (IOException e) {
//                LOG.error(e.getMessage());
//                throw new BigDataWareHouseException();
//            }
//        }
        
        ConnectionPoolManager.getInstance().close(this);
    }
    
    public Connection getHbaseConnection() {
        return hbaseConnection;
    }

    public void setHbaseConnection(Connection hbaseConnection) {
        this.hbaseConnection = hbaseConnection;
    }

    public TransportClient getEsClient() {
        return client;
    }

    public void setEsClient(TransportClient client) {
        this.client = client;
    }

    public java.sql.Connection getHiveConnection() {
        return hiveConnection;
    }

    public void setHiveConnection(java.sql.Connection hiveConnection) {
        this.hiveConnection = hiveConnection;
    }
}
