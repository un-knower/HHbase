/**
 * Copyright: Copyright (c) 2016 
 * Company:东方网力科技股份有限公司
 * 
 * @author huangjinyan
 * @date 2016年8月15日 下午2:32:13
 * @version V1.0
 */
package com.ning.hhbase.config;

import java.util.Properties;

import org.apache.log4j.Logger;

/**
 * @ClassName: NPBaseConfiguration
 * @Description: NPBase配置项
 * @author huangjinyan
 * @date 2016年8月15日 下午2:32:13
 *
 **/
public final class NPBaseConfiguration {

    /**
     * @Fields log : 日志
     **/
    private static Logger LOG = Logger.getLogger(NPBaseConfiguration.class);

    // 常量定义
    public static final String HBASE_BUFFER_SIZE = "hbase.buffer.size";
    public static final String HBASE_COPROCESSER_PATH = "hbase.coprocesser.path";
    public static final String HBASE_ZOOKEEPERIP = "hbase.zookeeperIp";
    public static final String HBASE_ZOOKEEPERPORT = "hbase.zookeeperPort";
    public static final String HBASE_HOST = "hbase.hbaseHost";
    public static final String HIVE_URL = "hive.url";
    public static final String HIVE_USERNAME = "hive.userName";
    public static final String HIVE_PASSWORD = "hive.password";
    public static final String ES_ADDRESS = "es.address";
    public static final String ES_PORT = "es.port";
    public static final String ES_CLUSTER = "es.cluster";
    public static final String ES_BATCH_SIZE = "es.batch.size";
    public static final String ES_SHARDS = "es.shards";
    public static final String ES_REPLICAS = "es.replicas";
    public static final String ESSWITCH = "es.switch";
    public static final String ZOOKEEPER_SERVER = "kafka.zookeeper.server";
    public static final String INIT_CONNECTIONS = "connpool.init.connections";
    public static final String MAX_CONNECTIONS = "connpool.max.connections";
    public static final String CONNECTION_TIMEOUT = "connpool.connection.timeOut";
    public static final String CONNECTION_RETRY_TIME = "connpool.retry.time";
    public static final String MYCAT_URL = "mycat.url";
    public static final String MYCAT_USER = "mycat.user";
    public static final String MYCAT_PASSWORD = "mycat.password";
    public static final String MYCAT_SWITCH = "mycat.switch";
    public static final String HDFS_URL = "hdfs.url";
    public static final String DATA_ACCESS_SPLIT = "data.access.split";
    public static final String ZOOKEEPER_SESSION_TIMEOUT = "zookeeper.sessionTimeout";
    public static final String HIVEMYSQL_URL = "hive.mysql.url";
    public static final String HIVEMYSQL_USER = "hive.mysql.user";
    public static final String HIVEMYSQL_PASSWORD = "hive.mysql.password";
    public static final String HIVE_THRIFT_URL = "hive.thrift.url";
    public static final String USE_NPBASE_FILTER = "use.npbase.filter";
    public static final String HIVE_ORC_SWITCH = "hive.orc.switch";
    
    
    private Properties configProp = new Properties();

    public NPBaseConfiguration(){
        //默认值设置
        configProp.setProperty(HBASE_BUFFER_SIZE, "60*1024*1024");
        configProp.setProperty(HIVE_USERNAME, "");
        configProp.setProperty(HIVE_PASSWORD, "");
        configProp.setProperty(ES_BATCH_SIZE, "10000");
        configProp.setProperty(ES_SHARDS, "12");
        configProp.setProperty(ES_REPLICAS, "0");
        configProp.setProperty(ESSWITCH, "true");
        configProp.setProperty(INIT_CONNECTIONS, "5");
        configProp.setProperty(MAX_CONNECTIONS, "50");
        configProp.setProperty(CONNECTION_TIMEOUT, "1000*60*2");
        configProp.setProperty(CONNECTION_RETRY_TIME, "1000");
        configProp.setProperty(MYCAT_SWITCH, "false");
        configProp.setProperty(ZOOKEEPER_SESSION_TIMEOUT, "30000");
        configProp.setProperty(USE_NPBASE_FILTER, "false");
        configProp.setProperty(HIVE_ORC_SWITCH, "true");
    }
    
    public void setProperty(Properties prop) {
        this.configProp = prop;
    }

    public String getValue(String key){
        return (String) configProp.getProperty(key);
    }
    
    public void setValue(String key,String value){
        configProp.setProperty(key, value);
    }
    
}
