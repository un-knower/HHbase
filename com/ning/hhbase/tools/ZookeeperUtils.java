/**
 * Copyright: Copyright (c) 2016 
 * Company:东方网力科技股份有限公司
 * 
 * @author huangjinyan
 * @date 2016年8月15日 下午3:37:19
 * @version V1.0
 */
package com.ning.hhbase.tools;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import com.google.gson.Gson;
import com.ning.hhbase.bean.Column;
import com.ning.hhbase.bean.ESTable;
import com.ning.hhbase.bean.HbaseTable;
import com.ning.hhbase.bean.Index;
import com.ning.hhbase.bean.Table;
import com.ning.hhbase.common.Constants;
import com.ning.hhbase.config.NPBaseConfiguration;
import com.ning.hhbase.connection.ConnectionPoolManager;
import com.ning.hhbase.connection.ESHbaseConnection;
import com.ning.hhbase.engine.NPBaseEngine;
import com.ning.hhbase.exception.BigDataWareHouseException;

/**
 * @ClassName: ZookeeperUtils
 * @Description: 移植原tableDefine类
 * @author huangjinyan
 * @date 2016年8月15日 下午4:12:07
 * 
 **/
public class ZookeeperUtils
{
    
    /**
     * @Fields nameSpace : 使用es
     **/
    private final String esNameSpace = "es";
    
    /**
     * @Fields nameSpace : 使用hbase
     **/
    private final String hbaseNameSpace = "hbase";
    
    /**
     * @Fields zookeeper :
     **/
    private static ZooKeeperWatcher zookeeper;
    
    /**
     * @Fields metadataZNode : 元数据信息
     **/
    private String metadataZNode;
    
    /**
     * @Fields hbaseConfig : hbaseConfig
     **/
    private static Configuration hConfig = null;
    
    /**
     * LOG
     */
    private static final Log LOG = LogFactory.getLog(ZookeeperUtils.class);
    
    /**
     * @Fields cpm : 连接池
     **/
    private ConnectionPoolManager cpm = ConnectionPoolManager.getInstance();
    
    private static ZooKeeper zooK;
    
    private NPBaseConfiguration config = null;
    
    public void setConfig(NPBaseConfiguration config) {
        this.config = config;
    }
    
    
    /**
     * 创建一个新的实例 ZookeeperUtils.
     * <p>
     * Title:
     * </p>
     * <p>
     * Description:
     * </p>
     * 
     * @throws KeeperException
     * @throws ZooKeeperConnectionException
     * @throws IOException
     **/
    public ZookeeperUtils(NPBaseConfiguration config)
        throws KeeperException, ZooKeeperConnectionException, IOException
    {
        this.config = config;
        
     // 获取hbaseconfig信息
        hConfig = ESHbaseConnection.gethbaseConfig();
        
        try
        {
            zookeeper = new ZooKeeperWatcher(hConfig, "admin", null);
            Watcher watcher = new Watcher()
            {
                
                public void process(WatchedEvent event)
                {
                    LOG.info("process : " + event.getType());
                }
            };
            zooK = new ZooKeeper(config.getValue(NPBaseConfiguration.ZOOKEEPER_SERVER), Integer.parseInt(config.getValue(NPBaseConfiguration.ZOOKEEPER_SESSION_TIMEOUT)), watcher);
        }
        catch (ZooKeeperConnectionException e)
        {
            LOG.error(e.getMessage(), e);
        }
        catch (IOException e)
        {
            LOG.error(e.getMessage(), e);
        }
        
        
        metadataZNode = ZKUtil.joinZNode(zookeeper.baseZNode, "metadata");
        ZKUtil.createAndFailSilent(zookeeper, this.metadataZNode);
    }
    
    /**
     * 
     * @Title: setESTableDefine
     * @Description: TODO
     * @param table
     * @throws KeeperException
     */
    public void setESTableDefine(ESTable table)
        throws KeeperException
    {
        String tableName = table.getName();
        String namespacePath = ZKUtil.joinZNode(this.metadataZNode, esNameSpace);
        String tablePath = ZKUtil.joinZNode(namespacePath, tableName);
        Gson gson = new Gson();
        String data = gson.toJson(table);
        
        ZKUtil.createWithParents(this.zookeeper, tablePath, Bytes.toBytes(data));
    }
    
    /**
     * 
     * @Title: setHbaseTableDefine
     * @Description: TODO
     * @param table
     * @param columns
     * @throws KeeperException
     * @throws IOException
     */
    public void setHbaseTableDefine(HTable table, List<Column> columns)
        throws KeeperException, IOException
    {
        if (table == null)
        {
            throw new IllegalArgumentException("null pointer");
        }
        TableName tableName = table.getTableDescriptor().getTableName();
        String namespacePath = ZKUtil.joinZNode(this.metadataZNode, hbaseNameSpace);
        String tablePath = ZKUtil.joinZNode(namespacePath, tableName.getNameAsString());
        HbaseTable tableDef = new HbaseTable();
        tableDef.setColumnList(columns);
        tableDef.setName(tableName.getNameAsString());
        Gson gson = new Gson();
        String data = gson.toJson(tableDef);
        
        ZKUtil.createWithParents(this.zookeeper, tablePath, Bytes.toBytes(data));
    }
    
    /**
     * 
     * @Title: setHbaseTableDefine
     * @Description: TODO
     * @param table
     * @param columns
     * @throws KeeperException
     * @throws IOException
     */
    public void setHbaseTableDefine(HbaseTable table)
        throws KeeperException, IOException
    {
        if (table == null)
        {
            throw new IllegalArgumentException("null pointer");
        }
        String tableName = table.getName();
        String namespacePath = ZKUtil.joinZNode(this.metadataZNode, hbaseNameSpace);
        String tablePath = ZKUtil.joinZNode(namespacePath, tableName);
        Gson gson = new Gson();
        String data = gson.toJson(table);
        
        ZKUtil.createWithParents(this.zookeeper, tablePath, Bytes.toBytes(data));
    }
    
    /**
     * 
     * @Title: setESIndexDefine
     * @Description: TODO
     * @param tableName
     * @param index
     * @throws KeeperException
     * @throws IOException
     * @throws InterruptedException
     */
    public void setESIndexDefine(String tableName, Index index)
        throws KeeperException, IOException, InterruptedException
    {
        if (tableName == null)
        {
            throw new IllegalArgumentException("null tableName");
        }
        HbaseTable table = getHbaseTableDefWithName(tableName);
        
        table.setEsIndex(index);
        deleteAndSaveTable(tableName, table);
    }
    
    /**
     * 
     * @Title: setGlobalIndexDefine
     * @Description: TODO
     * @param tableName
     * @param index
     * @throws KeeperException
     * @throws IOException
     * @throws InterruptedException
     */
    public void setGlobalIndexDefine(String tableName, Index index)
        throws KeeperException, IOException, InterruptedException
    {
        if (tableName == null)
        {
            throw new IllegalArgumentException("null tableName");
        }
        HbaseTable table = getHbaseTableDefWithName(tableName);
        
        table.getGlobalIndexes().add(index);
        deleteAndSaveTable(tableName, table);
    }
    
    /**
     * 
     * @Title: deleteAndSaveTable
     * @Description: TODO
     * @param tableName
     * @param table
     * @throws IOException
     * @throws KeeperException
     */
    private void deleteAndSaveTable(String tableName, HbaseTable table)
        throws IOException, KeeperException
    {
        deleteTableDefine(tableName, hbaseNameSpace);
        
        String namespacePath = ZKUtil.joinZNode(this.metadataZNode, hbaseNameSpace);
        String tablePath = ZKUtil.joinZNode(namespacePath, tableName);
        Gson gson = new Gson();
        String data = gson.toJson(table);
        ZKUtil.createWithParents(this.zookeeper, tablePath, Bytes.toBytes(data));
    }
    
    /**
     * 
     * @Title: deleteESIndexDefine
     * @Description: TODO
     * @param tableName
     * @param indexName
     * @throws KeeperException
     * @throws IOException
     * @throws InterruptedException
     */
    public void deleteESIndexDefine(String tableName, String indexName)
        throws KeeperException, IOException, InterruptedException
    {
        if (tableName == null)
        {
            throw new IllegalArgumentException("null tableName");
        }
        HbaseTable table = getHbaseTableDefWithName(tableName);
        table.setEsIndex(null);
        
        deleteAndSaveTable(tableName, table);
    }
    
    /**
     * 
     * @Title: deleteGlobalIndexDefine
     * @Description: TODO
     * @param tableName
     * @param indexName
     * @throws KeeperException
     * @throws IOException
     * @throws InterruptedException
     */
    public void deleteGlobalIndexDefine(String tableName, String indexName)
        throws KeeperException, IOException, InterruptedException
    {
        if (tableName == null)
        {
            throw new IllegalArgumentException("null tableName");
        }
        HbaseTable table = getHbaseTableDefWithName(tableName);
        List<Index> indexList = table.getGlobalIndexes();
        Index oldIndex = null;
        for (int i = 0; i < indexList.size(); i++)
        {
            oldIndex = indexList.get(i);
            if (oldIndex.getIndexName().equals(indexName))
            {
                indexList.remove(i);
            }
        }
        deleteAndSaveTable(tableName, table);
    }
    
    /**
     * 
     * @Title: getFullTableDefine
     * @Description: TODO
     * @param table
     * @return
     * @throws IOException
     * @throws KeeperException
     * @throws InterruptedException
     */
    public HbaseTable getFullTableDefine(HTable table)
        throws IOException, KeeperException, InterruptedException
    {
        if (table == null)
        {
            throw new IllegalArgumentException("null pointer");
        }
        TableName tableName = table.getName();
        HbaseTable tableDef = (HbaseTable)getTableDefWithName(tableName.getNameAsString(), hbaseNameSpace);
        
        return tableDef;
    }
    
    /**
     * 
     * @Title: getHbaseTableDefWithName
     * @Description: TODO
     * @param tableName
     * @return
     * @throws KeeperException
     * @throws InterruptedException
     */
    public HbaseTable getHbaseTableDefWithName(String tableName)
        throws KeeperException, InterruptedException
    {
        String tablePath = getMetaPathInZK(tableName, hbaseNameSpace);
        String tableString = Bytes.toString(ZKUtil.getData(zookeeper, tablePath));
        Gson gson = new Gson();
        HbaseTable tableDef = gson.fromJson(tableString, HbaseTable.class);
        return tableDef;
    }
    
    /**
     * 
     * @Title: getESTableDefWithName
     * @Description: TODO
     * @param tableName
     * @return
     * @throws KeeperException
     * @throws InterruptedException
     */
    public ESTable getESTableDefWithName(String tableName)
        throws KeeperException, InterruptedException
    {
        String tablePath = getMetaPathInZK(tableName, esNameSpace);
        String tableString = Bytes.toString(ZKUtil.getData(zookeeper, tablePath));
        Gson gson = new Gson();
        ESTable tableDef = gson.fromJson(tableString, ESTable.class);
        return tableDef;
    }
    
    /**
     * 
     * @Title: getTableDefWithName
     * @Description: TODO
     * @param tableName
     * @param nameSpace
     * @return
     * @throws KeeperException
     * @throws InterruptedException
     */
    public Table getTableDefWithName(String tableName, String nameSpace)
        throws KeeperException, InterruptedException
    {
        String tablePath = getMetaPathInZK(tableName, nameSpace);
        String tableString = Bytes.toString(ZKUtil.getData(zookeeper, tablePath));
        Gson gson = new Gson();
        Table tableDef = null;
        if (esNameSpace.equals(nameSpace))
        {
            ESTable table = gson.fromJson(tableString, ESTable.class);
            tableDef = table;
        }
        else
        {
            HbaseTable table = gson.fromJson(tableString, HbaseTable.class);
            tableDef = table;
        }
        return tableDef;
    }
    
    /**
     * 
     * @Title: getTableDefine
     * @Description: TODO
     * @param table
     * @return
     * @throws IOException
     * @throws KeeperException
     * @throws InterruptedException
     */
    public List<Column> getTableDefine(HTable table)
        throws IOException, KeeperException, InterruptedException
    {
        return getFullTableDefine(table).getColumnList();
    }
    
    public String getMetaPathInZK(String tableName, String nameSpace)
    {
        if (tableName == null)
        {
            throw new IllegalArgumentException("null pointer");
        }
        String namespacePath = ZKUtil.joinZNode(this.metadataZNode, nameSpace);
        String tablePath = ZKUtil.joinZNode(namespacePath, tableName);
        return tablePath;
    }
    
    /**
     * 
     * @Title: updateTableDefine
     * @Description: TODO
     * @param table
     * @param columns
     * @throws IOException
     * @throws NoNodeException
     * @throws KeeperException
     */
    public void updateTableDefine(HTable table, List<Column> columns)
        throws IOException, NoNodeException, KeeperException
    {
        if ((table == null) || (columns == null))
        {
            throw new IllegalArgumentException("null pointer");
        }
        TableName tableName = table.getName();
        
        deleteTableDefine(tableName.getNameAsString(), hbaseNameSpace);
        setHbaseTableDefine(table, columns);
        
    }
    
    /**
     * 
     * @Title: deleteTableDefine
     * @Description: TODO
     * @param tableName
     * @param nameSpace
     * @throws IOException
     * @throws KeeperException
     */
    public void deleteTableDefineInZookeeper(String tableName, String type)
        throws IOException, KeeperException
    {
        if ("es".equals(type))
        {
            deleteTableDefine(tableName, esNameSpace);
        }
        else
        {
            deleteTableDefine(tableName, hbaseNameSpace);
        }
    }
    
    /**
     * 
     * @Title: deleteTableDefine
     * @Description: TODO
     * @param tableName
     * @param nameSpace
     * @throws IOException
     * @throws KeeperException
     */
    public void deleteTableDefine(String tableName, String nameSpace)
        throws IOException, KeeperException
    {
        if (tableName == null)
        {
            throw new IllegalArgumentException("null pointer");
        }
        String tablePath = getMetaPathInZK(tableName, nameSpace);
        ZKUtil.deleteNode(zookeeper, tablePath);
    }
    
    /**
     * 
     * @Title: setTableDefine
     * @Description: TODO
     * @param table
     * @throws KeeperException
     */
    public void setTableDefine(Table table)
        throws KeeperException
    {
        if (table instanceof HbaseTable)
        {
            String tableName = table.getName();
            String namespacePath = ZKUtil.joinZNode(this.metadataZNode, hbaseNameSpace);
            String tablePath = ZKUtil.joinZNode(namespacePath, tableName);
            Gson gson = new Gson();
            String data = gson.toJson(table);
            
            ZKUtil.createWithParents(this.zookeeper, tablePath, Bytes.toBytes(data));
        }
        else
        {
            setESTableDefine((ESTable)table);
        }
        
    }
    
    /**
     * @Title: getAllTableDefine
     * @Description: TODO
     * @return
     * @throws IOException
     * @throws KeeperException
     * @throws InterruptedException
     * @throws ClassNotFoundException
     * @throws BigDataWareHouseException
     **/
    public List<Table> getAllTableDefine()
        throws IOException, KeeperException, InterruptedException, ClassNotFoundException, BigDataWareHouseException
    {
        List<Table> tables = new ArrayList<Table>();
        ESHbaseMetaDataUtils.getInstance(config).setEsTables(getHalfTableDefine(esNameSpace));
        ESHbaseMetaDataUtils.getInstance(config).setHbaseTables(getHalfTableDefine(hbaseNameSpace));
        tables.addAll(ESHbaseMetaDataUtils.getInstance(config).getEsTables());
        tables.addAll(ESHbaseMetaDataUtils.getInstance(config).getHbaseTables());
        return tables;
    }
    
    public List<Table> getHalfTableDefine(String nameSpace)
        throws KeeperException, InterruptedException
    {
        List<Table> tables = new ArrayList<Table>();
        String esNamespacePath = ZKUtil.joinZNode(this.metadataZNode, nameSpace);
        List<String> esTableNames = ZKUtil.listChildrenNoWatch(zookeeper, esNamespacePath);
        Table esTable = null;
        if (esTableNames != null)
        {
            for (int i = 0; i < esTableNames.size(); i++)
            {
                esTable = getTableDefWithName(esTableNames.get(i), nameSpace);
                tables.add(esTable);
            }
        }
        return tables;
    }
    
    /**
     * @Title: getTable
     * @Description: TODO
     * @param tableName
     * @return
     * @throws ClassNotFoundException
     * @throws IOException
     * @throws KeeperException
     * @throws InterruptedException
     * @throws BigDataWareHouseException
     **/
    public Table getTable(String tableName)
        throws ClassNotFoundException, IOException, KeeperException, InterruptedException, BigDataWareHouseException
    {
        for (Table table : this.getAllTableDefine())
        {
            if (table.getName().equals(tableName))
            {
                return table;
            }
        }
        return null;
    }
    
    /**
     * 
     * <设置对应znode下的数据 , -1表示匹配所有版本>
     * <功能详细描述>
     * @param node
     * @param data
     * @see [类、类#方法、类#成员]
     */
    public static void setData(String node, String data)
    {
        Stat stat = null;
        try
        {
            stat = zooK.setData(node, data.getBytes(), -1);
        }
        catch (Exception e)
        {
            LOG.error(e.getMessage());
        }
        if(null != stat){
            LOG.info("exists result : {}"+stat.getVersion());
        }
    }
    
    /**
     * 
     * <获取某节点数据>
     * <功能详细描述>
     * @param node
     * @return
     * @see [类、类#方法、类#成员]
     */
    public static String getData(String node) {  
        String result = null;  
         try {  
             byte[] bytes = zooK.getData(node, null, null);  
             result = new String(bytes);  
        } catch (Exception e) {  
            LOG.error(e.getMessage());  
             return null;
        }  
         LOG.info("getdata result : {}"+result);  
         return result;
    }  
    
    public static void main(String[] args)
        throws ZooKeeperConnectionException, KeeperException, IOException, ClassNotFoundException,
        BigDataWareHouseException, InterruptedException
    {
////        ZookeeperUtils td = new ZookeeperUtils();
//        
//        // ConnectionPoolManager cpm = ConnectionPoolManager.getInstance();
//        // HTable table = (HTable)
//        // cpm.getConnection().getHbaseConnection().getTable(TableName.valueOf("zktest14"));
//        // List<Table> tables =
//        // ConfigurationUtil.getConfiguration().getTables().getTables();
//        // List<Column> columns = null;
//        // for (int i = 0; i < tables.size(); i++) {
//        // if(tables.get(i).getName().equals("zktest3")){
//        // columns = tables.get(i).getColumnList();
//        // }
//        // }
//        // td.setTableDefine(table, columns);
//        
//        // td.getTableDefine(table);
//        
//        // td.deleteIndexDefine("test6", "elasticsearch_index66");
//        
//        // td.getAllTableDefine();
    }
    
}
