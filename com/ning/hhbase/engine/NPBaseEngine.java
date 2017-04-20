/**
 * Copyright: Copyright (c) 2016 
 * Company:东方网力科技股份有限公司
 * 
 * @author huangjinyan
 * @date 2016年9月23日 下午3:01:39
 * @version V1.0
 */
package com.ning.hhbase.engine;

import java.io.UnsupportedEncodingException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import com.mysql.jdbc.PreparedStatement;
import com.ning.hhbase.bean.Condition;
import com.ning.hhbase.bean.ESCountResult;
import com.ning.hhbase.bean.ESInsertResult;
import com.ning.hhbase.bean.ESQueryResult;
import com.ning.hhbase.bean.ESQueryResultKV;
import com.ning.hhbase.bean.GlobalQueryResult;
import com.ning.hhbase.bean.GlobalQueryResultKV;
import com.ning.hhbase.bean.HbaseInsertResult;
import com.ning.hhbase.bean.HiveQueryResult;
import com.ning.hhbase.bean.KeyValue;
import com.ning.hhbase.bean.KeyValueRange;
import com.ning.hhbase.bean.Limit;
import com.ning.hhbase.bean.Sort;
import com.ning.hhbase.bean.Table;
import com.ning.hhbase.bean.TableJoinStruct;
import com.ning.hhbase.config.NPBaseConfiguration;
import com.ning.hhbase.connection.ConnectionPoolManager;
import com.ning.hhbase.connection.ESHbaseConnection;
import com.ning.hhbase.exception.BigDataWareHouseException;
import com.ning.hhbase.service.BaseDataService;
import com.ning.hhbase.service.BaseTableService;
import com.ning.hhbase.service.QueryService;
import com.ning.hhbase.tools.DatabaseMetaDateUtils;
import com.ning.hhbase.tools.ESHbaseMetaDataUtils;
import com.ning.hhbase.tools.FiltersUtils;
import com.ning.hhbase.tools.LogUtils;

/**
 * @ClassName: NPBaseEngine
 * @Description: TODO
 * @author huangjinyan
 * @date 2016年9月23日 下午3:01:39
 *
 **/
public final class NPBaseEngine {

    /**
     * @Fields log : 日志
     **/
    private static Logger log = Logger.getLogger(NPBaseEngine.class);

    private static NPBaseConfiguration config = null;

    private static NPBaseEngine engine = new NPBaseEngine();

    private static QueryService queryService = null;
    private static BaseDataService baseDataService = null;
    private static BaseTableService baseTableService = null;
    private static DatabaseMetaDateUtils databaseMetaData = null;
    private static NPBaseMetaData meta = null;

    public static void build(NPBaseConfiguration config) throws BigDataWareHouseException {
        NPBaseEngine.config = config;

        queryService = new QueryService(config);
        baseDataService = new BaseDataService(config);
        baseTableService = new BaseTableService(config);
        queryService.setBaseDataService(baseDataService);
        databaseMetaData = DatabaseMetaDateUtils.getInstance(config);
        meta = new NPBaseMetaData(config);
        
        checkConfigValues(config);

        initComponents(config);
        
        
        
    }

    private static void checkConfigValues(NPBaseConfiguration config) throws BigDataWareHouseException {
        checkSingleValue(config, NPBaseConfiguration.HIVE_URL);
        checkSingleValue(config, NPBaseConfiguration.HIVE_THRIFT_URL);
        checkSingleValue(config, NPBaseConfiguration.HIVEMYSQL_URL);
        checkSingleValue(config, NPBaseConfiguration.HIVEMYSQL_USER);
        checkSingleValue(config, NPBaseConfiguration.HIVEMYSQL_PASSWORD);
        checkSingleValue(config, NPBaseConfiguration.HDFS_URL);
        checkSingleValue(config, NPBaseConfiguration.MYCAT_SWITCH);
        checkSingleValue(config, NPBaseConfiguration.ZOOKEEPER_SERVER);
        checkSingleValue(config, NPBaseConfiguration.ESSWITCH);
        checkSingleValue(config, NPBaseConfiguration.HBASE_HOST);
        checkSingleValue(config, NPBaseConfiguration.HBASE_ZOOKEEPERIP);
        checkSingleValue(config, NPBaseConfiguration.HBASE_ZOOKEEPERPORT);
    }

    private static void checkSingleValue(NPBaseConfiguration config, String key) throws BigDataWareHouseException {
        if (config.getValue(key) == null) {
            LogUtils.errorMsg(BigDataWareHouseException.CONFIG_IS_NULL, key);
        }
    }

    private static void initComponents(NPBaseConfiguration config) throws BigDataWareHouseException {
        // 初始成功以后再初始化 NPBaseEngine
        try {
            // 初始化连接池
            log.debug("==============init pool start==================");
            ConnectionPoolManager.getInstance();
            log.debug("==============init pool end==================");
        } catch (Exception e) {
            log.error("==============初始化连接池异常");
            LogUtils.errorMsg(e, e.getMessage());
        }

        try {
            // 初始化zookeeper上的元数据信息
            log.debug("==============初始化zookeeper上的元数据信息 start==================");
            ESHbaseMetaDataUtils.getInstance(config).init();
            log.debug("==============初始化zookeeper上的元数据信息 end==================");
        } catch (Exception e) {
            log.error("==============初始化zookeeper上的元数据信息异常");
            LogUtils.errorMsg(e, e.getMessage());
        }

        try {
            // 如果开过关掉，不初始化mycat集群信息
            if (Boolean.parseBoolean(config.getValue(NPBaseConfiguration.MYCAT_SWITCH))) {
                // 初始化mycat的元数据信息
                log.debug("==============初始化mysql的表名信息 start==================");
                DatabaseMetaDateUtils.getInstance(config).init();
                log.debug("==============初始化mysql的表名信息 end==================");
            }
        } catch (Exception e) {
            log.error("==============初始化mycat的元数据信息异常");
            log.error(e.getMessage(), e);
        }

        if ("true".equals(config.getValue(NPBaseConfiguration.USE_NPBASE_FILTER))) {
            try {
                // 加载过滤器
                log.debug("==============加载过滤器 start==================");
                FiltersUtils.getInstance().loadConfiguration();
                log.debug("==============加载过滤器 end==================");
            } catch (Exception e) {
                log.error("==============加载过滤器异常");
                LogUtils.errorMsg(e, e.getMessage());
            }
        }
    }

    private NPBaseEngine() {
    }

    public static NPBaseEngine getSingleInstance() throws BigDataWareHouseException {

        if (config == null) {
            // 抛异常
            throw new BigDataWareHouseException(BigDataWareHouseException.NPBASE_NOT_INIT);
        }

        if (engine == null) {
            engine = new NPBaseEngine();
        }

        return engine;
    }

    public NPBaseMetaData getNPBaseMetaData() {
        return meta;
    }

    public static NPBaseConfiguration getConfig() {
        return config;
    }

    /**
     * 获取连接
     * 
     * @Title: getConnection
     * @Description: TODO
     * @return
     * @throws BigDataWareHouseException
     */
    public ESHbaseConnection getConnection() throws BigDataWareHouseException {
        return ConnectionPoolManager.getInstance().getConnection();
    }

    /**
     * 获取连接
     * 
     * @Title: getConnection
     * @Description: TODO
     * @return
     * @throws BigDataWareHouseException
     */
    public void releaseConnection(ESHbaseConnection connection) throws BigDataWareHouseException {
        ConnectionPoolManager.getInstance().close(connection);
    }

    /**
     * @Title: queryTableWithRowkey
     * @Description: 查询rowkey对外接口
     * @param tableName
     * @param resultColumns
     * @param rows
     * @return
     * @return
     */
    public HiveQueryResult queryHivetableWithSql(String sql) {
        return queryService.queryHivetableWithSql(sql);
    }

    /**
     * @Title: queryTableWithRowkey
     * @Description: 查询rowkey对外接口
     * @param tableName
     * @param resultColumns
     * @param rows
     * @return
     */
    public GlobalQueryResult queryTableWithRowkey(String tableName, String[] resultColumns, String[] rows) {
        return queryService.queryTableWithRowkey(tableName, resultColumns, rows);
    }
    
    /**
     * @Title: queryTableWithRowkey
     * @Description: 查询rowkey对外接口
     * @param tableName
     * @param resultColumns
     * @param rows
     * @return
     */
    public GlobalQueryResultKV queryTableWithRowkeyReturnKV(String tableName, String[] resultColumns, String[] rows) {
        return queryService.queryTableWithRowkeyReturnKV(tableName, resultColumns, rows);
    }

    /**
     * @Title: queryESTableWithFulltext
     * @Description: ES索引查询的对外接口
     * @param tableName
     * @param resultColumns
     * @param condition
     * @param sortList
     * @param limit
     * @return
     */
    public ESQueryResult queryESTableWithFulltext(String tableName, String[] resultColumns, Condition condition, List<Sort> sortList, Limit limit) {
        return queryService.queryESTableWithFulltext(tableName, resultColumns, condition, sortList, limit);
    }
    
    /**
     * @Title: queryESTableWithFulltext
     * @Description: ES索引查询的对外接口
     * @param tableName
     * @param resultColumns
     * @param condition
     * @param sortList
     * @param limit
     * @return
     */
    public ESQueryResultKV queryESTableWithFulltextReturnKV(String tableName, String[] resultColumns, Condition condition, List<Sort> sortList, Limit limit) {
        return queryService.queryESTableWithFulltextReturnKV(tableName, resultColumns, condition, sortList, limit);
    }
    
    /**
     * @Title: queryESTableWithFulltext
     * @Description: ES索引查询的对外接口
     * @param tableName
     * @param resultColumns
     * @param condition
     * @param sortList
     * @param limit
     * @return
     */
    public ESQueryResult queryESTableWithFulltext(String tableName, String[] resultColumns, Condition condition, List<Sort> sortList, Limit limit, String filterName) {
        return queryService.queryESTableWithFulltext(tableName, resultColumns, condition, sortList, limit, filterName);
    }
    
    /**
     * @Title: queryESTableWithFulltext
     * @Description: ES索引查询的对外接口
     * @param tableName
     * @param resultColumns
     * @param condition
     * @param sortList
     * @param limit
     * @return
     */
    public ESQueryResultKV queryESTableWithFulltextReturnKV(String tableName, String[] resultColumns, Condition condition, List<Sort> sortList, Limit limit, String filterName) {
        return queryService.queryESTableWithFulltextReturnKV(tableName, resultColumns, condition, sortList, limit, filterName);
    }
    
    /**
     * @Title: queryESTableWithKeyword
     * @Description: ES索引查询的对外接口
     * @param tableName
     * @param resultColumns
     * @param condition
     * @param sortList
     * @param limit
     * @return ESQueryResult
     */
    public ESQueryResult queryESTableWithKeyword(String tableName, String[] resultColumns, Condition condition, List<Sort> sortList, Limit limit) {
        return queryService.queryESTableWithKeyword(tableName, resultColumns, condition, sortList, limit);
    }
    
    /**
     * @Title: queryESTableWithKeywordReturnKV
     * @Description: ES索引查询的对外接口
     * @param tableName
     * @param resultColumns
     * @param condition
     * @param sortList
     * @param limit
     * @return ESQueryResultKV
     */
    public ESQueryResultKV queryESTableWithKeywordReturnKV(String tableName, String[] resultColumns, Condition condition, List<Sort> sortList, Limit limit) {
        return queryService.queryESTableWithKeywordReturnKV(tableName, resultColumns, condition, sortList, limit);
    }
    

    /**
     * @Title: queryESTableWithKeyword
     * @Description: ES索引查询的对外接口
     * @param tableName
     * @param resultColumns
     * @param condition
     * @param sortList
     * @param limit
     * @return
     */
    public ESQueryResult queryESTableWithKeyword(String tableName, String[] resultColumns, Condition condition, List<Sort> sortList, Limit limit, String filterName) {
        return queryService.queryESTableWithKeyword(tableName, resultColumns, condition, sortList, limit, filterName);
    }
    
    /**
     * @Title: queryESTableWithKeywordReturnKV
     * @Description: ES索引查询的对外接口
     * @param tableName
     * @param resultColumns
     * @param condition
     * @param sortList
     * @param limit
     * @return
     */
    public ESQueryResultKV queryESTableWithKeywordReturnKV(String tableName, String[] resultColumns, Condition condition, List<Sort> sortList, Limit limit, String filterName) {
        return queryService.queryESTableWithKeywordReturnKV(tableName, resultColumns, condition, sortList, limit, filterName);
    }

    /**
     * @Title: queryESTableWithIKReturnKV
     * @Description: ES索引查询的对外接口
     * @param tableName
     * @param resultColumns
     * @param condition
     * @param sortList
     * @param limit
     * @return
     */
    public ESQueryResultKV queryESTableWithIKReturnKV(String tableName, String[] resultColumns, Condition condition, List<Sort> sortList, Limit limit) {
        return queryService.queryESTableWithIKReturnKV(tableName, resultColumns, condition, sortList, limit);
    }
    
    /**
     * @Title: queryESTableWithIK
     * @Description: ES索引查询的对外接口
     * @param tableName
     * @param resultColumns
     * @param condition
     * @param sortList
     * @param limit
     * @return
     */
    public ESQueryResult queryESTableWithIK(String tableName, String[] resultColumns, Condition condition, List<Sort> sortList, Limit limit) {
        return queryService.queryESTableWithIK(tableName, resultColumns, condition, sortList, limit);
    }

    /**
     * @Title: ReturnKV
     * @Description: ES索引查询的对外接口
     * @param tableName
     * @param resultColumns
     * @param condition
     * @param sortList
     * @param limit
     * @return
     */
    public ESQueryResultKV queryESTableWithIKReturnKV(String tableName, String[] resultColumns, Condition condition, List<Sort> sortList, Limit limit, String filterName) {
        return queryService.queryESTableWithIKReturnKV(tableName, resultColumns, condition, sortList, limit,filterName);
    }
    
    /**
     * @Title: queryESTableWithIK
     * @Description: ES索引查询的对外接口
     * @param tableName
     * @param resultColumns
     * @param condition
     * @param sortList
     * @param limit
     * @return
     */
    public ESQueryResult queryESTableWithIK(String tableName, String[] resultColumns, Condition condition, List<Sort> sortList, Limit limit, String filterName) {
        return queryService.queryESTableWithIK(tableName, resultColumns, condition, sortList, limit, filterName);
    }

    /**
     * 关联查询多张表，返回最外层表的列结果
     * 
      * @Title: queryESTableReturnKV
      * @Description: TODO
      * @param tableName
      * @param resultColumns
      * @param condition
      * @param sortList
      * @param limit
      * @param filterName
      * @param string
      * @return
     */
    public ESQueryResultKV queryMutiESTableReturnKV(TableJoinStruct struct, String[] resultColumns,List<Sort> sortList, Limit limit) {
        return queryService.queryMutiESTableReturnKV(struct, resultColumns, sortList, limit);
    }
    
    /**
     * @Title: queryESTable
     * @Description: ES索引count条数的对外接口
     * @param tableName
     * @param resultColumns
     * @param condition
     * @param sortList
     * @param limit
     * @return
     */
    public ESCountResult countESTable(String tableName, Condition condition) {
        return queryService.countESTable(tableName, condition);
    }

    /**
     * @Title: rangeQueryGlobalTable
     * @Description: 范围查询二级索引(不分页)
     * @param tableName
     * @param resultColumns
     * @param keyValueRanges
     * @return
     */
    public GlobalQueryResult rangeQueryGlobalTable(String tableName, String[] resultColumns, KeyValueRange[] keyValueRanges) {
        return queryService.rangeQueryGlobalTable(tableName, resultColumns, keyValueRanges);
    }
    
    /**
     * @Title: rangeQueryGlobalTable
     * @Description: 范围查询二级索引(不分页)
     * @param tableName
     * @param resultColumns
     * @param keyValueRanges
     * @return
     */
    public GlobalQueryResultKV rangeQueryGlobalTableReturnKV(String tableName, String[] resultColumns, KeyValueRange[] keyValueRanges) {
        return queryService.rangeQueryGlobalTableReturnKV(tableName, resultColumns, keyValueRanges);
    }

    /**
     * @Title: rangeQueryGlobalTable
     * @Description: 范围查询二级索引(不分页)
     * @param tableName
     * @param resultColumns
     * @param keyValueRanges
     * @return
     */
    public GlobalQueryResult rangeQueryGlobalTable(String tableName, String[] resultColumns, KeyValueRange[] keyValueRanges, String filterName) {
        return queryService.rangeQueryGlobalTable(tableName, resultColumns, keyValueRanges, filterName);
    }
    
    /**
     * @Title: rangeQueryGlobalTable
     * @Description: 范围查询二级索引(不分页)
     * @param tableName
     * @param resultColumns
     * @param keyValueRanges
     * @return
     */
    public GlobalQueryResultKV rangeQueryGlobalTableReturnKV(String tableName, String[] resultColumns, KeyValueRange[] keyValueRanges, String filterName) {
        return queryService.rangeQueryGlobalTableReturnKV(tableName, resultColumns, keyValueRanges, filterName);
    }

    /**
     * @Title: queryGlobalTable
     * @Description: 二级索引定值查询接口(不分页)
     * @param tableName
     * @param resultColumns
     * @param keyValues
     * @return
     */
    public GlobalQueryResult queryGlobalTable(String tableName, String[] resultColumns, KeyValue[] keyValues) {
        return queryService.queryGlobalTable(tableName, resultColumns, keyValues);
    }
    
    /**
     * @Title: queryGlobalTable
     * @Description: 二级索引定值查询接口(不分页)
     * @param tableName
     * @param resultColumns
     * @param keyValues
     * @return
     */
    public GlobalQueryResultKV queryGlobalTableReturnKV(String tableName, String[] resultColumns, KeyValue[] keyValues) {
        return queryService.queryGlobalTableReturnKV(tableName, resultColumns, keyValues);
    }

    /**
     * @Title: queryGlobalTable
     * @Description: 二级索引定值查询接口(不分页)
     * @param tableName
     * @param resultColumns
     * @param keyValues
     * @return
     */
    public GlobalQueryResult queryGlobalTable(String tableName, String[] resultColumns, KeyValue[] keyValues, String filterName) {
        return queryService.queryGlobalTable(tableName, resultColumns, keyValues, filterName);
    }
    
    /**
     * @Title: queryGlobalTable
     * @Description: 二级索引定值查询接口(不分页)
     * @param tableName
     * @param resultColumns
     * @param keyValues
     * @return
     */
    public GlobalQueryResultKV queryGlobalTableReturnKV(String tableName, String[] resultColumns, KeyValue[] keyValues, String filterName) {
        return queryService.queryGlobalTableReturnKV(tableName, resultColumns, keyValues, filterName);
    }

    /**
     * @Title: queryGlobalTablePage
     * @Description: 二级索范围引查询接口(分页)
     * @param tableName
     * @param columns
     * @param keyValuesRange
     * @param endkey
     * @param limit
     * @return
     */
    public GlobalQueryResult rangeQueryGlobalTablePage(String tableName, String[] resultColumns, KeyValueRange[] keyValueRanges, String endkey, int limit) {
        return queryService.rangeQueryGlobalTablePage(tableName, resultColumns, keyValueRanges, endkey, limit);
    }
    
    /**
     * @Title: queryGlobalTablePage
     * @Description: 二级索范围引查询接口(分页)
     * @param tableName
     * @param columns
     * @param keyValuesRange
     * @param endkey
     * @param limit
     * @return
     */
    public GlobalQueryResultKV rangeQueryGlobalTablePageReturnKV(String tableName, String[] resultColumns, KeyValueRange[] keyValueRanges, String endkey, int limit) {
        return queryService.rangeQueryGlobalTablePageReturnKV(tableName, resultColumns, keyValueRanges, endkey, limit);
    }

    /**
     * @Title: queryGlobalTablePage
     * @Description: 二级索范围引查询接口(分页)
     * @param tableName
     * @param columns
     * @param keyValuesRange
     * @param endkey
     * @param limit
     * @return
     */
    public GlobalQueryResult rangeQueryGlobalTablePage(String tableName, String[] resultColumns, KeyValueRange[] keyValueRanges, String endkey, int limit, String filterName) {
        return queryService.rangeQueryGlobalTablePage(tableName, resultColumns, keyValueRanges, endkey, limit, filterName);
    }
    
    /**
     * @Title: queryGlobalTablePage
     * @Description: 二级索范围引查询接口(分页)
     * @param tableName
     * @param columns
     * @param keyValuesRange
     * @param endkey
     * @param limit
     * @return
     */
    public GlobalQueryResultKV rangeQueryGlobalTablePageReturnKV(String tableName, String[] resultColumns, KeyValueRange[] keyValueRanges, String endkey, int limit, String filterName) {
        return queryService.rangeQueryGlobalTablePageReturnKV(tableName, resultColumns, keyValueRanges, endkey, limit, filterName);
    }

    /**
     * @Title: queryGlobalTablePage
     * @Description: 二级索引定值查询接口(分页)
     * @param tableName
     * @param columns
     * @param keyValues
     * @param endkey
     * @param limit
     * @return
     */
    public GlobalQueryResult queryGlobalTablePage(String tableName, String[] resultColumns, KeyValue[] keyValues, String endkey, int limit) {
        return queryService.queryGlobalTablePage(tableName, resultColumns, keyValues, endkey, limit);
    }
    
    /**
     * @Title: queryGlobalTablePage
     * @Description: 二级索引定值查询接口(分页)
     * @param tableName
     * @param columns
     * @param keyValues
     * @param endkey
     * @param limit
     * @return
     */
    public GlobalQueryResultKV queryGlobalTablePageReturnKV(String tableName, String[] resultColumns, KeyValue[] keyValues, String endkey, int limit) {
        return queryService.queryGlobalTablePageReturnKV(tableName, resultColumns, keyValues, endkey, limit);
    }

    /**
     * @Title: queryGlobalTablePage
     * @Description: 二级索引定值查询接口(分页)
     * @param tableName
     * @param columns
     * @param keyValues
     * @param endkey
     * @param limit
     * @return
     */
    public GlobalQueryResult queryGlobalTablePage(String tableName, String[] resultColumns, KeyValue[] keyValues, String endkey, int limit, String filterName) {
        return queryService.queryGlobalTablePage(tableName, resultColumns, keyValues, endkey, limit, filterName);
    }
    
    /**
     * @Title: queryGlobalTablePage
     * @Description: 二级索引定值查询接口(分页)
     * @param tableName
     * @param columns
     * @param keyValues
     * @param endkey
     * @param limit
     * @return
     */
    public GlobalQueryResultKV queryGlobalTablePageReturnKV(String tableName, String[] resultColumns, KeyValue[] keyValues, String endkey, int limit, String filterName) {
        return queryService.queryGlobalTablePageReturnKV(tableName, resultColumns, keyValues, endkey, limit, filterName);
    }

    /**
     * 
     * @Title: accessInsertES
     * @Description: TODO
     * @param table
     * @param columns
     * @param dataList
     * @param ec
     * @return
     * @throws BigDataWareHouseException
     */
    public boolean accessInsertES(Table table, String[] columns, List<Object[]> dataList, ESHbaseConnection ec) throws BigDataWareHouseException {
        return baseDataService.accessInsertES(table, columns, dataList, ec);
    }

    /**
     * @Title: insertES
     * @Description: 插入ES数据接口
     * @param tableName 插入表名
     * @param columns 插入数据列名
     * @param dataList 插入的数据
     * @return
     * @throws BigDataWareHouseException
     * @throws ClassNotFoundException
     * @throws InterruptedException
     * @throws UnsupportedEncodingException
     **/
    public ESInsertResult insertESData(String tableName, String[] columns, List<Object[]> dataList) {
        return baseDataService.insertESData(tableName, columns, dataList);
    }

    /**
     * @Title: insert
     * @Description: 插入数据接口
     * @param tableName 插入表名
     * @param columns 插入数据列名
     * @param dataList 插入的数据
     * @return 成功：true
     * @throws BigDataWareHouseException
     * @throws InterruptedException
     * @throws ClassNotFoundException
     * @throws UnsupportedEncodingException
     */
    public boolean accessInsert(Table table, String[] columns, List<Object[]> dataList, ESHbaseConnection ec) throws BigDataWareHouseException {
        return baseDataService.accessInsert(table, columns, dataList, ec);
    }

    /**
     * @Title: insertData
     * @Description: 插入数据接口
     * @param tableName 插入表名
     * @param columns 插入数据列名
     * @param dataList 插入的数据
     * @return
     * @throws BigDataWareHouseException
     * @throws InterruptedException
     * @throws ClassNotFoundException
     * @throws UnsupportedEncodingException
     */
    public HbaseInsertResult insertData(String tableName, String[] columns, List<Object[]> dataList) {
        return baseDataService.insertData(tableName, columns, dataList);
    }

    /**
     * @Title: getConnection
     * @Description: 获得连接
     * @return
     */
    public Connection getMyCatConnection() {
        return baseDataService.getMyCatConnection();
    }

    /**
     * @Title: save
     * @Description: 持久化
     * @param connection 连接
     * @param tableName 表名
     * @param dataList 数据
     * @param columns 列
     * @param columnLengthMap 列长度
     * @return count
     * @throws BigDataWareHouseException
     * @throws SQLException
     */
    public long insertMyCatData(Connection connection, String tableName, List<Object[]> dataList, String[] columns, Map<String, Integer> columnLengthMap) throws BigDataWareHouseException {
        return baseDataService.save(connection, tableName, dataList, columns, columnLengthMap);
    }

    /**
     * 
     * <批量保存mysql数据> <功能详细描述>
     * 
     * @param connection
     * @param tableName
     * @param inserSql
     * @param params
     * @return
     * @throws SQLException
     * @see [类、类#方法、类#成员]
     */
    public PreparedStatement saveBatch(Connection connection, String tableName, String inserSql, Object params[]) throws BigDataWareHouseException {
        return baseDataService.saveBatch(connection, tableName, inserSql, params);
    }

    /**
     * @Title: supportedQueryTypes
     * @Description: TODO
     * @param tableName
     * @return
     **/
    public List<String> supportedQueryTypes(String tableName) {
        return baseTableService.supportedQueryTypes(tableName);
    }
    
    
    public QueryService getQueryService() {
        return queryService;
    }

    public BaseDataService getBaseDataService() {
        return baseDataService;
    }

    public DatabaseMetaDateUtils getDatabaseMetaData() {
        return databaseMetaData;
    }

}
