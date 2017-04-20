/**
 * Copyright: Copyright (c) 2016 
 * Company:东方网力科技股份有限公司
 * 
 * @author huangjinyan
 * @date 2016年8月15日 下午1:45:19
 * @version V1.0
 */
package com.ning.hhbase.service;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.collections.CollectionUtils;
import org.apache.log4j.Logger;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.ning.hhbase.bean.Column;
import com.ning.hhbase.bean.Condition;
import com.ning.hhbase.bean.ESCountResult;
import com.ning.hhbase.bean.ESQueryResult;
import com.ning.hhbase.bean.ESQueryResultKV;
import com.ning.hhbase.bean.ESTable;
import com.ning.hhbase.bean.GlobalQueryResult;
import com.ning.hhbase.bean.GlobalQueryResultKV;
import com.ning.hhbase.bean.HbaseTable;
import com.ning.hhbase.bean.HiveQueryResult;
import com.ning.hhbase.bean.Index;
import com.ning.hhbase.bean.KeyValue;
import com.ning.hhbase.bean.KeyValueRange;
import com.ning.hhbase.bean.Limit;
import com.ning.hhbase.bean.ResultCode;
import com.ning.hhbase.bean.Sort;
import com.ning.hhbase.bean.Table;
import com.ning.hhbase.bean.TableJoinStruct;
import com.ning.hhbase.config.NPBaseConfiguration;
import com.ning.hhbase.exception.BigDataWareHouseException;
import com.ning.hhbase.tools.ESHbaseMetaDataUtils;
import com.ning.hhbase.tools.HbaseUtils;
import com.ning.hhbase.tools.HiveUtil;

/**
 * @ClassName: QueryService
 * @Description: 查询服务类
 * @author huangjinyan
 * @date 2016年8月15日 下午1:45:19
 *
 **/
public class QueryService {

    /**
     * oldEndKey
     */
    public static String oldEndKey = null;

    /**
     * @Fields LOG : 日志
     **/
    private static Logger LOG = Logger.getLogger(BaseDataService.class);

    private BaseDataService baseDataService = null;
    
    private BaseTableService baseTableService = null;
    
    private ESHbaseMetaDataUtils metaUtils = null;
    
    private HiveUtil hiveUtil = null;
    
    private HbaseUtils hbaseUtils = null;
    
    public QueryService(NPBaseConfiguration config){
        baseDataService = new BaseDataService(config);
        baseTableService = new BaseTableService(config);
        hiveUtil = new HiveUtil(config);
        metaUtils = ESHbaseMetaDataUtils.getInstance(config);
        hbaseUtils = new HbaseUtils(config);
    }
    
    public void setBaseDataService(BaseDataService baseDataService) {
        this.baseDataService = baseDataService;
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
    public  HiveQueryResult queryHivetableWithSql(String sql){
        ResultSet set = null;
        HiveQueryResult hqr = new HiveQueryResult();
        ResultCode rc = new ResultCode();
        
        try {
            set = hiveUtil.queryTableWithSql(sql,rc);
            try {
                hqr.setResult(hiveUtil.extractData(set));
            } catch (SQLException e) {
                rc.setResultCode(ResultCode.INTERNAL_ERROR);
                hqr.setException(new BigDataWareHouseException(e));
            }
            
        } catch (BigDataWareHouseException e) {
            rc.setResultCode(ResultCode.INTERNAL_ERROR);
            hqr.setException(e);
        }
        hqr.setStatusCode(rc.getResultCode());
        return hqr;
    }
    
    /**
     * @Title: queryTableWithRowkey
     * @Description: 查询rowkey对外接口
     * @param tableName
     * @param resultColumns
     * @param rows
     * @return
     */
    public  GlobalQueryResult queryTableWithRowkey(String tableName, String[] resultColumns, String[] rows){
        GlobalQueryResult gqr = new GlobalQueryResult();
        ResultCode rc = new ResultCode();
        try {
            gqr.setResult(baseDataService.queryTableWithRowkey(tableName, resultColumns, rows,rc));
        } catch (BigDataWareHouseException e) {
            if("0".equals(rc.getResultCode())){
                gqr.setStatusCode(ResultCode.INTERNAL_ERROR);
            }
            gqr.setException(e);
            return gqr;
        }
        gqr.setStatusCode(rc.getResultCode());
        return gqr;
    }
    
    /**
     * 
      * @Title: queryTableWithRowkeyReturnKV
      * @Description: TODO
      * @param tableName
      * @param resultColumns
      * @param rows
      * @return
     */
    public GlobalQueryResultKV queryTableWithRowkeyReturnKV(String tableName, String[] resultColumns, String[] rows) {
        GlobalQueryResultKV gqr = new GlobalQueryResultKV();
        ResultCode rc = new ResultCode();
        try {
            gqr.setResult(baseDataService.queryTableWithRowkeyReturnKV(tableName, resultColumns, rows,rc));
        } catch (BigDataWareHouseException e) {
            if("0".equals(rc.getResultCode())){
                gqr.setStatusCode(ResultCode.INTERNAL_ERROR);
            }
            gqr.setException(e);
            return gqr;
        }
        gqr.setStatusCode(rc.getResultCode());
        return gqr;
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
    public  ESQueryResult queryESTableWithFulltext(String tableName, String[] resultColumns, Condition condition, List<Sort> sortList, Limit limit) {
        return baseDataService.queryESTable(tableName, resultColumns, condition, sortList, limit, null,"fulltext");
    }
    
    /**
     * 
      * @Title: queryESTableWithFulltextReturnKV
      * @Description: TODO
      * @param tableName
      * @param resultColumns
      * @param condition
      * @param sortList
      * @param limit
      * @return
     */
    public ESQueryResultKV queryESTableWithFulltextReturnKV(String tableName, String[] resultColumns, Condition condition, List<Sort> sortList, Limit limit) {
        return baseDataService.queryESTableReturnKV(tableName, resultColumns, condition, sortList, limit, null,"fulltext");
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
    public  ESQueryResult queryESTableWithFulltext(String tableName, String[] resultColumns, Condition condition, List<Sort> sortList, Limit limit, String filterName) {
        return baseDataService.queryESTable(tableName, resultColumns, condition, sortList, limit, filterName,"fulltext");
    }
    
    /**
     * 
      * @Title: queryESTableWithFulltextReturnKV
      * @Description: TODO
      * @param tableName
      * @param resultColumns
      * @param condition
      * @param sortList
      * @param limit
      * @param filterName
      * @return
     */
    public ESQueryResultKV queryESTableWithFulltextReturnKV(String tableName, String[] resultColumns, Condition condition, List<Sort> sortList, Limit limit,
            String filterName) {
        return baseDataService.queryESTableReturnKV(tableName, resultColumns, condition, sortList, limit, filterName,"fulltext");
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
    public  ESQueryResult queryESTableWithKeyword(String tableName, String[] resultColumns, Condition condition, List<Sort> sortList, Limit limit) {
        return baseDataService.queryESTable(tableName, resultColumns, condition, sortList, limit, null,"keyword");
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
    public  ESQueryResult queryESTableWithKeyword(String tableName, String[] resultColumns, Condition condition, List<Sort> sortList, Limit limit, String filterName) {
        return baseDataService.queryESTable(tableName, resultColumns, condition, sortList, limit, filterName,"keyword");
    }
    
    /**
     * 
      * @Title: queryESTableWithKeywordReturnKV
      * @Description: TODO
      * @param tableName
      * @param resultColumns
      * @param condition
      * @param sortList
      * @param limit
      * @return
     */
    public ESQueryResultKV queryESTableWithKeywordReturnKV(String tableName, String[] resultColumns, Condition condition, List<Sort> sortList, Limit limit) {
        return baseDataService.queryESTableReturnKV(tableName, resultColumns, condition, sortList, limit, null,"keyword");
    }
    
    /**
     * 
      * @Title: queryESTableWithKeywordReturnKV
      * @Description: TODO
      * @param tableName
      * @param resultColumns
      * @param condition
      * @param sortList
      * @param limit
      * @param filterName
      * @return
     */
    public ESQueryResultKV queryESTableWithKeywordReturnKV(String tableName, String[] resultColumns, Condition condition, List<Sort> sortList, Limit limit,
            String filterName) {
        return baseDataService.queryESTableReturnKV(tableName, resultColumns, condition, sortList, limit, filterName,"keyword");
    }
    
    /**
     * 
      * @Title: queryESTableWithIKReturnKV
      * @Description: TODO
      * @param tableName
      * @param resultColumns
      * @param condition
      * @param sortList
      * @param limit
      * @return
     */
    public ESQueryResultKV queryESTableWithIKReturnKV(String tableName, String[] resultColumns, Condition condition, List<Sort> sortList, Limit limit) {
        return baseDataService.queryESTableReturnKV(tableName, resultColumns, condition, sortList, limit, null,"ik");
    }
    
    /**
     * 
      * @Title: queryESTableWithIKReturnKV
      * @Description: TODO
      * @param tableName
      * @param resultColumns
      * @param condition
      * @param sortList
      * @param limit
      * @param filterName
      * @return
     */
    public ESQueryResultKV queryESTableWithIKReturnKV(String tableName, String[] resultColumns, Condition condition, List<Sort> sortList, Limit limit,
            String filterName) {
        return baseDataService.queryESTableReturnKV(tableName, resultColumns, condition, sortList, limit, filterName,"ik");
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
    public  ESQueryResult queryESTableWithIK(String tableName, String[] resultColumns, Condition condition, List<Sort> sortList, Limit limit) {
        return baseDataService.queryESTable(tableName, resultColumns, condition, sortList, limit, null,"ik");
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
    public  ESQueryResult queryESTableWithIK(String tableName, String[] resultColumns, Condition condition, List<Sort> sortList, Limit limit, String filterName) {
        return baseDataService.queryESTable(tableName, resultColumns, condition, sortList, limit, filterName,"ik");
    }

    
    /**
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
        return baseDataService.queryMutiESTableReturnKV(struct, resultColumns, sortList, limit);
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
    public  ESCountResult countESTable(String tableName, Condition condition) {
        return baseDataService.countESTable(tableName, condition);
    }

    /**
     * @Title: rangeQueryGlobalTable
     * @Description: 范围查询二级索引(不分页)
     * @param tableName
     * @param resultColumns
     * @param keyValueRanges
     * @return
     */
    public  GlobalQueryResult rangeQueryGlobalTable(String tableName, String[] resultColumns, KeyValueRange[] keyValueRanges) {
        return baseDataService.rangeQueryGlobalTable(tableName, resultColumns, keyValueRanges, null);
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
        return baseDataService.rangeQueryGlobalTableReturnKV(tableName, resultColumns, keyValueRanges, null);
    }
    
    /**
     * @Title: rangeQueryGlobalTable
     * @Description: 范围查询二级索引(不分页)
     * @param tableName
     * @param resultColumns
     * @param keyValueRanges
     * @return
     */
    public  GlobalQueryResult rangeQueryGlobalTable(String tableName, String[] resultColumns, KeyValueRange[] keyValueRanges, String filterName) {
        return baseDataService.rangeQueryGlobalTable(tableName, resultColumns, keyValueRanges, filterName);
    }
    
    /**
     * @Title: rangeQueryGlobalTable
     * @Description: 范围查询二级索引(不分页)
     * @param tableName
     * @param resultColumns
     * @param keyValueRanges
     * @return
     */
    public  GlobalQueryResultKV rangeQueryGlobalTableReturnKV(String tableName, String[] resultColumns, KeyValueRange[] keyValueRanges, String filterName) {
        return baseDataService.rangeQueryGlobalTableReturnKV(tableName, resultColumns, keyValueRanges, filterName);
    }

    /**
     * @Title: queryGlobalTable
     * @Description: 二级索引定值查询接口(不分页)
     * @param tableName
     * @param resultColumns
     * @param keyValues
     * @return
     */
    public  GlobalQueryResult queryGlobalTable(String tableName, String[] resultColumns, KeyValue[] keyValues) {
        return baseDataService.queryGlobalTable(tableName, resultColumns, keyValues, null);
    }
    
    /**
     * @Title: queryGlobalTable
     * @Description: 二级索引定值查询接口(不分页)
     * @param tableName
     * @param resultColumns
     * @param keyValues
     * @return
     */
    public  GlobalQueryResultKV queryGlobalTableReturnKV(String tableName, String[] resultColumns, KeyValue[] keyValues) {
        return baseDataService.queryGlobalTableReturnKV(tableName, resultColumns, keyValues, null);
    }

    /**
     * @Title: queryGlobalTable
     * @Description: 二级索引定值查询接口(不分页)
     * @param tableName
     * @param resultColumns
     * @param keyValues
     * @return
     */
    public  GlobalQueryResult queryGlobalTable(String tableName, String[] resultColumns, KeyValue[] keyValues, String filterName) {
        return baseDataService.queryGlobalTable(tableName, resultColumns, keyValues, filterName);
    }
    
    /**
     * @Title: queryGlobalTable
     * @Description: 二级索引定值查询接口(不分页)
     * @param tableName
     * @param resultColumns
     * @param keyValues
     * @return
     */
    public  GlobalQueryResultKV queryGlobalTableReturnKV(String tableName, String[] resultColumns, KeyValue[] keyValues, String filterName) {
        return baseDataService.queryGlobalTableReturnKV(tableName, resultColumns, keyValues, filterName);
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
    public  GlobalQueryResult rangeQueryGlobalTablePage(String tableName, String[] resultColumns, KeyValueRange[] keyValueRanges, String endkey, int limit) {
        return baseDataService.rangeQueryGlobalTablePage(tableName, resultColumns, keyValueRanges, endkey, limit, null);
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
    public  GlobalQueryResultKV rangeQueryGlobalTablePageReturnKV(String tableName, String[] resultColumns, KeyValueRange[] keyValueRanges, String endkey, int limit) {
        return baseDataService.rangeQueryGlobalTablePageReturnKV(tableName, resultColumns, keyValueRanges, endkey, limit, null);
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
    public  GlobalQueryResult rangeQueryGlobalTablePage(String tableName, String[] resultColumns, KeyValueRange[] keyValueRanges, String endkey, int limit, String filterName) {
        return baseDataService.rangeQueryGlobalTablePage(tableName, resultColumns, keyValueRanges, endkey, limit, filterName);
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
    public  GlobalQueryResultKV rangeQueryGlobalTablePageReturnKV(String tableName, String[] resultColumns, KeyValueRange[] keyValueRanges, String endkey, int limit, String filterName) {
        return baseDataService.rangeQueryGlobalTablePageReturnKV(tableName, resultColumns, keyValueRanges, endkey, limit, filterName);
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
    public  GlobalQueryResult queryGlobalTablePage(String tableName, String[] resultColumns, KeyValue[] keyValues, String endkey, int limit) {
        return baseDataService.queryGlobalTablePage(tableName, resultColumns, keyValues, endkey, limit, null);
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
    public  GlobalQueryResultKV queryGlobalTablePageReturnKV(String tableName, String[] resultColumns, KeyValue[] keyValues, String endkey, int limit) {
        return baseDataService.queryGlobalTablePageReturnKV(tableName, resultColumns, keyValues, endkey, limit, null);
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
    public  GlobalQueryResult queryGlobalTablePage(String tableName, String[] resultColumns, KeyValue[] keyValues, String endkey, int limit, String filterName) {
        return baseDataService.queryGlobalTablePage(tableName, resultColumns, keyValues, endkey, limit, filterName);
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
    public  GlobalQueryResultKV queryGlobalTablePageReturnKV(String tableName, String[] resultColumns, KeyValue[] keyValues, String endkey, int limit, String filterName) {
        return baseDataService.queryGlobalTablePageReturnKV(tableName, resultColumns, keyValues, endkey, limit, filterName);
    }
    
    /**
     * @Title: getAllIndex
     * @Description: 获取该表的所有索引
     * @param table
     * @throws IOException
     **/
    public List<Index> getAllIndex(Table table) throws IOException {
        // 入口日志
        if (LOG.isDebugEnabled()) {
            LOG.debug("Enter getIndex() method");
        }

        // 参数日志
        if (LOG.isDebugEnabled()) {
            StringBuffer buffer = new StringBuffer();
            buffer.append("table=");
            buffer.append(table);
            LOG.debug(buffer.toString());
        }

        List<Index> allIndex = new ArrayList<Index>();
        if (table != null && table instanceof HbaseTable) {
            HbaseTable hbaseTable = (HbaseTable) table;
            if (null != hbaseTable.getEsIndex()) {
                allIndex.add(hbaseTable.getEsIndex());
            }

            if (null != hbaseTable.getGlobalIndexes()) {
                allIndex.addAll(hbaseTable.getGlobalIndexes());
            }
        }

        if (table != null && table instanceof ESTable) {
            ESTable esTable = (ESTable) table;
            Index index = new Index();
            index.setIndexName(esTable.getIndexName());
            index.setIndexType("es");
            index.setIndexColumnList(table.getColumnList());
            allIndex.add(index);
        }

        LOG.debug("index size:" + allIndex.size());

        // 出口日志
        if (LOG.isDebugEnabled()) {
            LOG.debug("Exit getIndex() method");
        }
        return allIndex;
    }

    /**
     * @Title: queryType
     * @Description: 查询方式
     * @param table table
     * @return String
     * @throws IOException
     */
    public JSONArray queryType(Table table) throws IOException {
        // 入口日志
        if (LOG.isDebugEnabled()) {
            LOG.debug("Enter queryType() method");
        }

        // 参数日志
        if (LOG.isDebugEnabled()) {
            StringBuffer buffer = new StringBuffer();
            buffer.append("table=");
            buffer.append(table);
            LOG.debug(buffer.toString());
        }

        JSONArray array = new JSONArray();
        List<String> types = baseTableService.supportedQueryTypes(table);
        for (int i = 0; i < types.size(); i++) {
            array.add(types.get(i));
        }

        // 出口日志
        if (LOG.isDebugEnabled()) {
            LOG.debug("Exit queryType() method");
        }

        return array;
    }

    /**
     * @Title: getGlobalIndexColumn
     * @Description: 获取二级索引列
     * @param tableName tableName
     * @param indexName indexName
     * @return
     * @throws BigDataWareHouseException
     **/
    public List<Column> getGlobalIndexColumn(String tableName, String indexName) throws BigDataWareHouseException {

        // 入口日志
        if (LOG.isDebugEnabled()) {
            LOG.debug("Enter getGlobalIndexColumn() method");
        }

        // 参数日志
        if (LOG.isDebugEnabled()) {
            StringBuffer buffer = new StringBuffer();
            buffer.append("tableName=");
            buffer.append(tableName);
            buffer.append("indexName=");
            buffer.append(indexName);
            LOG.debug(buffer.toString());
        }

        List<Column> columns = null;

        try {
            // 根据表名获取表
            Table table = metaUtils.getTable(tableName);

            // 二级索引必须是hbase表
            if (null != table && table instanceof HbaseTable) {
                HbaseTable hbaseTable = (HbaseTable) table;
                List<Index> indexs = hbaseTable.getGlobalIndexes();

                Index _index = null;
                if (CollectionUtils.isNotEmpty(indexs)) {
                    for (Index index : indexs) {
                        if (indexName.equals(index.getIndexName())) {
                            _index = index;
                            break;
                        }
                    }
                    columns = _index.getIndexColumnList();
                }
            }
        } catch (Exception e) {
            throw new BigDataWareHouseException(e);
        }

        // 出口日志
        if (LOG.isDebugEnabled()) {
            LOG.debug("Exit getGlobalIndexColumn() method");
        }

        return columns;
    }

    /**
     * @Title: getColumn
     * @Description: 获取表的列对象
     * @param tableName
     * @return
     * @throws BigDataWareHouseException
     **/
    public List<Column> getColumn(String tableName) throws BigDataWareHouseException {

        // 入口日志
        if (LOG.isDebugEnabled()) {
            LOG.debug("Enter getColumn() method");
        }

        // 参数日志
        if (LOG.isDebugEnabled()) {
            StringBuffer buffer = new StringBuffer();
            buffer.append("tableName=");
            buffer.append(tableName);
            LOG.debug(buffer.toString());
        }

        List<Column> columns = null;

        try {
            // 根据表名获取表
            Table table = metaUtils.getTable(tableName);

            // 根据具体的表来判定列
            if (null != table && table instanceof HbaseTable) {
                HbaseTable hbaseTable = (HbaseTable) table;
                columns = hbaseTable.getColumnList();
            } else if (table != null && table instanceof ESTable) {
                List<Column> _columns = null;
                ESTable esTable = (ESTable) table;
                _columns = esTable.getColumnList();
                Column col = new Column();
                col.setName("_id");
                col.setRowKey(true);
                col.setType("string");
                columns = new ArrayList<Column>();
                columns.add(col);
                columns.addAll(_columns);
            }
        } catch (Exception e) {
            throw new BigDataWareHouseException(e);
        }

        // 出口日志
        if (LOG.isDebugEnabled()) {
            LOG.debug("Exit getGlobalColumn() method");
        }

        return columns;
    }


   

    /**
     * @Title: addData
     * @Description: 数据新增
     * @param table table
     * @param jsonRes jsonRes
     * @return rs
     * @throws Exception Exception
     **/
    public String addData(Table table, String jsonRes) throws Exception {
        // 入口日志
        if (LOG.isDebugEnabled()) {
            LOG.debug("Enter addData() method");
        }
        // 参数日志
        if (LOG.isDebugEnabled()) {
            StringBuffer buffer = new StringBuffer();
            buffer.append("table=");
            buffer.append(table);
            buffer.append("jsonRes=");
            buffer.append(jsonRes);
            LOG.debug(buffer.toString());
        }

        // 返回参数
        String returnStr = null;
        JSONObject jobj = JSON.parseObject(jsonRes);
        List<Column> colList = table.getColumnList();

        if (table instanceof ESTable) {
            List<Object[]> dataList = new ArrayList<>();
            String[] cols = new String[colList.size()];
            String[] data = new String[colList.size()];
            String val = null;
            for (int i = 0; i < colList.size(); i++) {
                val = jobj.getString(colList.get(i).getName());

                data[i] = new String(val.getBytes("gbk"), "gbk");
                cols[i] = colList.get(i).getName();
            }
            dataList.add(data);
            try {
                baseDataService.insertES(table.getName(), cols, dataList);
                returnStr = "success";
            } catch (Exception e) {
                returnStr = "插入失败:" + e.getMessage();
            }
        } else {

            String rowKey = null;
            List<Column> allcols = table.getColumnList();
            for (int i = 0; i < allcols.size(); i++) {
                if (allcols.get(i).isRowKey()) {
                    rowKey = allcols.get(i).getName();
                }
            }

            if (jobj.getString(rowKey) == null || "".equals(jobj.getString(rowKey))) {
                returnStr = "主键为空！";
            }

            List<String> rowkeys = new ArrayList<String>();
            rowkeys.add(jobj.getString(rowKey));
            List<Object[]> result = null;
            try {
                result = hbaseUtils.selectWithRowkeys(table.getName(), rowkeys, table.getColumnsStringArray(), null, null);
            } catch (Exception e) {
                returnStr = "查询rowkey失败！:" + e.getMessage();
            }
            if (result != null && result.size() > 0) {
                returnStr = "主键已存在！";
            } else {

                List<Object[]> dataList = new ArrayList<>();
                String[] cols = new String[colList.size()];
                String[] data = new String[colList.size()];
                String val = null;
                for (int i = 0; i < colList.size(); i++) {
                    val = jobj.getString(colList.get(i).getName());

                    data[i] = new String(val.getBytes("gbk"), "gbk");
                    cols[i] = colList.get(i).getName();
                }
                dataList.add(data);

                try {
                    baseDataService.insert(table.getName(), cols, dataList);
                    returnStr = "success";
                } catch (Exception e) {
                    returnStr = "插入失败:" + e.getMessage();
                }
            }
        }

        // 出口日志
        if (LOG.isDebugEnabled()) {
            LOG.debug("Exit addData() method");
        }

        return returnStr;
    }

}
