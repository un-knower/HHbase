/**
 * Copyright: Copyright (c) 2016 
 * Company:东方网力科技股份有限公司
 * 
 * @author huangjinyan
 * @date 2016年8月15日 下午1:45:02
 * @version V1.0
 */
package com.ning.hhbase.service;

import java.io.UnsupportedEncodingException;
import java.sql.Connection;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.log4j.Logger;

import com.google.gson.Gson;
import com.mysql.jdbc.PreparedStatement;
import com.ning.hhbase.bean.Column;
import com.ning.hhbase.bean.Condition;
import com.ning.hhbase.bean.Conditions;
import com.ning.hhbase.bean.ESCountResult;
import com.ning.hhbase.bean.ESInsertResult;
import com.ning.hhbase.bean.ESQueryResult;
import com.ning.hhbase.bean.ESQueryResultKV;
import com.ning.hhbase.bean.ESTable;
import com.ning.hhbase.bean.GlobalQueryResult;
import com.ning.hhbase.bean.GlobalQueryResultKV;
import com.ning.hhbase.bean.GlobalQueryRowkeyPage;
import com.ning.hhbase.bean.HbaseInsertResult;
import com.ning.hhbase.bean.HbaseTable;
import com.ning.hhbase.bean.Index;
import com.ning.hhbase.bean.KVInterface;
import com.ning.hhbase.bean.KeyValue;
import com.ning.hhbase.bean.KeyValueRange;
import com.ning.hhbase.bean.Limit;
import com.ning.hhbase.bean.OrcTable;
import com.ning.hhbase.bean.ResultCode;
import com.ning.hhbase.bean.Sort;
import com.ning.hhbase.bean.Table;
import com.ning.hhbase.bean.TableJoin;
import com.ning.hhbase.bean.TableJoinStruct;
import com.ning.hhbase.config.NPBaseConfiguration;
import com.ning.hhbase.connection.ESHbaseConnection;
import com.ning.hhbase.exception.BigDataWareHouseException;
import com.ning.hhbase.tools.DateValidationUtils;
import com.ning.hhbase.tools.ESHbaseMetaDataUtils;
import com.ning.hhbase.tools.EsUtils;
import com.ning.hhbase.tools.HbaseUtils;
import com.ning.hhbase.tools.HiveUtil;
import com.ning.hhbase.tools.LogUtils;
import com.ning.hhbase.tools.MyCatUtils;
import com.ning.hhbase.tools.NumberValidationUtils;

import jodd.util.StringUtil;

/**
 * @ClassName: BaseDataService
 * @Description: 基本数据操作服务 移植原来DataApi
 * @author huangjinyan
 * @date 2016年8月15日 下午4:47:34
 *
 **/
public class BaseDataService {

    /**
     * @Fields LOG : 日志
     **/
    private static Logger LOG = Logger.getLogger(BaseDataService.class);

    private EsUtils esUtils = null;

    private HbaseUtils hbaseUtils = null;

    private ESHbaseMetaDataUtils metaUtils = null;

    private HiveUtil hiveUtil = null;

    private MyCatUtils myCatUtils = null;

    /**
     * @Fields df : 日期
     **/
    public static DateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    public BaseDataService(NPBaseConfiguration config) {
        metaUtils = ESHbaseMetaDataUtils.getInstance(config);
        hbaseUtils = new HbaseUtils(config);
        esUtils = new EsUtils(config);
        hiveUtil = new HiveUtil(config);
        myCatUtils = MyCatUtils.getInstance(config);

    }

    /**
     * @Title: deleteTable
     * @Description: TODO
     * @param table
     * @param rowKey
     * @return
     * @throws ClassNotFoundException
     * @throws BigDataWareHouseException
     * @throws InterruptedException
     **/
    public String deleteTable(Table table, String rowKey) throws BigDataWareHouseException {
        // 入口日志
        if (LOG.isDebugEnabled()) {
            LOG.debug("Enter deleteTable() method");
        }
        // 参数日志
        if (LOG.isDebugEnabled()) {
            StringBuffer buffer = new StringBuffer();
            buffer.append("rowKey=");
            buffer.append(rowKey);
            buffer.append("table=");
            buffer.append(table);
            LOG.debug(buffer.toString());
        }
        String msg = hbaseUtils.deleteData(table, rowKey);

        // 出口日志
        if (LOG.isDebugEnabled()) {
            LOG.debug("Exit deleteTable() method");
        }
        return msg;
    }

    public String deleteESTable(Table tableRes, String rowKey) throws BigDataWareHouseException {
        // 入口日志
        if (LOG.isDebugEnabled()) {
            LOG.debug("Enter deleteTable() method");
        }
        // 参数日志
        if (LOG.isDebugEnabled()) {
            StringBuffer buffer = new StringBuffer();
            buffer.append("rowKey=");
            buffer.append(rowKey);
            buffer.append("table=");
            buffer.append(tableRes);
            LOG.debug(buffer.toString());
        }

        String msg = esUtils.delete(tableRes, rowKey);
        esUtils.delete(tableRes, "");
        // 出口日志
        if (LOG.isDebugEnabled()) {
            LOG.debug("Exit deleteTable() method");
        }
        return msg;
    }

    /**
     * @Title: deleteTable
     * @Description: TODO
     * @param tableName
     * @param rowKey
     * @return
     * @throws ClassNotFoundException
     * @throws BigDataWareHouseException
     * @throws InterruptedException
     **/
    public String deleteTable(String tableName, String rowKey) throws BigDataWareHouseException {
        Table table = metaUtils.getTable(tableName);
        return deleteTable(table, rowKey);
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
        // 入口日志
        if (LOG.isDebugEnabled()) {
            LOG.debug("Enter insertES() method");
        }
        // 参数日志
        if (LOG.isDebugEnabled()) {
            StringBuffer buffer = new StringBuffer();
            buffer.append("table=");
            buffer.append(table);
            buffer.append("columns=");
            buffer.append(columns);
            buffer.append("dataListSize=");
            buffer.append(dataList.size());
            buffer.append("ec=");
            buffer.append(ec);
            LOG.debug(buffer.toString());
        }

        String returnMsg = "success";
        boolean returnFlag = false;

        String rowKey = null;
        List<Column> cols = table.getColumnList();
        for (int i = 0; i < cols.size(); i++) {
            if (cols.get(i).isRowKey()) {
                rowKey = cols.get(i).getName();
            }
        }

        // 执行插入方法
        returnMsg = esUtils.accessInsert((ESTable) table, columns, rowKey, dataList, ec);
        LOG.debug("==========ES插入数据状态：" + returnMsg);
        if ("success".equals(returnMsg)) {
            returnFlag = true;
        }

        // 出口日志
        if (LOG.isDebugEnabled()) {
            LOG.debug("Exit insertES() method");
        }
        return returnFlag;
    }

    /**
     * @Title: insertES
     * @Description: 插入ES数据接口
     * @param tableName
     *            插入表名
     * @param columns
     *            插入数据列名
     * @param dataList
     *            插入的数据
     * @return
     * @throws BigDataWareHouseException
     * @throws ClassNotFoundException
     * @throws InterruptedException
     * @throws UnsupportedEncodingException
     **/
    public boolean insertES(String tableName, String[] columns, List<Object[]> dataList) throws BigDataWareHouseException {
        // 入口日志
        if (LOG.isDebugEnabled()) {
            LOG.debug("Enter insertES() method");
        }
        // 参数日志
        if (LOG.isDebugEnabled()) {
            StringBuffer buffer = new StringBuffer();
            buffer.append("tableName=");
            buffer.append(tableName);
            buffer.append("columns=");
            buffer.append(columns);
            buffer.append("dataListSize=");
            buffer.append(dataList.size());
            LOG.debug(buffer.toString());
        }

        String returnMsg = "success";
        boolean returnFlag = true;
        ResultCode rc = new ResultCode();
        // 获取表的配置文件
        Table table = metaUtils.getTable(tableName);
        if (table == null) {
            LOG.debug("table:" + tableName + " not exists!");
            returnFlag = false;
        }

        // 检查表传入列是否合法，列是否存在
        returnMsg = commonCheckForInsert(tableName, columns, table, rc);
        if (!"success".equals(returnMsg)) {
            LOG.debug(returnMsg);
            returnFlag = false;
        }

        String rowKey = null;
        List<Column> cols = table.getColumnList();
        for (int i = 0; i < cols.size(); i++) {
            if (cols.get(i).isRowKey()) {
                rowKey = cols.get(i).getName();
            }
        }

        // 执行插入方法
        String msg = esUtils.insert((ESTable) table, columns, rowKey, dataList);

        LOG.debug("==========ES插入数据状态：" + msg);

        // 出口日志
        if (LOG.isDebugEnabled()) {
            LOG.debug("Exit insertES() method");
        }
        return returnFlag;
    }

    /**
     * @Title: insertES
     * @Description: 插入ES数据接口
     * @param tableName
     *            插入表名
     * @param columns
     *            插入数据列名
     * @param dataList
     *            插入的数据
     * @return
     * @throws BigDataWareHouseException
     * @throws ClassNotFoundException
     * @throws InterruptedException
     * @throws UnsupportedEncodingException
     **/
    public ESInsertResult insertESData(String tableName, String[] columns, List<Object[]> dataList) {
        // 入口日志
        if (LOG.isDebugEnabled()) {
            LOG.debug("Enter insertESData() method");
        }
        // 参数日志
        if (LOG.isDebugEnabled()) {
            StringBuffer buffer = new StringBuffer();
            buffer.append("tableName=");
            buffer.append(tableName);
            buffer.append("columns=");
            buffer.append(columns);
            buffer.append("dataListSize=");
            buffer.append(dataList.size());
            LOG.debug(buffer.toString());
        }

        ESInsertResult esqr = new ESInsertResult();
        ResultCode rc = new ResultCode();
        esqr.setStatusCode(rc.getResultCode());
        String returnMsg = "success";
        // 获取表的配置文件
        Table table = metaUtils.getTable(tableName);
        if (table == null) {
            LOG.debug("table:" + tableName + " not exists!");
            rc.setResultCode(ResultCode.TABLE_NOT_EXISTS);
            esqr.setStatusCode(rc.getResultCode());
            return esqr;
        }

        // 检查表传入列是否合法，列是否存在
        returnMsg = commonCheckForInsert(tableName, columns, table, rc);
        if (!"success".equals(returnMsg)) {
            LOG.debug(returnMsg);
            esqr.setStatusCode(rc.getResultCode());
            return esqr;
        }

        String rowKey = null;
        List<Column> cols = table.getColumnList();
        for (int i = 0; i < cols.size(); i++) {
            if (cols.get(i).isRowKey()) {
                rowKey = cols.get(i).getName();
            }
        }

        List<Object[]> dataListobj = new ArrayList<Object[]>();
        for (int i = 0; i < dataList.size(); i++) {
            dataListobj.add(dataList.get(i));
        }

        // 执行插入方法
        String msg = null;
        try {
            msg = esUtils.insert((ESTable) table, columns, rowKey, dataListobj);
        } catch (BigDataWareHouseException e) {
            rc.setResultCode(ResultCode.INTERNAL_ERROR);
            esqr.setStatusCode(rc.getResultCode());
            esqr.setException(e);
        }

        LOG.debug("==========ES插入数据状态：" + msg);
        if (!"success".equals(msg)) {
            rc.setResultCode(ResultCode.INTERNAL_ERROR);
            esqr.setStatusCode(rc.getResultCode());
        }

        // 出口日志
        if (LOG.isDebugEnabled()) {
            LOG.debug("Exit insertESData() method");
        }
        return esqr;
    }

    /**
     * 
     * @Title: deleteES
     * @Description: TODO
     * @param tableName
     * @param columns
     * @param dataList
     * @return
     * @throws BigDataWareHouseException
     */
    public boolean deleteES(String tableName, String[] columns, List<Object[]> dataList) throws BigDataWareHouseException {
        // 入口日志
        if (LOG.isDebugEnabled()) {
            LOG.debug("Enter deleteES() method");
        }
        // 参数日志
        if (LOG.isDebugEnabled()) {
            StringBuffer buffer = new StringBuffer();
            buffer.append("tableName=");
            buffer.append(tableName);
            buffer.append("columns=");
            buffer.append(columns);
            buffer.append("dataListSize=");
            buffer.append(dataList.size());
            LOG.debug(buffer.toString());
        }

        boolean returnFlag = true;
        int index = -1;
        String column = null;
        // 获取表的配置文件
        Table table = metaUtils.getTable(tableName);
        if (table == null) {
            LOG.debug("table:" + tableName + " not exists!");
            returnFlag = false;
        }

        List<Column> cols = table.getColumnList();
        outer: for (int i = 0; i < cols.size(); i++) {
            if (cols.get(i).isRowKey()) {
                for (int j = 0; j < columns.length; j++) {
                    if (cols.get(i).getName().equals(columns[j])) {
                        index = j;
                        column = columns[j];
                        break outer;
                    }
                }
            }
        }

        esUtils.batchDeleteES(tableName, column, dataList, index);
        // 出口日志
        if (LOG.isDebugEnabled()) {
            LOG.debug("Exit deleteES() method");
        }
        return returnFlag;
    }

    /**
     * @Title: insert
     * @Description: 插入数据接口
     * @param tableName
     *            插入表名
     * @param columns
     *            插入数据列名
     * @param dataList
     *            插入的数据
     * @return 成功：true
     * @throws BigDataWareHouseException
     * @throws InterruptedException
     * @throws ClassNotFoundException
     * @throws UnsupportedEncodingException
     */
    public boolean accessInsert(Table table, String[] columns, List<Object[]> dataList, ESHbaseConnection ec) throws BigDataWareHouseException {
        // 入口日志
        if (LOG.isDebugEnabled()) {
            LOG.debug("Enter insertES() method");
        }
        // 参数日志
        if (LOG.isDebugEnabled()) {
            StringBuffer buffer = new StringBuffer();
            buffer.append("table=");
            buffer.append(table);
            buffer.append("columns=");
            buffer.append(columns);
            buffer.append("dataListSize=");
            buffer.append(dataList.size());
            buffer.append("ec=");
            buffer.append(ec);
            LOG.debug(buffer.toString());
        }
        boolean returnFlag = false;

        String rowKey = null;
        List<Column> cols = table.getColumnList();
        boolean keyStructFlag = false;
        for (int i = 0; i < cols.size(); i++) {
            if (cols.get(i).isRowKey()) {
                rowKey = cols.get(i).getName();
                if (cols.get(i).isStruct()) {
                    keyStructFlag = true;
                }
            }
        }
        // 执行插入数据
        String msg = hbaseUtils.accessInsertToTable(table, columns, dataList, rowKey, keyStructFlag, ec);
        if ("success".equals(msg)) {
            returnFlag = true;
        }
        LOG.debug(" Hbase插入数据状态：" + msg);
        // 出口日志
        if (LOG.isDebugEnabled()) {
            LOG.debug("Exit insertES() method");
        }
        return returnFlag;
    }

    /**
     * @Title: insert
     * @Description: 插入数据接口
     * @param tableName
     *            插入表名
     * @param columns
     *            插入数据列名
     * @param dataList
     *            插入的数据
     * @return
     * @throws BigDataWareHouseException
     * @throws InterruptedException
     * @throws ClassNotFoundException
     * @throws UnsupportedEncodingException
     */
    public boolean insert(String tableName, String[] columns, List<Object[]> dataList) throws BigDataWareHouseException {
        // 入口日志
        if (LOG.isDebugEnabled()) {
            LOG.debug("Enter insertES() method");
        }
        // 参数日志
        if (LOG.isDebugEnabled()) {
            StringBuffer buffer = new StringBuffer();
            buffer.append("tableName=");
            buffer.append(tableName);
            buffer.append("columns=");
            buffer.append(columns);
            buffer.append("dataListSize=");
            buffer.append(dataList.size());
            LOG.debug(buffer.toString());
        }
        String returnMsg = "success";
        boolean returnFlag = true;
        ResultCode rc = new ResultCode();
        // 获取表的配置文件
        Table table = metaUtils.getTable(tableName);
        if (table == null) {
            LOG.debug("table:" + tableName + " not exists!");
            returnFlag = false;
        }

        // 检查表传入列是否合法，列是否存在
        returnMsg = commonCheckForInsert(tableName, columns, table, rc);
        if (!"success".equals(returnMsg)) {
            LOG.debug(returnMsg);
            returnFlag = false;
        }

        String rowKey = null;
        List<Column> cols = table.getColumnList();
        boolean keyStructFlag = false;
        for (int i = 0; i < cols.size(); i++) {
            if (cols.get(i).isRowKey()) {
                rowKey = cols.get(i).getName();
                if (cols.get(i).isStruct()) {
                    keyStructFlag = true;
                }
            }
        }
        // 执行插入数据
        String msg = hbaseUtils.insertToTable(table, columns, dataList, rowKey, keyStructFlag);

        LOG.debug(" Hbase插入数据状态：" + msg);
        // 出口日志
        if (LOG.isDebugEnabled()) {
            LOG.debug("Exit insertES() method");
        }
        return returnFlag;
    }

    /**
     * @Title: insertData
     * @Description: 插入数据接口
     * @param tableName
     *            插入表名
     * @param columns
     *            插入数据列名
     * @param dataList
     *            插入的数据
     * @return
     * @throws BigDataWareHouseException
     * @throws InterruptedException
     * @throws ClassNotFoundException
     * @throws UnsupportedEncodingException
     */
    public HbaseInsertResult insertData(String tableName, String[] columns, List<Object[]> dataList) {
        // 入口日志
        if (LOG.isDebugEnabled()) {
            LOG.debug("Enter insertData() method");
        }
        // 参数日志
        if (LOG.isDebugEnabled()) {
            StringBuffer buffer = new StringBuffer();
            buffer.append("tableName=");
            buffer.append(tableName);
            buffer.append("columns=");
            buffer.append(columns);
            buffer.append("dataListSize=");
            buffer.append(dataList.size());
            LOG.debug(buffer.toString());
        }
        String returnMsg = "success";
        HbaseInsertResult hir = new HbaseInsertResult();
        ResultCode rc = new ResultCode();
        hir.setStatusCode(rc.getResultCode());
        // 获取表的配置文件
        Table table = metaUtils.getTable(tableName);
        if (table == null) {
            LOG.debug("table:" + tableName + " not exists!");
            rc.setResultCode(ResultCode.TABLE_NOT_EXISTS);
            hir.setStatusCode(rc.getResultCode());
            return hir;
        }

        // 检查表传入列是否合法，列是否存在
        returnMsg = commonCheckForInsert(tableName, columns, table, rc);
        if (!"success".equals(returnMsg)) {
            LOG.debug(returnMsg);
            hir.setStatusCode(rc.getResultCode());
            return hir;
        }

        String rowKey = null;
        List<Column> cols = table.getColumnList();
        boolean keyStructFlag = false;
        for (int i = 0; i < cols.size(); i++) {
            if (cols.get(i).isRowKey()) {
                rowKey = cols.get(i).getName();
                if (cols.get(i).isStruct()) {
                    keyStructFlag = true;
                }
            }
        }
        List<Object[]> dataListobj = new ArrayList<Object[]>();
        for (int i = 0; i < dataList.size(); i++) {
            dataListobj.add(dataList.get(i));
        }

        // 执行插入数据
        String msg = null;
        try {
            msg = hbaseUtils.insertToTable(table, columns, dataListobj, rowKey, keyStructFlag);
        } catch (BigDataWareHouseException e) {
            rc.setResultCode(ResultCode.INTERNAL_ERROR);
            hir.setStatusCode(rc.getResultCode());
            hir.setException(e);
        }

        if (!"success".equals(msg)) {
            rc.setResultCode(ResultCode.INTERNAL_ERROR);
            hir.setStatusCode(rc.getResultCode());
        }

        LOG.debug(" Hbase插入数据状态：" + msg);
        // 出口日志
        if (LOG.isDebugEnabled()) {
            LOG.debug("Exit insertData() method");
        }
        return hir;
    }

    /**
     * 
     * @Title: delete
     * @Description: TODO
     * @param tableName
     * @param columns
     * @param dataList
     * @return
     * @throws BigDataWareHouseException
     */
    public boolean delete(String tableName, String[] columns, List<Object[]> dataList) throws BigDataWareHouseException {
        // 入口日志
        if (LOG.isDebugEnabled()) {
            LOG.debug("Enter delete() method");
        }
        // 参数日志
        if (LOG.isDebugEnabled()) {
            StringBuffer buffer = new StringBuffer();
            buffer.append("tableName=");
            buffer.append(tableName);
            buffer.append("columns=");
            buffer.append(columns);
            buffer.append("dataListSize=");
            buffer.append(dataList.size());
            LOG.debug(buffer.toString());
        }

        boolean returnFlag = true;
        String column = null;
        // 获取表的配置文件
        Table table = metaUtils.getTable(tableName);
        if (table == null) {
            LOG.debug("table:" + tableName + " not exists!");
            returnFlag = false;
        }

        // List<Column> cols = table.getColumnList();
        // outer: for (int i = 0; i < cols.size(); i++) {
        // if (cols.get(i).isRowKey()) {
        //
        // for (int j = 0; j < columns.length; j++) {
        // if (cols.get(i).getName().equals(columns[j])) {
        // index = j;
        // column = columns[j];
        // break outer;
        // }
        // }
        // }
        // }

        String rowKey = null;
        List<Column> cols = table.getColumnList();
        boolean keyStructFlag = false;
        for (int i = 0; i < cols.size(); i++) {
            if (cols.get(i).isRowKey()) {
                rowKey = cols.get(i).getName();
                if (cols.get(i).isStruct()) {
                    keyStructFlag = true;
                }
            }
        }

        hbaseUtils.batchDeleteData(table, columns, dataList, rowKey, keyStructFlag);
        // 出口日志
        if (LOG.isDebugEnabled()) {
            LOG.debug("Exit delete() method");
        }
        return returnFlag;
    }

    /**
     * @Title: queryTableWithRowkeyRange
     * @Description: TODO
     * @param tableName
     * @param columns
     * @param rows
     * @param rc
     * @return
     * @throws BigDataWareHouseException
     * @throws ClassNotFoundException
     * @throws InterruptedException
     **/
    public List<Object[]> queryTableWithRowkeyRange(String tableName, String[] columns, String startRow, String endRow, ResultCode rc)
            throws BigDataWareHouseException {
        // 入口日志
        if (LOG.isDebugEnabled()) {
            LOG.debug("Enter queryTableWithRowkeyRange() method");
        }
        // 参数日志
        if (LOG.isDebugEnabled()) {
            StringBuffer buffer = new StringBuffer();
            buffer.append("tableName=");
            buffer.append(tableName);
            buffer.append("columns=");
            buffer.append(columns);
            buffer.append("startRow=");
            buffer.append(startRow);
            buffer.append("endRow=");
            buffer.append(endRow);
            LOG.debug(buffer.toString());
        }
        String returnMsg = "success";
        // 获取表的配置文件
        Table table = metaUtils.getTable(tableName);
        if (table == null) {
            LOG.debug("table:" + tableName + " not exists!");
            rc.setResultCode(ResultCode.TABLE_NOT_EXISTS);
            LogUtils.errorMsg(BigDataWareHouseException.TABLE_NOT_EXISTS, tableName);
        }

        // 检查表传入列是否合法，列是否存在
        returnMsg = commonCheck(tableName, columns, table, rc);
        if (!"success".equals(returnMsg)) {
            LOG.debug(returnMsg);
            LogUtils.errorMsg(returnMsg);
        }

        List<Object[]> returnList = hbaseUtils.selectWithRowkeyRange(tableName, startRow, endRow, columns, null, null);

        // 出口日志
        if (LOG.isDebugEnabled()) {
            LOG.debug("Exit queryTableWithRowkeyRange() method");
        }
        // 参数日志
        if (LOG.isDebugEnabled()) {
            StringBuffer buffer = new StringBuffer();
            buffer.append("returnListSize=");
            buffer.append(returnList.size());
            LOG.debug(buffer.toString());
        }
        return returnList;
    }

    /**
     * @Title: queryTableWithNoRowkey
     * @Description: TODO
     * @param tableName
     * @param columns
     * @param rows
     * @param rc
     * @return
     * @throws BigDataWareHouseException
     * @throws ClassNotFoundException
     * @throws InterruptedException
     **/
    public List<Object[]> queryTableWithNoRowkey(String tableName, String[] columns, ResultCode rc) throws BigDataWareHouseException {
        // 入口日志
        if (LOG.isDebugEnabled()) {
            LOG.debug("Enter queryTableWithNoRowkey() method");
        }
        // 参数日志
        if (LOG.isDebugEnabled()) {
            StringBuffer buffer = new StringBuffer();
            buffer.append("tableName=");
            buffer.append(tableName);
            buffer.append("columns=");
            buffer.append(columns);
            LOG.debug(buffer.toString());
        }
        String returnMsg = "success";
        // 获取表的配置文件
        Table table = metaUtils.getTable(tableName);
        if (table == null) {
            LOG.debug("table:" + tableName + " not exists!");
            rc.setResultCode(ResultCode.TABLE_NOT_EXISTS);
            LogUtils.errorMsg(BigDataWareHouseException.TABLE_NOT_EXISTS, tableName);
        }

        // 检查表传入列是否合法，列是否存在
        returnMsg = commonCheck(tableName, columns, table, rc);
        if (!"success".equals(returnMsg)) {
            LOG.debug(returnMsg);
            LogUtils.errorMsg(returnMsg);
        }

        List<Object[]> returnList = hbaseUtils.scanTable(tableName, columns, null, rc);

        // 出口日志
        if (LOG.isDebugEnabled()) {
            LOG.debug("Exit queryTableWithNoRowkey() method");
        }
        // 参数日志
        if (LOG.isDebugEnabled()) {
            StringBuffer buffer = new StringBuffer();
            buffer.append("returnListSize=");
            buffer.append(returnList.size());
            LOG.debug(buffer.toString());
        }
        return returnList;
    }

    /**
     * @Title: queryTableWithRowkey
     * @Description: TODO
     * @param tableName
     * @param columns
     * @param rows
     * @param rc
     * @return
     * @throws BigDataWareHouseException
     * @throws ClassNotFoundException
     * @throws InterruptedException
     **/
    public List<Object[]> queryTableWithRowkey(String tableName, String[] columns, String[] rows, ResultCode rc) throws BigDataWareHouseException {
        // 入口日志
        if (LOG.isDebugEnabled()) {
            LOG.debug("Enter queryTableWithRowkey() method");
        }
        // 参数日志
        if (LOG.isDebugEnabled()) {
            StringBuffer buffer = new StringBuffer();
            buffer.append("tableName=");
            buffer.append(tableName);
            buffer.append("columns=");
            buffer.append(columns);
            buffer.append("rowsSize=");
            buffer.append(rows.length);
            LOG.debug(buffer.toString());
        }
        String returnMsg = "success";
        // 获取表的配置文件
        Table table = metaUtils.getTable(tableName);
        if (table == null) {
            LOG.debug("table:" + tableName + " not exists!");
            rc.setResultCode(ResultCode.TABLE_NOT_EXISTS);
            LogUtils.errorMsg(BigDataWareHouseException.TABLE_NOT_EXISTS, tableName);
        }

        // 检查表传入列是否合法，列是否存在
        returnMsg = commonCheck(tableName, columns, table, rc);
        if (!"success".equals(returnMsg)) {
            LOG.debug(returnMsg);
            LogUtils.errorMsg(returnMsg);
        }

        List<String> rowkeys = new ArrayList<String>();
        for (int i = 0; i < rows.length; i++) {
            rowkeys.add(rows[i]);
        }

        List<Object[]> returnList = hbaseUtils.selectWithRowkeys(tableName, rowkeys, columns, null, null);

        // 出口日志
        if (LOG.isDebugEnabled()) {
            LOG.debug("Exit queryTableWithRowkey() method");
        }
        // 参数日志
        if (LOG.isDebugEnabled()) {
            StringBuffer buffer = new StringBuffer();
            buffer.append("returnListSize=");
            buffer.append(returnList.size());
            LOG.debug(buffer.toString());
        }
        return returnList;
    }

    public List<Map<String, Object>> queryTableWithRowkeyReturnKV(String tableName, String[] columns, String[] rows, ResultCode rc)
            throws BigDataWareHouseException {
        // 入口日志
        if (LOG.isDebugEnabled()) {
            LOG.debug("Enter queryTableWithRowkeyReturnKV() method");
        }
        // 参数日志
        if (LOG.isDebugEnabled()) {
            StringBuffer buffer = new StringBuffer();
            buffer.append("tableName=");
            buffer.append(tableName);
            buffer.append("columns=");
            buffer.append(columns);
            buffer.append("rowsSize=");
            buffer.append(rows.length);
            LOG.debug(buffer.toString());
        }
        String returnMsg = "success";
        // 获取表的配置文件
        Table table = metaUtils.getTable(tableName);
        if (table == null) {
            LOG.debug("table:" + tableName + " not exists!");
            rc.setResultCode(ResultCode.TABLE_NOT_EXISTS);
            LogUtils.errorMsg(BigDataWareHouseException.TABLE_NOT_EXISTS, tableName);
        }

        // 检查表传入列是否合法，列是否存在
        returnMsg = commonCheck(tableName, columns, table, rc);
        if (!"success".equals(returnMsg)) {
            LOG.debug(returnMsg);
            LogUtils.errorMsg(returnMsg);
        }

        List<String> rowkeys = new ArrayList<String>();
        for (int i = 0; i < rows.length; i++) {
            rowkeys.add(rows[i]);
        }

        List<Map<String, Object>> returnList = hbaseUtils.selectWithRowkeysResultKV(tableName, rowkeys, columns, null, null);

        // 出口日志
        if (LOG.isDebugEnabled()) {
            LOG.debug("Exit queryTableWithRowkeyReturnKV() method");
        }
        // 参数日志
        if (LOG.isDebugEnabled()) {
            StringBuffer buffer = new StringBuffer();
            buffer.append("returnListSize=");
            buffer.append(returnList.size());
            LOG.debug(buffer.toString());
        }
        return returnList;
    }

    /**
     * @Title: queryTableWithES
     * @Description: 根据条件查询数据接口
     * @param tableName
     *            查询的表名
     * @param columns
     *            需要返回的列
     * @param orCondition
     *            查询条件
     * @param limit
     *            返回条数
     * @param rc
     * @return
     * @throws BigDataWareHouseException
     * @throws InterruptedException
     * @throws ClassNotFoundException
     */
    public List<Object[]> queryTableWithES(String tableName, String[] columns, Condition condition, List<Sort> sortList, Limit limit, AtomicLong total,
            ResultCode rc, String indexType) throws BigDataWareHouseException {
        return queryTableWithES(tableName, columns, condition, sortList, limit, total, rc, null, indexType);
    }

    /**
     * @Title: queryTableWithES
     * @Description: 根据条件查询数据接口
     * @param tableName
     *            查询的表名
     * @param columns
     *            需要返回的列
     * @param orCondition
     *            查询条件
     * @param limit
     *            返回条数
     * @param rc
     * @param filterName
     * @param indexType
     * @return
     * @throws BigDataWareHouseException
     * @throws InterruptedException
     * @throws ClassNotFoundException
     */
    public List<Object[]> queryTableWithES(String tableName, String[] columns, Condition condition, List<Sort> sortList, Limit limit, AtomicLong total,
            ResultCode rc, String filterName, String indexType) throws BigDataWareHouseException {
        // 入口日志
        if (LOG.isDebugEnabled()) {
            LOG.debug("Enter queryTableWithES() method");
        }
        // 参数日志
        if (LOG.isDebugEnabled()) {
            StringBuffer buffer = new StringBuffer();
            buffer.append("tableName=");
            buffer.append(tableName);
            buffer.append("columns=");
            buffer.append(columns);
            buffer.append("condition=");
            buffer.append(condition);
            buffer.append("sortList=");
            buffer.append(sortList);
            buffer.append("limit=");
            buffer.append(limit);
            buffer.append("total=");
            buffer.append(total);
            buffer.append("rc=");
            buffer.append(rc);
            buffer.append("filterName=");
            buffer.append(filterName);
            LOG.debug(buffer.toString());
        }

        String returnMsg = "success";

        // 获取表对象
        Table table = metaUtils.getTable(tableName);
        if (table == null) {
            rc.setResultCode(ResultCode.TABLE_NOT_EXISTS);
            LogUtils.errorMsg(BigDataWareHouseException.TABLE_NOT_EXISTS, tableName);
        }

        // 准备查询前对表，对字段，对返回列，对查询条件的各种验证
        returnMsg = prepareESQuery(table, "query", tableName, columns, condition, rc);
        if (!"success".equals(returnMsg)) {
            LogUtils.errorMsg(returnMsg);
        }
        List<Object[]> returnList = null;

        if (sortList == null) {
            sortList = new ArrayList<Sort>();
        }
        // if(limit == null){
        // limit = new Limit(0,10);
        // }

        // 执行对应的查询
        if (table instanceof ESTable) {
            LOG.debug("queryTableWithES() method, es table");
            ESTable esTable = (ESTable) table;

            if ((indexType != null && esTable.getIndexType() != null) && !indexType.equals(esTable.getIndexType())) {
                rc.setResultCode(ResultCode.INDEX_TYPE_NOT_MATCH);
                LogUtils.errorMsg(BigDataWareHouseException.INDEX_TYPE_NOT_MATCH);
            }
            // 查询ES hive表，直接获取结果
            returnList = esUtils.queryESTable(esTable, columns, condition, sortList, limit, total, filterName, rc);

            if (null != filterName) {
                // 如果查询结果为不符合limit长度，需要继续查询，如此操作是因为过滤器过滤数据导致结果集不符合limit长度
                while (null != returnList && returnList.size() > 0 && returnList.size() < limit.getCount()) {
                    limit.setIndex(limit.getIndex() + limit.getCount());
                    // 设置结果
                    returnList.addAll(esUtils.queryESTable(esTable, columns, condition, sortList, limit, total, filterName, rc));

                    List<Object[]> returnListTmp = esUtils.queryESTable(esTable, columns, condition, sortList, limit, total, null, rc);
                    // 如果不过滤时无查询结果，无需继续查询
                    if (null == returnListTmp || returnListTmp.size() <= 0) {
                        break;
                    }
                }
            }

        } else {
            HbaseTable hbaseTable = (HbaseTable) table;

            LOG.debug("queryTableWithES() method, hbase table");
            // 根据表名tableName和查询条件去ES查询出 limit条索引信息rowkey，得到rowkey信息
            List<String> rowkeys = esUtils.queryTable(hbaseTable, columns, condition, sortList, limit, total);

            // 根据表名tableName和得到的rowkey信息去HBase去查询数据，得到最终数据
            // 将最终数据封装返回
            returnList = hbaseUtils.selectWithRowkeys(tableName, rowkeys, columns, filterName, rc);

            // 如果查询结果为不符合limit长度，需要继续查询，如此操作是因为过滤器过滤数据导致结果集不符合limit长度
            if (null != filterName) {
                while (null != returnList && returnList.size() > 0 && returnList.size() < limit.getCount()) {
                    long tmp = limit.getCount() - returnList.size();
                    limit.setIndex(limit.getCount() + limit.getIndex());
                    rowkeys = esUtils.queryTable(hbaseTable, columns, condition, sortList, limit, total);
                    // 如果无查询结果，跳出循环
                    if (rowkeys.size() <= 0) {
                        break;
                    }
                    List<String> rowkeysTmp = new ArrayList<String>();
                    if (rowkeys.size() > tmp) {

                        for (int i = 0; i < tmp; i++) {
                            rowkeysTmp.add(rowkeys.get(i));
                        }
                    }
                    // 设置结果
                    returnList.addAll(hbaseUtils.selectWithRowkeys(tableName, rowkeysTmp, columns, filterName, rc));
                }
            }

        }
        // 出口日志
        if (LOG.isDebugEnabled()) {
            LOG.debug("Exit queryTableWithES() method");
        }
        // 参数日志
        if (LOG.isDebugEnabled()) {
            StringBuffer buffer = new StringBuffer();
            buffer.append("returnListSize=");
            buffer.append(returnList.size());
            LOG.debug(buffer.toString());
        }
        return returnList;
    }

    private List<Map<String, Object>> queryTableWithESResultKV(String tableName, String[] columns, Condition condition, List<Sort> sortList, Limit limit,
            AtomicLong total, ResultCode rc, String filterName, String indexType) throws BigDataWareHouseException {
        // 入口日志
        if (LOG.isDebugEnabled()) {
            LOG.debug("Enter queryTableWithES() method");
        }
        // 参数日志
        if (LOG.isDebugEnabled()) {
            StringBuffer buffer = new StringBuffer();
            buffer.append("tableName=");
            buffer.append(tableName);
            buffer.append("columns=");
            buffer.append(columns);
            buffer.append("condition=");
            buffer.append(condition);
            buffer.append("sortList=");
            buffer.append(sortList);
            buffer.append("limit=");
            buffer.append(limit);
            buffer.append("total=");
            buffer.append(total);
            buffer.append("rc=");
            buffer.append(rc);
            buffer.append("filterName=");
            buffer.append(filterName);
            LOG.debug(buffer.toString());
        }

        String returnMsg = "success";

        // 获取表对象
        Table table = metaUtils.getTable(tableName);
        if (table == null) {
            rc.setResultCode(ResultCode.TABLE_NOT_EXISTS);
            LogUtils.errorMsg(BigDataWareHouseException.TABLE_NOT_EXISTS, tableName);
        }

        // 准备查询前对表，对字段，对返回列，对查询条件的各种验证
        returnMsg = prepareESQuery(table, "query", tableName, columns, condition, rc);
        if (!"success".equals(returnMsg)) {
            LogUtils.errorMsg(returnMsg);
        }
        List<Map<String, Object>> returnList = null;

        if (sortList == null) {
            sortList = new ArrayList<Sort>();
        }
        // if(limit == null){
        // limit = new Limit(0,10);
        // }

        // 执行对应的查询
        if (table instanceof ESTable) {
            LOG.debug("queryTableWithES() method, es table");
            ESTable esTable = (ESTable) table;

            if (!indexType.equals(esTable.getIndexType())) {
                rc.setResultCode(ResultCode.INDEX_TYPE_NOT_MATCH);
                LogUtils.errorMsg(BigDataWareHouseException.INDEX_TYPE_NOT_MATCH);
            }
            // 查询ES hive表，直接获取结果
            returnList = esUtils.queryESTableResultKV(esTable, columns, condition, sortList, limit, total, filterName, rc);

            if (null != filterName) {
                // 如果查询结果为不符合limit长度，需要继续查询，如此操作是因为过滤器过滤数据导致结果集不符合limit长度
                while (null != returnList && returnList.size() > 0 && returnList.size() < limit.getCount()) {
                    limit.setIndex(limit.getIndex() + limit.getCount());
                    // 设置结果
                    returnList.addAll(esUtils.queryESTableResultKV(esTable, columns, condition, sortList, limit, total, filterName, rc));

                    List<Map<String, Object>> returnListTmp = esUtils.queryESTableResultKV(esTable, columns, condition, sortList, limit, total, null, rc);
                    // 如果不过滤时无查询结果，无需继续查询
                    if (null == returnListTmp || returnListTmp.size() <= 0) {
                        break;
                    }
                }
            }

        } else {
            HbaseTable hbaseTable = (HbaseTable) table;

            LOG.debug("queryTableWithES() method, hbase table");
            // 根据表名tableName和查询条件去ES查询出 limit条索引信息rowkey，得到rowkey信息
            List<String> rowkeys = esUtils.queryTable(hbaseTable, columns, condition, sortList, limit, total);

            // 根据表名tableName和得到的rowkey信息去HBase去查询数据，得到最终数据
            // 将最终数据封装返回
            returnList = hbaseUtils.selectWithRowkeysResultKV(tableName, rowkeys, columns, filterName, rc);

            // 如果查询结果为不符合limit长度，需要继续查询，如此操作是因为过滤器过滤数据导致结果集不符合limit长度
            if (null != filterName) {
                while (null != returnList && returnList.size() > 0 && returnList.size() < limit.getCount()) {
                    long tmp = limit.getCount() - returnList.size();
                    limit.setIndex(limit.getCount() + limit.getIndex());
                    rowkeys = esUtils.queryTable(hbaseTable, columns, condition, sortList, limit, total);
                    // 如果无查询结果，跳出循环
                    if (rowkeys.size() <= 0) {
                        break;
                    }
                    List<String> rowkeysTmp = new ArrayList<String>();
                    if (rowkeys.size() > tmp) {

                        for (int i = 0; i < tmp; i++) {
                            rowkeysTmp.add(rowkeys.get(i));
                        }
                    }
                    // 设置结果
                    returnList.addAll(hbaseUtils.selectWithRowkeysResultKV(tableName, rowkeysTmp, columns, filterName, rc));
                }
            }

        }
        // 出口日志
        if (LOG.isDebugEnabled()) {
            LOG.debug("Exit queryTableWithES() method");
        }
        // 参数日志
        if (LOG.isDebugEnabled()) {
            StringBuffer buffer = new StringBuffer();
            buffer.append("returnListSize=");
            buffer.append(returnList.size());
            LOG.debug(buffer.toString());
        }
        return returnList;
    }

    /**
     * @Title: prepareESQuery
     * @Description: 查询ES索引之前的验证
     * @param tableName
     * @param columns
     * @param condition
     * @param rc
     * @return
     * @throws BigDataWareHouseException
     */
    private String prepareESQuery(Table table, String type, String tableName, String[] columns, Condition condition, ResultCode rc) {
        // 入口日志
        if (LOG.isDebugEnabled()) {
            LOG.debug("Enter prepareESQuery() method");
        }
        // 参数日志
        if (LOG.isDebugEnabled()) {
            StringBuffer buffer = new StringBuffer();
            buffer.append("tableName=");
            buffer.append(tableName);
            buffer.append("columns=");
            buffer.append(columns);
            buffer.append("condition=");
            buffer.append(condition);
            buffer.append("rc=");
            buffer.append(rc);
            LOG.debug(buffer.toString());
        }

        String returnMsg = "success";
        // 查看表对象是否合法，查看列是否合法，传入的查询列是否在表内存在
        returnMsg = prepareCommon(table, type, tableName, columns, rc);
        if (!"success".equals(returnMsg)) {
            return returnMsg;
        }
        if (table instanceof ESTable) {
            LOG.debug("prepareESQuery() method, es table");
            ESTable esTable = (ESTable) table;
            Map<String, Column> colMap = esTable.getIndexColumnMap();
            // 检查查询条件是否是建索的引字段
            returnMsg = findIndexColumnsInCons(condition, colMap, rc, returnMsg);
            return returnMsg;
        } else {
            LOG.debug("prepareESQuery() method, hbase table");
            // 检查查询条件是否有对应的索引存在
            returnMsg = findUseIndexForES(table, condition, rc, returnMsg);

        }
        // 判断condition条件的合法性
        String result = verifyCondition(tableName, table.getHiveColumnMap(), condition, rc);
        if (!"success".equals(result)) {
            returnMsg = result;
            LOG.debug(returnMsg);
            return returnMsg;
        }

        // 出口日志
        if (LOG.isDebugEnabled()) {
            LOG.debug("Exit prepareESQuery() method");
        }
        // 参数日志
        if (LOG.isDebugEnabled()) {
            StringBuffer buffer = new StringBuffer();
            buffer.append("table=");
            buffer.append(table);
            LOG.debug(buffer.toString());
        }
        return returnMsg;
    }

    /**
     * 
     * @Title: findIndexColumnsInCons
     * @Description: TODO
     * @param condition
     * @param colMap
     * @param returnMsg2
     * @param rc
     * @return
     */
    private String findIndexColumnsInCons(Condition condition, Map<String, Column> colMap, ResultCode rc, String returnMsg) {
        if ((condition instanceof Conditions) && ((Conditions) condition).getCdList().size() > 0) {
            // 多条件查询的情况
            Conditions conditions = (Conditions) condition;
            List<Condition> cdList = conditions.getCdList();
            for (int i = 0; i < cdList.size(); i++) {
                returnMsg = findIndexColumnsInCons(cdList.get(i), colMap, rc, returnMsg);
                if (!"success".equals(returnMsg)) {
                    return returnMsg;
                }
            }

        } else {
            // 单条件查询的情况
            if (colMap.get(condition.getColumn()) == null && StringUtil.isNotEmpty(condition.getColumn())) {
                rc.setResultCode(ResultCode.NO_INDEX);
                returnMsg = BigDataWareHouseException.buildMsg(BigDataWareHouseException.NO_INDEX);
                LOG.debug(returnMsg);
                return returnMsg;
            } else {
                Column col = colMap.get(condition.getColumn());
                if(col != null){
                    String dateVal = condition.getValue();
                    if (("date".equals(col.getType()) || "timestamp".equals(col.getType())) && dateVal != null && dateVal.trim().length() == 19) {
                        try {
                            condition.setValue(toUTC4Simple(dateVal.trim()));
                        } catch (ParseException e) {
                            rc.setResultCode(ResultCode.DATE_FORMAT_ERROR);
                        }
                    }
                }
            }
        }
        return returnMsg;
    }

    private static String toUTC4Simple(String text) throws ParseException {
        Date date = sdf.parse(text);
        // 1、取得时间：
        java.util.Calendar cal = java.util.Calendar.getInstance();
        cal.setTime(date);

        // 2、取得时间偏移量：
        int zoneOffset = cal.get(java.util.Calendar.ZONE_OFFSET);

        // 3、取得夏令时差：
        int dstOffset = cal.get(java.util.Calendar.DST_OFFSET);

        // 4、从间里扣除这些差量，即可以取得UTC时间：
        cal.add(java.util.Calendar.MILLISECOND, -(zoneOffset + dstOffset));

        return sdf.format(cal.getTime());
    }

    /**
     * 
     * @Title: findUseIndexForES
     * @Description: TODO
     * @param table
     * @param condition
     * @param rc
     * @param returnMsg
     * @return
     */
    private String findUseIndexForES(Table table, Condition condition, ResultCode rc, String returnMsg) {
        HbaseTable hbaseTable = (HbaseTable) table;
        Index index = hbaseTable.getEsIndex();

        boolean outFlag = false;

        if ((condition instanceof Conditions) && ((Conditions) condition).getCdList().size() > 0) {
            // 多条件查询的情况
            outFlag = mutiConditions(condition, index);
        } else {
            // 但条件查询的情况
            outFlag = singleCondition(condition, index);
        }

        if (!outFlag) {
            rc.setResultCode(ResultCode.NO_INDEX);
            returnMsg = BigDataWareHouseException.buildMsg(BigDataWareHouseException.NO_INDEX);
            LOG.debug(returnMsg);
            return returnMsg;
        }
        return returnMsg;
    }

    /**
     * 
     * @Title: singleCondition
     * @Description: TODO
     * @param condition
     * @param index
     * @return
     */
    private boolean singleCondition(Condition condition, Index index) {
        List<Column> colList;
        Column col;
        boolean outFlag = false;
        if (null == condition.getColumn() || "".equals(condition.getColumn())) {
            outFlag = true;

        } else {
            boolean flag = false;

            colList = index.getIndexColumnList();
            for (int j2 = 0; j2 < colList.size(); j2++) {
                col = colList.get(j2);
                if (col.getHiveName().equals(condition.getColumn())) {
                    flag = true;
                    break;
                }
            }

            outFlag = flag;
        }
        return outFlag;
    }

    /**
     * 
     * @Title: mutiConditions
     * @Description: TODO
     * @param condition
     * @param index
     * @return
     */
    private boolean mutiConditions(Condition condition, Index index) {
        List<Column> colList;
        Column col;
        Condition con;
        boolean outFlag = false;
        Conditions conditions = (Conditions) condition;
        List<Condition> cdList = conditions.getCdList();

        colList = index.getIndexColumnList();
        boolean flag = true;

        for (int j = 0; j < cdList.size(); j++) {
            con = cdList.get(j);
            if ((con instanceof Conditions) && ((Conditions) con).getCdList().size() > 0) {
                // 多条件查询的情况
                outFlag = mutiConditions(con, index);
            } else {
                // 但条件查询的情况
                outFlag = singleCondition(con, index);
            }
            // boolean inflag = false;
            // for (int j2 = 0; j2 < colList.size(); j2++) {
            // col = colList.get(j2);
            // if (col.getName().equals(con.getColumn())) {
            // inflag = true;
            // break;
            // }
            // }
            // if (!inflag) {
            // flag = false;
            // break;
            // }

        }
        outFlag = flag;
        return outFlag;
    }

    /**
     * @Title: prepareCommon
     * @Description: TODO
     * @param type
     * @param tableName
     * @param columns
     * @param rc
     * @return
     * @throws BigDataWareHouseException
     **/
    private String prepareCommon(Table table, String type, String tableName, String[] columns, ResultCode rc) {
        // 入口日志
        if (LOG.isDebugEnabled()) {
            LOG.debug("Enter prepareCommon() method");
        }
        // 参数日志
        if (LOG.isDebugEnabled()) {
            StringBuffer buffer = new StringBuffer();
            buffer.append("tableName=");
            buffer.append(tableName);
            buffer.append("columns=");
            buffer.append(columns);
            buffer.append("rc=");
            buffer.append(rc);
            LOG.debug(buffer.toString());
        }

        // 解析查询条件Condition
        // 获取表的配置文件
        String msg = "success";
        // Table table = metaUtils.getTable(tableName);
        if (null == table) {
            rc.setResultCode(ResultCode.TABLE_NOT_EXISTS);
            msg = BigDataWareHouseException.buildMsg(BigDataWareHouseException.TABLE_NOT_EXISTS, tableName);
            LOG.debug(msg);
            return msg;
        }

        if ("query".equals(type)) {
            // 判断传入的列是否合法
            if (null == columns || columns.length == 0) {
                rc.setResultCode(ResultCode.COLUMNS_NO_DATA);
                msg = BigDataWareHouseException.buildMsg(BigDataWareHouseException.COLUMNS_NO_DATA);
                LOG.debug(msg);
                return msg;
            }
            List<String> list = table.getHiveColumnsStringList();
            for (String col : columns) {
                if ((!list.contains(col)) && (!col.equals("_id"))) {
                    rc.setResultCode(ResultCode.COLUMN_NOT_EXISTS);
                    msg = BigDataWareHouseException.buildMsg(BigDataWareHouseException.COLUMN_NOT_EXISTS, col, tableName);
                    LOG.debug(msg);
                    return msg;
                }
            }
        }
        // 出口日志
        if (LOG.isDebugEnabled()) {
            LOG.debug("Exit prepareCommon() method");
        }
        // 参数日志
        if (LOG.isDebugEnabled()) {
            StringBuffer buffer = new StringBuffer();
            buffer.append("table=");
            buffer.append(table);
            LOG.debug(buffer.toString());
        }
        return msg;
    }

    /**
     * @Title: queryESTable
     * @Description: ES索引查询的对外接口
     * @param tableName
     * @param resultColumns
     * @param condition
     * @param sortList
     * @param limit
     * @param tableType
     * @return
     */
    public ESQueryResult queryESTable(String tableName, String[] resultColumns, Condition condition, List<Sort> sortList, Limit limit, String filterName,
            String indexType) {
        // 入口日志
        if (LOG.isDebugEnabled()) {
            LOG.debug("Enter queryESTable() method");
        }
        // 参数日志
        if (LOG.isDebugEnabled()) {
            StringBuffer buffer = new StringBuffer();
            buffer.append("tableName=");
            buffer.append(tableName);
            buffer.append("resultColumns=");
            buffer.append(resultColumns);
            buffer.append("condition=");
            buffer.append(condition);
            buffer.append("sortList=");
            buffer.append(sortList);
            buffer.append("limit=");
            buffer.append(limit);
            buffer.append("filterName=");
            buffer.append(filterName);
            LOG.debug(buffer.toString());
        }

        ESQueryResult esqr = new ESQueryResult();
        AtomicLong total = new AtomicLong(0);
        ResultCode rc = new ResultCode();
        Gson gson = new Gson();
        try {

            // 执行查询方法
            esqr.setResult(queryTableWithES(tableName, resultColumns, condition, sortList, limit, total, rc, filterName, indexType));
            esqr.setResultString(gson.toJson(esqr.getResult()));
        } catch (Exception e) {
            if ("0".equals(rc.getResultCode())) {
                rc.setResultCode(ResultCode.INTERNAL_ERROR);
            }
            LOG.debug("queryTableWithES error:" + e.getMessage());
            esqr.setException(new BigDataWareHouseException(e));
        } finally {
            esqr.setTotal(total.get());
            esqr.setStatusCode(rc.getResultCode());
        }
        // 出口日志
        if (LOG.isDebugEnabled()) {
            LOG.debug("Exit queryESTable() method");
        }
        // 参数日志
        if (LOG.isDebugEnabled()) {
            StringBuffer buffer = new StringBuffer();
            buffer.append("ESQueryResult=");
            buffer.append(esqr);
            LOG.debug(buffer.toString());
        }
        return esqr;
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
    public ESQueryResultKV queryESTableReturnKV(String tableName, String[] resultColumns, Condition condition, List<Sort> sortList, Limit limit,
            String filterName, String indexType) {
        // 入口日志
        if (LOG.isDebugEnabled()) {
            LOG.debug("Enter queryESTableReturnKV() method");
        }
        // 参数日志
        if (LOG.isDebugEnabled()) {
            StringBuffer buffer = new StringBuffer();
            buffer.append("tableName=");
            buffer.append(tableName);
            buffer.append("resultColumns=");
            buffer.append(resultColumns);
            buffer.append("condition=");
            buffer.append(condition);
            buffer.append("sortList=");
            buffer.append(sortList);
            buffer.append("limit=");
            buffer.append(limit);
            buffer.append("filterName=");
            buffer.append(filterName);
            LOG.debug(buffer.toString());
        }

        ESQueryResultKV esqr = new ESQueryResultKV();
        AtomicLong total = new AtomicLong(0);
        ResultCode rc = new ResultCode();
        // Gson gson = new Gson();
        try {

            // 执行查询方法
            esqr.setResult(queryTableWithESResultKV(tableName, resultColumns, condition, sortList, limit, total, rc, filterName, indexType));
            // esqr.setResultString(gson.toJson(esqr.getResult()));
        } catch (Exception e) {
            if ("0".equals(rc.getResultCode())) {
                rc.setResultCode(ResultCode.INTERNAL_ERROR);
            }
            LOG.debug("queryTableWithESResultKV error:" + e.getMessage());
            esqr.setException(new BigDataWareHouseException(e));
        } finally {
            esqr.setTotal(total.get());
            esqr.setStatusCode(rc.getResultCode());
        }
        // 出口日志
        if (LOG.isDebugEnabled()) {
            LOG.debug("Exit queryESTableReturnKV() method");
        }
        // 参数日志
        if (LOG.isDebugEnabled()) {
            StringBuffer buffer = new StringBuffer();
            buffer.append("ESQueryResultKV=");
            buffer.append(esqr);
            LOG.debug(buffer.toString());
        }
        return esqr;
    }

    /**
     * @Title: countESTable
     * @Description: ES索引count条数的对外接口
     * @param tableName
     * @param resultColumns
     * @param condition
     * @param sortList
     * @param limit
     * @return
     */
    public ESCountResult countESTable(String tableName, Condition condition) {
        // 入口日志
        if (LOG.isDebugEnabled()) {
            LOG.debug("Enter countESTable() method");
        }
        // 参数日志
        if (LOG.isDebugEnabled()) {
            StringBuffer buffer = new StringBuffer();
            buffer.append("tableName=");
            buffer.append(tableName);
            buffer.append("condition=");
            buffer.append(condition);
            LOG.debug(buffer.toString());
        }

        ESCountResult esqr = new ESCountResult();
        ResultCode rc = new ResultCode();
        String returnMsg = "success";
        try {
            // 获取table表对象
            Table table = metaUtils.getTable(tableName);
            if (table == null) {
                LOG.debug("table:" + tableName + " not exists!");
                rc.setResultCode(ResultCode.TABLE_NOT_EXISTS);
                esqr.setStatusCode(rc.getResultCode());
                return esqr;
            }

            // 准备查询前对表，对字段，对返回列，对查询条件的各种验证
            returnMsg = prepareESQuery(table, "count", tableName, null, condition, rc);
            if (!"success".equals(returnMsg)) {
                LOG.debug(returnMsg);
                esqr.setStatusCode(rc.getResultCode());
                return esqr;
            }

            // 开始查询
            if (table instanceof HbaseTable) {
                HbaseTable hbaseTable = (HbaseTable) table;
                esqr.setTotal(esUtils.countTable(hbaseTable, condition));
            } else {
                ESTable esTable = (ESTable) table;
                esqr.setTotal(esUtils.countESTable(esTable, condition));
            }
        } catch (Exception e) {
            if ("0".equals(rc.getResultCode())) {
                rc.setResultCode(ResultCode.INTERNAL_ERROR);
            }
            LOG.debug("countTable error:" + e.getMessage());
            esqr.setException(new BigDataWareHouseException(e));
        } finally {
            // 设置结果
            esqr.setStatusCode(rc.getResultCode());
        }
        // 出口日志
        if (LOG.isDebugEnabled()) {
            LOG.debug("Exit countESTable() method");
        }
        // 参数日志
        if (LOG.isDebugEnabled()) {
            StringBuffer buffer = new StringBuffer();
            buffer.append("ESCountResult=");
            buffer.append(esqr);
            LOG.debug(buffer.toString());
        }
        return esqr;
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
    public ESQueryResultKV queryMutiESTableReturnKV(TableJoinStruct struct, String[] resultColumns, List<Sort> sortList, Limit limit) {
        // 入口日志
        if (LOG.isDebugEnabled()) {
            LOG.debug("Enter queryMutiESTableReturnKV() method");
        }
        // 参数日志
        if (LOG.isDebugEnabled()) {
            StringBuffer buffer = new StringBuffer();
            buffer.append("struct=");
            buffer.append(struct);
            buffer.append("resultColumns=");
            buffer.append(resultColumns);
            buffer.append("sortList=");
            buffer.append(sortList);
            buffer.append("limit=");
            buffer.append(limit);
            LOG.debug(buffer.toString());
        }

        ESQueryResultKV esqr = new ESQueryResultKV();
        AtomicLong total = new AtomicLong(0);
        ResultCode rc = new ResultCode();

        String returnMsg = "success";

        TableJoin outerTable = struct.getOuterTable();
        TableJoin innerTable = struct.getInnerTable();
        // 获取表对象
        Table table = metaUtils.getTable(outerTable.getTableName());
        if (table == null) {
            rc.setResultCode(ResultCode.TABLE_NOT_EXISTS);
            LOG.debug(BigDataWareHouseException.buildMsg(BigDataWareHouseException.TABLE_NOT_EXISTS, outerTable.getTableName()));
            esqr.setStatusCode(rc.getResultCode());
            return esqr;
        }
        // 准备查询前对表，对字段，对返回列，对查询条件的各种验证
        returnMsg = prepareESQuery(table, "query", outerTable.getTableName(), resultColumns, outerTable.getCondition(), rc);
        if (!"success".equals(returnMsg)) {
            LOG.debug(returnMsg);
            esqr.setStatusCode(rc.getResultCode());
            return esqr;
        }

        if (innerTable instanceof TableJoinStruct) {
            TableJoinStruct innerStruct = (TableJoinStruct) innerTable;
            verifyMutiTables(innerStruct, rc);
            if (!"0".equals(rc.getResultCode())) {
                esqr.setStatusCode(rc.getResultCode());
                return esqr;
            }
        } else {
            // 获取表对象
            Table tableIn = metaUtils.getTable(innerTable.getTableName());
            if (tableIn == null) {
                rc.setResultCode(ResultCode.TABLE_NOT_EXISTS);
                LOG.debug(BigDataWareHouseException.buildMsg(BigDataWareHouseException.TABLE_NOT_EXISTS, innerTable.getTableName()));
                esqr.setStatusCode(rc.getResultCode());
                return esqr;
            }
            returnMsg = verifyCondition(innerTable.getTableName(), tableIn.getHiveColumnMap(), innerTable.getCondition(), rc);
            if (!"success".equals(returnMsg)) {
                esqr.setStatusCode(rc.getResultCode());
                return esqr;
            }
        }

        if (sortList == null) {
            sortList = new ArrayList<Sort>();
        }

        try {
            // 执行查询方法
            esqr.setResult(esUtils.queryMutiESTableWithAPI(struct, resultColumns, sortList, limit, total, rc));
        } catch (Exception e) {
            if ("0".equals(rc.getResultCode())) {
                rc.setResultCode(ResultCode.INTERNAL_ERROR);
            }
            LOG.debug("queryMutiESTableReturnKV error:" + e.getMessage());
            esqr.setException(new BigDataWareHouseException(e));
        } finally {
            esqr.setTotal(total.get());
            esqr.setStatusCode(rc.getResultCode());
        }
        // 出口日志
        if (LOG.isDebugEnabled()) {
            LOG.debug("Exit queryMutiESTableReturnKV() method");
        }
        // 参数日志
        if (LOG.isDebugEnabled()) {
            StringBuffer buffer = new StringBuffer();
            buffer.append("ESQueryResultKV=");
            buffer.append(esqr);
            LOG.debug(buffer.toString());
        }
        return esqr;
    }

    private void verifyMutiTables(TableJoinStruct struct, ResultCode rc) {
        TableJoin outerTable = struct.getOuterTable();
        TableJoin innerTable = struct.getInnerTable();
        String returnMsg = "success";
        // 获取表对象
        Table tableOut = metaUtils.getTable(outerTable.getTableName());
        if (tableOut == null) {
            rc.setResultCode(ResultCode.TABLE_NOT_EXISTS);
            LOG.debug(BigDataWareHouseException.buildMsg(BigDataWareHouseException.TABLE_NOT_EXISTS, outerTable.getTableName()));
            return;
        }
        returnMsg = verifyCondition(outerTable.getTableName(), tableOut.getHiveColumnMap(), outerTable.getCondition(), rc);
        if (!"success".equals(returnMsg)) {
            return;
        }
        if (innerTable instanceof TableJoinStruct) {
            TableJoinStruct innerStruct = (TableJoinStruct) innerTable;
            verifyMutiTables(innerStruct, rc);
        } else {
            // 获取表对象
            Table tableIn = metaUtils.getTable(innerTable.getTableName());
            if (tableIn == null) {
                rc.setResultCode(ResultCode.TABLE_NOT_EXISTS);
                LOG.debug(BigDataWareHouseException.buildMsg(BigDataWareHouseException.TABLE_NOT_EXISTS, innerTable.getTableName()));
                return;
            }
            returnMsg = verifyCondition(innerTable.getTableName(), tableIn.getHiveColumnMap(), innerTable.getCondition(), rc);
            if (!"success".equals(returnMsg)) {
                return;
            }
        }
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
        // 入口日志
        if (LOG.isDebugEnabled()) {
            LOG.debug("Enter rangeQueryGlobalTable() method");
        }
        // 参数日志
        if (LOG.isDebugEnabled()) {
            StringBuffer buffer = new StringBuffer();
            buffer.append("tableName=");
            buffer.append(tableName);
            buffer.append("resultColumns=");
            buffer.append(resultColumns);
            buffer.append("keyValueRanges=");
            buffer.append(keyValueRanges);
            buffer.append("filterName=");
            buffer.append(filterName);
            LOG.debug(buffer.toString());
        }

        GlobalQueryResult gqr = new GlobalQueryResult();
        ResultCode rc = new ResultCode();
        Gson gson = new Gson();
        String returnMsg = "success";
        try {
            // 获取table表对象
            Table table = metaUtils.getTable(tableName);
            if (table == null) {
                LOG.debug("table:" + tableName + " not exists!");
                rc.setResultCode(ResultCode.TABLE_NOT_EXISTS);
                gqr.setStatusCode(rc.getResultCode());
                return gqr;
            }

            // 检查表对象，检查表中的列，检查查询列是否存在
            returnMsg = prepareCommon(table, "query", tableName, resultColumns, rc);
            if (!"success".equals(returnMsg)) {
                LOG.debug(returnMsg);
                gqr.setStatusCode(rc.getResultCode());
                return gqr;
            }

            // 检查二级索引是否存在
            Index useIndex = verifyGlobalIndex(table, resultColumns, keyValueRanges, rc);
            if (useIndex == null) {
                LOG.debug(BigDataWareHouseException.NO_INDEX);
                gqr.setStatusCode(rc.getResultCode());
                return gqr;
            }
            // 开始查询
            List<String> rowkeys = hbaseUtils.rangeScanRange(useIndex, keyValueRanges);
            // 设置结果
            gqr.setResult(hbaseUtils.selectWithRowkeys(tableName, rowkeys, resultColumns, filterName, rc));
            gqr.setResultString(gson.toJson(gqr.getResult()));
        } catch (Exception e) {
            if ("0".equals(rc.getResultCode())) {
                rc.setResultCode(ResultCode.INTERNAL_ERROR);
            }
            LOG.debug("rangeScanRange error:" + e.getMessage());
            gqr.setException(new BigDataWareHouseException(e));
        } finally {
            gqr.setStatusCode(rc.getResultCode());
        }
        // 出口日志
        if (LOG.isDebugEnabled()) {
            LOG.debug("Exit rangeQueryGlobalTable() method");
        }
        // 参数日志
        if (LOG.isDebugEnabled()) {
            StringBuffer buffer = new StringBuffer();
            buffer.append("GlobalQueryResult=");
            buffer.append(gqr);
            LOG.debug(buffer.toString());
        }
        return gqr;
    }

    /**
     * 
     * @Title: rangeQueryGlobalTableReturnKV
     * @Description: TODO
     * @param tableName
     * @param resultColumns
     * @param keyValueRanges
     * @param object
     * @return
     */
    public GlobalQueryResultKV rangeQueryGlobalTableReturnKV(String tableName, String[] resultColumns, KeyValueRange[] keyValueRanges, String filterName) {
        // 入口日志
        if (LOG.isDebugEnabled()) {
            LOG.debug("Enter rangeQueryGlobalTableReturnKV() method");
        }
        // 参数日志
        if (LOG.isDebugEnabled()) {
            StringBuffer buffer = new StringBuffer();
            buffer.append("tableName=");
            buffer.append(tableName);
            buffer.append("resultColumns=");
            buffer.append(resultColumns);
            buffer.append("keyValueRanges=");
            buffer.append(keyValueRanges);
            buffer.append("filterName=");
            buffer.append(filterName);
            LOG.debug(buffer.toString());
        }

        GlobalQueryResultKV gqr = new GlobalQueryResultKV();
        ResultCode rc = new ResultCode();
        // Gson gson = new Gson();
        String returnMsg = "success";
        try {
            // 获取table表对象
            Table table = metaUtils.getTable(tableName);
            if (table == null) {
                LOG.debug("table:" + tableName + " not exists!");
                rc.setResultCode(ResultCode.TABLE_NOT_EXISTS);
                gqr.setStatusCode(rc.getResultCode());
                return gqr;
            }

            // 检查表对象，检查表中的列，检查查询列是否存在
            returnMsg = prepareCommon(table, "query", tableName, resultColumns, rc);
            if (!"success".equals(returnMsg)) {
                LOG.debug(returnMsg);
                gqr.setStatusCode(rc.getResultCode());
                return gqr;
            }

            // 检查二级索引是否存在
            Index useIndex = verifyGlobalIndex(table, resultColumns, keyValueRanges, rc);
            if (useIndex == null) {
                LOG.debug(BigDataWareHouseException.NO_INDEX);
                gqr.setStatusCode(rc.getResultCode());
                return gqr;
            }
            // 开始查询
            List<String> rowkeys = hbaseUtils.rangeScanRange(useIndex, keyValueRanges);
            // 设置结果
            gqr.setResult(hbaseUtils.selectWithRowkeysResultKV(tableName, rowkeys, resultColumns, filterName, rc));
            // gqr.setResultString(gson.toJson(gqr.getResult()));
        } catch (Exception e) {
            if ("0".equals(rc.getResultCode())) {
                rc.setResultCode(ResultCode.INTERNAL_ERROR);
            }
            LOG.debug("rangeScanRange error:" + e.getMessage());
            gqr.setException(new BigDataWareHouseException(e));
        } finally {
            gqr.setStatusCode(rc.getResultCode());
        }
        // 出口日志
        if (LOG.isDebugEnabled()) {
            LOG.debug("Exit rangeQueryGlobalTableReturnKV() method");
        }
        // 参数日志
        if (LOG.isDebugEnabled()) {
            StringBuffer buffer = new StringBuffer();
            buffer.append("GlobalQueryResult=");
            buffer.append(gqr);
            LOG.debug(buffer.toString());
        }
        return gqr;
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
        // 入口日志
        if (LOG.isDebugEnabled()) {
            LOG.debug("Enter queryGlobalTable() method");
        }
        // 参数日志
        if (LOG.isDebugEnabled()) {
            StringBuffer buffer = new StringBuffer();
            buffer.append("tableName=");
            buffer.append(tableName);
            buffer.append("resultColumns=");
            buffer.append(resultColumns);
            buffer.append("keyValues=");
            buffer.append(keyValues);
            buffer.append("filterName=");
            buffer.append(filterName);
            LOG.debug(buffer.toString());
        }

        GlobalQueryResult gqr = new GlobalQueryResult();
        ResultCode rc = new ResultCode();
        Gson gson = new Gson();
        String returnMsg = "success";
        try {
            // 获取表对象
            Table table = metaUtils.getTable(tableName);
            if (table == null) {
                LOG.debug("table:" + tableName + " not exists!");
                rc.setResultCode(ResultCode.TABLE_NOT_EXISTS);
                gqr.setStatusCode(rc.getResultCode());
                return gqr;
            }

            // 检查表对象，检查表中的列，检查查询列是否存在
            returnMsg = prepareCommon(table, "query", tableName, resultColumns, rc);
            if (!"success".equals(returnMsg)) {
                LOG.debug("table:" + tableName + " not exists!");
                rc.setResultCode(ResultCode.TABLE_NOT_EXISTS);
                gqr.setStatusCode(rc.getResultCode());
                return gqr;
            }
            // 检查查询条件是否有对应索引
            Index useIndex = verifyGlobalIndex(table, resultColumns, keyValues, rc);
            if (useIndex == null) {
                LogUtils.errorMsg(BigDataWareHouseException.NO_INDEX);
            }
            // 开始查询
            List<String> rowkeys = hbaseUtils.scanTable(useIndex, keyValues);
            // 设置结果
            gqr.setResult(hbaseUtils.selectWithRowkeys(tableName, rowkeys, resultColumns, filterName, rc));
            gqr.setResultString(gson.toJson(gqr.getResult()));
        } catch (BigDataWareHouseException e) {
            if ("0".equals(rc.getResultCode())) {
                rc.setResultCode(ResultCode.INTERNAL_ERROR);
            }
            LOG.debug("scanTable error:" + e.getMessage());
            gqr.setException(new BigDataWareHouseException(e));
        } finally {
            gqr.setStatusCode(rc.getResultCode());
        }
        // 出口日志
        if (LOG.isDebugEnabled()) {
            LOG.debug("Exit queryGlobalTable() method");
        }
        // 参数日志
        if (LOG.isDebugEnabled()) {
            StringBuffer buffer = new StringBuffer();
            buffer.append("GlobalQueryResult=");
            buffer.append(gqr);
            LOG.debug(buffer.toString());
        }
        return gqr;
    }

    /**
     * 
     * @Title: queryGlobalTableReturnKV
     * @Description: TODO
     * @param tableName
     * @param resultColumns
     * @param keyValues
     * @param object
     * @return
     */
    public GlobalQueryResultKV queryGlobalTableReturnKV(String tableName, String[] resultColumns, KeyValue[] keyValues, String filterName) {
        // 入口日志
        if (LOG.isDebugEnabled()) {
            LOG.debug("Enter queryGlobalTableReturnKV() method");
        }
        // 参数日志
        if (LOG.isDebugEnabled()) {
            StringBuffer buffer = new StringBuffer();
            buffer.append("tableName=");
            buffer.append(tableName);
            buffer.append("resultColumns=");
            buffer.append(resultColumns);
            buffer.append("keyValues=");
            buffer.append(keyValues);
            buffer.append("filterName=");
            buffer.append(filterName);
            LOG.debug(buffer.toString());
        }

        GlobalQueryResultKV gqr = new GlobalQueryResultKV();
        ResultCode rc = new ResultCode();
        // Gson gson = new Gson();
        String returnMsg = "success";
        try {
            // 获取表对象
            Table table = metaUtils.getTable(tableName);
            if (table == null) {
                LOG.debug("table:" + tableName + " not exists!");
                rc.setResultCode(ResultCode.TABLE_NOT_EXISTS);
                gqr.setStatusCode(rc.getResultCode());
                return gqr;
            }

            // 检查表对象，检查表中的列，检查查询列是否存在
            returnMsg = prepareCommon(table, "query", tableName, resultColumns, rc);
            if (!"success".equals(returnMsg)) {
                LOG.debug("table:" + tableName + " not exists!");
                rc.setResultCode(ResultCode.TABLE_NOT_EXISTS);
                gqr.setStatusCode(rc.getResultCode());
                return gqr;
            }
            // 检查查询条件是否有对应索引
            Index useIndex = verifyGlobalIndex(table, resultColumns, keyValues, rc);
            if (useIndex == null) {
                LogUtils.errorMsg(BigDataWareHouseException.NO_INDEX);
            }
            // 开始查询
            List<String> rowkeys = hbaseUtils.scanTable(useIndex, keyValues);
            // 设置结果
            gqr.setResult(hbaseUtils.selectWithRowkeysResultKV(tableName, rowkeys, resultColumns, filterName, rc));
            // gqr.setResultString(gson.toJson(gqr.getResult()));
        } catch (BigDataWareHouseException e) {
            if ("0".equals(rc.getResultCode())) {
                rc.setResultCode(ResultCode.INTERNAL_ERROR);
            }
            LOG.debug("scanTable error:" + e.getMessage());
            gqr.setException(new BigDataWareHouseException(e));
        } finally {
            gqr.setStatusCode(rc.getResultCode());
        }
        // 出口日志
        if (LOG.isDebugEnabled()) {
            LOG.debug("Exit queryGlobalTableReturnKV() method");
        }
        // 参数日志
        if (LOG.isDebugEnabled()) {
            StringBuffer buffer = new StringBuffer();
            buffer.append("GlobalQueryResult=");
            buffer.append(gqr);
            LOG.debug(buffer.toString());
        }
        return gqr;
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
    public GlobalQueryResult rangeQueryGlobalTablePage(String tableName, String[] resultColumns, KeyValueRange[] keyValueRanges, String endkey, int limit,
            String filterName) {
        // 入口日志
        if (LOG.isDebugEnabled()) {
            LOG.debug("Enter rangeQueryGlobalTablePage() method");
        }
        // 参数日志
        if (LOG.isDebugEnabled()) {
            StringBuffer buffer = new StringBuffer();
            buffer.append("tableName=");
            buffer.append(tableName);
            buffer.append("resultColumns=");
            buffer.append(resultColumns);
            buffer.append("keyValueRanges=");
            buffer.append(keyValueRanges);
            buffer.append("endkey=");
            buffer.append(endkey);
            buffer.append("limit=");
            buffer.append(limit);
            buffer.append("filterName=");
            buffer.append(filterName);
            LOG.debug(buffer.toString());
        }

        GlobalQueryResult gqr = new GlobalQueryResult();
        ResultCode rc = new ResultCode();
        Gson gson = new Gson();
        String returnMsg = "success";
        try {

            // 获取表对象
            Table table = metaUtils.getTable(tableName);
            if (table == null) {
                LOG.debug("table:" + tableName + " not exists!");
                rc.setResultCode(ResultCode.TABLE_NOT_EXISTS);
                gqr.setStatusCode(rc.getResultCode());
                return gqr;
            }

            // 检查表对象，检查表中的列，检查查询列是否存在
            returnMsg = prepareCommon(table, "query", tableName, resultColumns, rc);
            if (!"success".equals(returnMsg)) {
                LOG.debug("table:" + tableName + " not exists!");
                rc.setResultCode(ResultCode.TABLE_NOT_EXISTS);
                gqr.setStatusCode(rc.getResultCode());
                return gqr;
            }
            // 检查查询条件是否有对应索引
            Index useIndex = verifyGlobalIndex(table, resultColumns, keyValueRanges, rc);
            if (useIndex == null) {
                LogUtils.errorMsg(BigDataWareHouseException.NO_INDEX);
            }
            // 执行查询
            GlobalQueryRowkeyPage rowkeys = hbaseUtils.rangeScanRange(useIndex, keyValueRanges, endkey, limit);
            // 设置结果
            gqr.setResult(hbaseUtils.selectWithRowkeys(tableName, rowkeys.getRowkeys(), resultColumns, filterName, rc));
            gqr.setResultString(gson.toJson(gqr.getResult()));
            gqr.setEndkey(rowkeys.getEndKey());

            if (null != filterName) {
                // 如果查询结果为不符合limit长度，需要继续查询，如此操作是因为过滤器过滤数据导致结果集不符合limit长度
                while (null != gqr.getResult() && gqr.getResult().size() > 0 && gqr.getResult().size() < limit) {
                    rowkeys = hbaseUtils.rangeScanRange(useIndex, keyValueRanges, rowkeys.getEndKey(), limit);
                    // 如果无查询结果，跳出循环
                    if (rowkeys.getRowkeys().size() <= 0) {
                        break;
                    }
                    // 设置结果
                    gqr.addResult(hbaseUtils.selectWithRowkeys(tableName, rowkeys.getRowkeys(), resultColumns, filterName, rc));
                    gqr.setResultString(gson.toJson(gqr.getResult()));
                    gqr.setEndkey(rowkeys.getEndKey());
                }
            }

        } catch (Exception e) {
            if ("0".equals(rc.getResultCode())) {
                rc.setResultCode(ResultCode.INTERNAL_ERROR);
            }
            LOG.debug("rangeScanRange error:" + e.getMessage());
            gqr.setException(new BigDataWareHouseException(e));
        } finally {
            gqr.setStatusCode(rc.getResultCode());
        }
        // 出口日志
        if (LOG.isDebugEnabled()) {
            LOG.debug("Exit rangeQueryGlobalTablePage() method");
        }
        // 参数日志
        if (LOG.isDebugEnabled()) {
            StringBuffer buffer = new StringBuffer();
            buffer.append("GlobalQueryResult=");
            buffer.append(gqr);
            LOG.debug(buffer.toString());
        }
        return gqr;
    }

    /**
     * 
     * @Title: rangeQueryGlobalTablePageReturnKV
     * @Description: TODO
     * @param tableName
     * @param resultColumns
     * @param keyValueRanges
     * @param endkey
     * @param limit
     * @param object
     * @return
     */
    public GlobalQueryResultKV rangeQueryGlobalTablePageReturnKV(String tableName, String[] resultColumns, KeyValueRange[] keyValueRanges, String endkey,
            int limit, String filterName) {
        // 入口日志
        if (LOG.isDebugEnabled()) {
            LOG.debug("Enter rangeQueryGlobalTablePageReturnKV() method");
        }
        // 参数日志
        if (LOG.isDebugEnabled()) {
            StringBuffer buffer = new StringBuffer();
            buffer.append("tableName=");
            buffer.append(tableName);
            buffer.append("resultColumns=");
            buffer.append(resultColumns);
            buffer.append("keyValueRanges=");
            buffer.append(keyValueRanges);
            buffer.append("endkey=");
            buffer.append(endkey);
            buffer.append("limit=");
            buffer.append(limit);
            buffer.append("filterName=");
            buffer.append(filterName);
            LOG.debug(buffer.toString());
        }

        GlobalQueryResultKV gqr = new GlobalQueryResultKV();
        ResultCode rc = new ResultCode();
        // Gson gson = new Gson();
        String returnMsg = "success";
        try {

            // 获取表对象
            Table table = metaUtils.getTable(tableName);
            if (table == null) {
                LOG.debug("table:" + tableName + " not exists!");
                rc.setResultCode(ResultCode.TABLE_NOT_EXISTS);
                gqr.setStatusCode(rc.getResultCode());
                return gqr;
            }

            // 检查表对象，检查表中的列，检查查询列是否存在
            returnMsg = prepareCommon(table, "query", tableName, resultColumns, rc);
            if (!"success".equals(returnMsg)) {
                LOG.debug("table:" + tableName + " not exists!");
                rc.setResultCode(ResultCode.TABLE_NOT_EXISTS);
                gqr.setStatusCode(rc.getResultCode());
                return gqr;
            }
            // 检查查询条件是否有对应索引
            Index useIndex = verifyGlobalIndex(table, resultColumns, keyValueRanges, rc);
            if (useIndex == null) {
                LogUtils.errorMsg(BigDataWareHouseException.NO_INDEX);
            }
            // 执行查询
            GlobalQueryRowkeyPage rowkeys = hbaseUtils.rangeScanRange(useIndex, keyValueRanges, endkey, limit);
            // 设置结果
            gqr.setResult(hbaseUtils.selectWithRowkeysResultKV(tableName, rowkeys.getRowkeys(), resultColumns, filterName, rc));
            // gqr.setResultString(gson.toJson(gqr.getResult()));
            gqr.setEndkey(rowkeys.getEndKey());

            if (null != filterName) {
                // 如果查询结果为不符合limit长度，需要继续查询，如此操作是因为过滤器过滤数据导致结果集不符合limit长度
                while (null != gqr.getResult() && gqr.getResult().size() > 0 && gqr.getResult().size() < limit) {
                    rowkeys = hbaseUtils.rangeScanRange(useIndex, keyValueRanges, rowkeys.getEndKey(), limit);
                    // 如果无查询结果，跳出循环
                    if (rowkeys.getRowkeys().size() <= 0) {
                        break;
                    }
                    // 设置结果
                    gqr.addResult(hbaseUtils.selectWithRowkeysResultKV(tableName, rowkeys.getRowkeys(), resultColumns, filterName, rc));
                    // gqr.setResultString(gson.toJson(gqr.getResult()));
                    gqr.setEndkey(rowkeys.getEndKey());
                }
            }

        } catch (Exception e) {
            if ("0".equals(rc.getResultCode())) {
                rc.setResultCode(ResultCode.INTERNAL_ERROR);
            }
            LOG.debug("rangeScanRange error:" + e.getMessage());
            gqr.setException(new BigDataWareHouseException(e));
        } finally {
            gqr.setStatusCode(rc.getResultCode());
        }
        // 出口日志
        if (LOG.isDebugEnabled()) {
            LOG.debug("Exit rangeQueryGlobalTablePageReturnKV() method");
        }
        // 参数日志
        if (LOG.isDebugEnabled()) {
            StringBuffer buffer = new StringBuffer();
            buffer.append("GlobalQueryResult=");
            buffer.append(gqr);
            LOG.debug(buffer.toString());
        }
        return gqr;
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
        // 入口日志
        if (LOG.isDebugEnabled()) {
            LOG.debug("Enter queryGlobalTablePage() method");
        }
        // 参数日志
        if (LOG.isDebugEnabled()) {
            StringBuffer buffer = new StringBuffer();
            buffer.append("tableName=");
            buffer.append(tableName);
            buffer.append("resultColumns=");
            buffer.append(resultColumns);
            buffer.append("keyValues=");
            buffer.append(keyValues);
            buffer.append("endkey=");
            buffer.append(endkey);
            buffer.append("limit=");
            buffer.append(limit);
            buffer.append("filterName=");
            buffer.append(filterName);
            LOG.debug(buffer.toString());
        }

        GlobalQueryResult gqr = new GlobalQueryResult();
        ResultCode rc = new ResultCode();
        Gson gson = new Gson();
        String returnMsg = "success";
        try {

            // 获取表对象
            Table table = metaUtils.getTable(tableName);
            if (table == null) {
                LOG.debug("table:" + tableName + " not exists!");
                rc.setResultCode(ResultCode.TABLE_NOT_EXISTS);
                gqr.setStatusCode(rc.getResultCode());
                return gqr;
            }

            // 检查表对象，检查表中的列，检查查询列是否存在
            returnMsg = prepareCommon(table, "query", tableName, resultColumns, rc);
            if (!"success".equals(returnMsg)) {
                LOG.debug("table:" + tableName + " not exists!");
                rc.setResultCode(ResultCode.TABLE_NOT_EXISTS);
                gqr.setStatusCode(rc.getResultCode());
                return gqr;
            }
            // 检查查询条件是否有对应索引
            Index useIndex = verifyGlobalIndex(table, resultColumns, keyValues, rc);
            if (useIndex == null) {
                LogUtils.errorMsg(BigDataWareHouseException.NO_INDEX);
            }
            // 执行查询
            GlobalQueryRowkeyPage rowkeys = hbaseUtils.scanTable(useIndex, keyValues, endkey, limit);
            // 设置结果
            gqr.setResult(hbaseUtils.selectWithRowkeys(tableName, rowkeys.getRowkeys(), resultColumns, filterName, rc));
            gqr.setResultString(gson.toJson(gqr.getResult()));
            gqr.setEndkey(rowkeys.getEndKey());

            if (null != filterName) {
                // 如果查询结果为不符合limit长度，需要继续查询，如此操作是因为过滤器过滤数据导致结果集不符合limit长度
                while (null != gqr.getResult() && gqr.getResult().size() > 0 && gqr.getResult().size() <= limit) {
                    rowkeys = hbaseUtils.scanTable(useIndex, keyValues, endkey, limit);
                    // 如果无查询结果，跳出循环
                    if (rowkeys.getRowkeys().size() <= 0) {
                        break;
                    }
                    // 设置结果
                    gqr.addResult(hbaseUtils.selectWithRowkeys(tableName, rowkeys.getRowkeys(), resultColumns, filterName, rc));
                    gqr.setResultString(gson.toJson(gqr.getResult()));
                    gqr.setEndkey(rowkeys.getEndKey());
                }
            }

        } catch (Exception e) {
            if ("0".equals(rc.getResultCode())) {
                rc.setResultCode(ResultCode.INTERNAL_ERROR);
            }
            LOG.debug("scanTable error:" + e.getMessage());
            gqr.setException(new BigDataWareHouseException(e));
        } finally {
            gqr.setStatusCode(rc.getResultCode());
        }
        // 出口日志
        if (LOG.isDebugEnabled()) {
            LOG.debug("Exit queryGlobalTablePage() method");
        }
        // 参数日志
        if (LOG.isDebugEnabled()) {
            StringBuffer buffer = new StringBuffer();
            buffer.append("GlobalQueryResult=");
            buffer.append(gqr);
            LOG.debug(buffer.toString());
        }
        return gqr;
    }

    public GlobalQueryResultKV queryGlobalTablePageReturnKV(String tableName, String[] resultColumns, KeyValue[] keyValues, String endkey, int limit,
            String filterName) {
        // 入口日志
        if (LOG.isDebugEnabled()) {
            LOG.debug("Enter queryGlobalTablePageReturnKV() method");
        }
        // 参数日志
        if (LOG.isDebugEnabled()) {
            StringBuffer buffer = new StringBuffer();
            buffer.append("tableName=");
            buffer.append(tableName);
            buffer.append("resultColumns=");
            buffer.append(resultColumns);
            buffer.append("keyValues=");
            buffer.append(keyValues);
            buffer.append("endkey=");
            buffer.append(endkey);
            buffer.append("limit=");
            buffer.append(limit);
            buffer.append("filterName=");
            buffer.append(filterName);
            LOG.debug(buffer.toString());
        }

        GlobalQueryResultKV gqr = new GlobalQueryResultKV();
        ResultCode rc = new ResultCode();
        // Gson gson = new Gson();
        String returnMsg = "success";
        try {

            // 获取表对象
            Table table = metaUtils.getTable(tableName);
            if (table == null) {
                LOG.debug("table:" + tableName + " not exists!");
                rc.setResultCode(ResultCode.TABLE_NOT_EXISTS);
                gqr.setStatusCode(rc.getResultCode());
                return gqr;
            }

            // 检查表对象，检查表中的列，检查查询列是否存在
            returnMsg = prepareCommon(table, "query", tableName, resultColumns, rc);
            if (!"success".equals(returnMsg)) {
                LOG.debug("table:" + tableName + " not exists!");
                rc.setResultCode(ResultCode.TABLE_NOT_EXISTS);
                gqr.setStatusCode(rc.getResultCode());
                return gqr;
            }
            // 检查查询条件是否有对应索引
            Index useIndex = verifyGlobalIndex(table, resultColumns, keyValues, rc);
            if (useIndex == null) {
                LogUtils.errorMsg(BigDataWareHouseException.NO_INDEX);
            }
            // 执行查询
            GlobalQueryRowkeyPage rowkeys = hbaseUtils.scanTable(useIndex, keyValues, endkey, limit);
            // 设置结果
            gqr.setResult(hbaseUtils.selectWithRowkeysResultKV(tableName, rowkeys.getRowkeys(), resultColumns, filterName, rc));
            // gqr.setResultString(gson.toJson(gqr.getResult()));
            gqr.setEndkey(rowkeys.getEndKey());

            if (null != filterName) {
                // 如果查询结果为不符合limit长度，需要继续查询，如此操作是因为过滤器过滤数据导致结果集不符合limit长度
                while (null != gqr.getResult() && gqr.getResult().size() > 0 && gqr.getResult().size() <= limit) {
                    rowkeys = hbaseUtils.scanTable(useIndex, keyValues, endkey, limit);
                    // 如果无查询结果，跳出循环
                    if (rowkeys.getRowkeys().size() <= 0) {
                        break;
                    }
                    // 设置结果
                    gqr.addResult(hbaseUtils.selectWithRowkeysResultKV(tableName, rowkeys.getRowkeys(), resultColumns, filterName, rc));
                    // gqr.setResultString(gson.toJson(gqr.getResult()));
                    gqr.setEndkey(rowkeys.getEndKey());
                }
            }

        } catch (Exception e) {
            if ("0".equals(rc.getResultCode())) {
                rc.setResultCode(ResultCode.INTERNAL_ERROR);
            }
            LOG.debug("scanTable error:" + e.getMessage());
            gqr.setException(new BigDataWareHouseException(e));
        } finally {
            gqr.setStatusCode(rc.getResultCode());
        }
        // 出口日志
        if (LOG.isDebugEnabled()) {
            LOG.debug("Exit queryGlobalTablePageReturnKV() method");
        }
        // 参数日志
        if (LOG.isDebugEnabled()) {
            StringBuffer buffer = new StringBuffer();
            buffer.append("GlobalQueryResult=");
            buffer.append(gqr);
            LOG.debug(buffer.toString());
        }
        return gqr;
    }

    /**
     * @Title: verifyGlobalIndex
     * @Description: TODO
     * @param tableName
     * @param resultColumns
     * @param kvInterface
     * @param rc
     * @return
     * @throws BigDataWareHouseException
     **/
    private Index verifyGlobalIndex(Table table, String[] resultColumns, KVInterface[] kvInterface, ResultCode rc) {
        // 入口日志
        if (LOG.isDebugEnabled()) {
            LOG.debug("Enter verifyGlobalIndex() method");
        }
        // 参数日志
        if (LOG.isDebugEnabled()) {
            StringBuffer buffer = new StringBuffer();
            buffer.append("table=");
            buffer.append(table);
            buffer.append("resultColumns=");
            buffer.append(resultColumns);
            buffer.append("kvInterface=");
            buffer.append(kvInterface);
            buffer.append("rc=");
            buffer.append(rc);
            LOG.debug(buffer.toString());
        }

        HbaseTable hbaseTable = null;
        if (table instanceof HbaseTable) {
            hbaseTable = (HbaseTable) table;
        } else {
            return null;
        }
        List<Index> indexes = hbaseTable.getGlobalIndexes();
        Index index = null;
        Index useIndex = null;
        List<Column> colList = null;
        boolean flag = true;
        String keys = "";
        for (int i = 0; i < kvInterface.length; i++) {
            keys += kvInterface[i].getKey();
        }

        for (int i = 0; i < indexes.size(); i++) {
            index = indexes.get(i);

            colList = index.getIndexColumnList();
            if (kvInterface.length == colList.size()) {
                String indexKeys = "";
                for (int j = 0; j < colList.size(); j++) {
                    // 基线
                    // indexKeys += colList.get(j).getHiveName();
                    // 北京
                    indexKeys += colList.get(j).getName();
                }
                if (keys.equals(indexKeys)) {
                    useIndex = index;
                    flag = false;
                    break;
                }
            }

        }

        if (flag) {
            rc.setResultCode(ResultCode.NO_INDEX);
            return null;
        }

        // 出口日志
        if (LOG.isDebugEnabled()) {
            LOG.debug("Exit verifyGlobalIndex() method");
        }
        // 参数日志
        if (LOG.isDebugEnabled()) {
            StringBuffer buffer = new StringBuffer();
            buffer.append("useIndex=");
            buffer.append(useIndex);
            LOG.debug(buffer.toString());
        }
        return useIndex;
    }

    /**
     * @Title: verifyCondition
     * @Description: TODO
     * @param tableName
     * @param columnMap
     * @param condition
     * @param rc
     * @throws BigDataWareHouseException
     **/
    private String verifyCondition(String tableName, Map<String, Column> columnMap, Condition condition, ResultCode rc) {
        // 入口日志
        if (LOG.isDebugEnabled()) {
            LOG.debug("Enter verifyCondition() method");
        }
        // 参数日志
        if (LOG.isDebugEnabled()) {
            StringBuffer buffer = new StringBuffer();
            buffer.append("tableName=");
            buffer.append(tableName);
            buffer.append("columnMap=");
            buffer.append(columnMap);
            buffer.append("condition=");
            buffer.append(condition);
            buffer.append("rc=");
            buffer.append(rc);
            LOG.debug(buffer.toString());
        }

        String resultStr = "success";
        if (condition != null && condition instanceof Conditions) {

            // 如果是Conditions类对象，获取cdlist 遍历进行判断
            for (Condition cd : ((Conditions) condition).getCdList()) {

                // 遍历并递归判断条件列是否配置，遇到未配置列返回false
                resultStr = verifyCondition(tableName, columnMap, cd, rc);
                if (!"success".equals(resultStr)) {
                    return resultStr;
                }
            }
        } else if (condition != null) {

            //// 如果不是Conditions类对象，则按照Condition对象处理

            // 判断列是否存在并返回结果
            if (!columnMap.containsKey(condition.getColumn())) {
                rc.setResultCode(ResultCode.COLUMN_NOT_EXISTS);
                resultStr = BigDataWareHouseException.buildMsg(BigDataWareHouseException.COLUMN_NOT_EXISTS, condition.getColumn(), tableName);
                return resultStr;
            }

            // 判断值是否存在
            if (null == condition.getValue() || "".equals(condition.getValue())) {
                rc.setResultCode(ResultCode.VALUE_NO_DATA);
                resultStr = BigDataWareHouseException.buildMsg(BigDataWareHouseException.VALUE_NO_DATA, condition.getColumn());
                return resultStr;
            }

            // 判断条件值是否合法
            Column column = columnMap.get(condition.getColumn());

            // 数值型的值判断
            if (column.getType().equals(Column.type_number)) {
                if (!NumberValidationUtils.isRealNumber(condition.getValue())) {
                    resultStr = BigDataWareHouseException.buildMsg(BigDataWareHouseException.VALUE_FORMAT_ERROR, condition.getColumn(), condition.getValue());
                    return resultStr;
                }
            }

            // 日期的值判断
            if (column.getType().equals(Column.type_date)) {
                if (!DateValidationUtils.isDate(condition.getValue())) {
                    resultStr = BigDataWareHouseException.buildMsg(BigDataWareHouseException.VALUE_FORMAT_ERROR, condition.getColumn(), condition.getValue());
                    return resultStr;
                }
            }
        }
        // 出口日志
        if (LOG.isDebugEnabled()) {
            LOG.debug("Exit verifyCondition() method");
        }
        return resultStr;
    }

    /**
     * 
     * @Title: commonCheckForInsert
     * @Description: TODO
     * @param tableName
     * @param columns
     * @param table
     * @param rc
     * @return
     */
    private String commonCheckForInsert(String tableName, String[] columns, Table table, ResultCode rc) {
        // 入口日志
        if (LOG.isDebugEnabled()) {
            LOG.debug("Enter commonCheckForInsert() method");
        }
        // 参数日志
        if (LOG.isDebugEnabled()) {
            StringBuffer buffer = new StringBuffer();
            buffer.append("tableName=");
            buffer.append(tableName);
            buffer.append("columns=");
            buffer.append(columns);
            buffer.append("table=");
            buffer.append(table);
            LOG.debug(buffer.toString());
        }

        String resultStr = "success";
        // 判断传入的列是否合法
        if (null == columns || columns.length == 0) {
            resultStr = BigDataWareHouseException.buildMsg(BigDataWareHouseException.COLUMNS_IS_EMPTY);
            rc.setResultCode(ResultCode.COLUMNS_NO_DATA);
            return resultStr;
        }

        // 判断传入的列是否在表中
        Set<Column> hiveCols = table.getHiveColumnsDistinct();
        for (String col : columns) {
            boolean flag = false;
            for (Column hiveCol : hiveCols) {
                if (hiveCol.getName().equals(col)) {
                    flag = true;
                    break;
                }
            }
            if (!flag) {
                resultStr = BigDataWareHouseException.buildMsg(BigDataWareHouseException.COLUMN_NOT_EXISTS, col, tableName);
                rc.setResultCode(ResultCode.COLUMN_NOT_EXISTS);
                return resultStr;
            }
        }
        // 出口日志
        if (LOG.isDebugEnabled()) {
            LOG.debug("Exit commonCheckForInsert() method");
        }
        return resultStr;
    }

    /**
     * 检查参数
     * 
     * @Title: commonCheck
     * @Description: TODO
     * @param tableName
     * @param columns
     * @param table
     * @param rc
     * @throws BigDataWareHouseException
     */
    private String commonCheck(String tableName, String[] columns, Table table, ResultCode rc) {
        // 入口日志
        if (LOG.isDebugEnabled()) {
            LOG.debug("Enter commonCheck() method");
        }
        // 参数日志
        if (LOG.isDebugEnabled()) {
            StringBuffer buffer = new StringBuffer();
            buffer.append("tableName=");
            buffer.append(tableName);
            buffer.append("columns=");
            buffer.append(columns);
            buffer.append("table=");
            buffer.append(table);
            LOG.debug(buffer.toString());
        }

        String resultStr = "success";
        // 判断传入的列是否合法
        if (null == columns || columns.length == 0) {
            resultStr = BigDataWareHouseException.buildMsg(BigDataWareHouseException.COLUMNS_IS_EMPTY);
            rc.setResultCode(ResultCode.COLUMN_NOT_EXISTS);
            return resultStr;
        }

        List<String> list = table.getColumnsStringList();
        for (String col : columns) {
            if (!list.contains(col)) {
                resultStr = BigDataWareHouseException.buildMsg(BigDataWareHouseException.COLUMN_NOT_EXISTS, col, tableName);
                return resultStr;
            }
        }
        // 出口日志
        if (LOG.isDebugEnabled()) {
            LOG.debug("Exit commonCheck() method");
        }
        return resultStr;
    }

    /**
     * 
     * @Title: streamInsertOrc
     * @Description: TODO
     * @param table
     * @param columns
     * @param addContent
     * @return
     * @throws BigDataWareHouseException
     */
    public boolean streamInsertOrc(OrcTable table, String[] columns, List<Object[]> addContent) throws BigDataWareHouseException {
        // 入口日志
        if (LOG.isDebugEnabled()) {
            LOG.debug("Enter streamInsertOrc() method");
        }
        // 参数日志
        if (LOG.isDebugEnabled()) {
            StringBuffer buffer = new StringBuffer();
            buffer.append("table=");
            buffer.append(table);
            buffer.append("columns=");
            buffer.append(columns);
            buffer.append("addContent=");
            buffer.append(addContent);
            LOG.debug(buffer.toString());
        }
        boolean returnMsg = hiveUtil.streamInsertOrc(table, columns, addContent);
        // 出口日志
        if (LOG.isDebugEnabled()) {
            LOG.debug("Exit streamInsertOrc() method");
        }
        return returnMsg;
    }

    /**
     * 
     * @Title: deleteOrcTable
     * @Description: TODO
     * @param conn
     * @param table
     * @param colNames
     * @param deleteContent
     * @param logicKey
     * @return
     * @throws BigDataWareHouseException
     */
    public boolean deleteOrcTable(Connection conn, OrcTable table, String[] colNames, List<String[]> deleteContent, String[] logicKey)
            throws BigDataWareHouseException {
        // 入口日志
        if (LOG.isDebugEnabled()) {
            LOG.debug("Enter deleteOrcTable() method");
        }
        // 参数日志
        if (LOG.isDebugEnabled()) {
            StringBuffer buffer = new StringBuffer();
            buffer.append("conn=");
            buffer.append(conn);
            buffer.append("table=");
            buffer.append(table);
            buffer.append("colNames=");
            buffer.append(colNames);
            buffer.append("deleteContent=");
            buffer.append(deleteContent);
            buffer.append("logicKey=");
            buffer.append(logicKey);
            LOG.debug(buffer.toString());
        }
        boolean returnMsg = hiveUtil.deleteOrcTable(conn, table, colNames, deleteContent, logicKey);
        // 出口日志
        if (LOG.isDebugEnabled()) {
            LOG.debug("Exit deleteOrcTable() method");
        }
        return returnMsg;
    }

    public boolean updateOrcTable(Connection conn, OrcTable table, String[] colNames, List<String[]> updateContent, String[] logicKey)
            throws BigDataWareHouseException {
        // 入口日志
        if (LOG.isDebugEnabled()) {
            LOG.debug("Enter updateOrcTable() method");
        }
        // 参数日志
        if (LOG.isDebugEnabled()) {
            StringBuffer buffer = new StringBuffer();
            buffer.append("conn=");
            buffer.append(conn);
            buffer.append("table=");
            buffer.append(table);
            buffer.append("colNames=");
            buffer.append(colNames);
            buffer.append("updateContent=");
            buffer.append(updateContent);
            buffer.append("logicKey=");
            buffer.append(logicKey);
            LOG.debug(buffer.toString());
        }
        boolean returnMsg = hiveUtil.updateOrcTable(conn, table, colNames, updateContent, logicKey);
        // 出口日志
        if (LOG.isDebugEnabled()) {
            LOG.debug("Exit updateOrcTable() method");
        }
        return returnMsg;
    }

    /**
     * @Title: getConnection
     * @Description: 获得连接
     * @return
     */
    public Connection getMyCatConnection() {
        // 入口日志
        if (LOG.isDebugEnabled()) {
            LOG.debug("Enter getMyCatConnection() method");
        }
        Connection conn = myCatUtils.getConnection();
        // 出口日志
        if (LOG.isDebugEnabled()) {
            LOG.debug("Exit getMyCatConnection() method");
        }
        return conn;
    }

    /**
     * @Title: save
     * @Description: 持久化
     * @param connection
     *            连接
     * @param tableName
     *            表名
     * @param dataList
     *            数据
     * @param columns
     *            列
     * @param columnLengthMap
     *            列长度
     * @return count
     * @throws BigDataWareHouseException
     * @throws SQLException
     */
    public long save(Connection connection, String tableName, List<Object[]> dataList, String[] columns, Map<String, Integer> columnLengthMap)
            throws BigDataWareHouseException {
        // 入口日志
        if (LOG.isDebugEnabled()) {
            LOG.debug("Enter save() method");
        }
        // 参数日志
        if (LOG.isDebugEnabled()) {
            StringBuffer buffer = new StringBuffer();
            buffer.append("connection=");
            buffer.append(connection);
            buffer.append("tableName=");
            buffer.append(tableName);
            buffer.append("dataList=");
            buffer.append(dataList);
            buffer.append("columns=");
            buffer.append(columns);
            buffer.append("columnLengthMap=");
            buffer.append(columnLengthMap);
            LOG.debug(buffer.toString());
        }
        long returnVal = 0;
        try {
            returnVal = myCatUtils.save(connection, tableName, dataList, columns, columnLengthMap);
        } catch (SQLException e) {
            throw new BigDataWareHouseException(e.getMessage(), e);
        }
        // 出口日志
        if (LOG.isDebugEnabled()) {
            LOG.debug("Exit save() method");
        }
        return returnVal;
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
        // 入口日志
        if (LOG.isDebugEnabled()) {
            LOG.debug("Enter saveBatch() method");
        }
        // 参数日志
        if (LOG.isDebugEnabled()) {
            StringBuffer buffer = new StringBuffer();
            buffer.append("connection=");
            buffer.append(connection);
            buffer.append("tableName=");
            buffer.append(tableName);
            buffer.append("inserSql=");
            buffer.append(inserSql);
            buffer.append("params=");
            buffer.append(params);
            LOG.debug(buffer.toString());
        }
        PreparedStatement pst = null;
        try {
            pst = myCatUtils.saveBatch(connection, tableName, inserSql, params);
        } catch (SQLException e) {
            throw new BigDataWareHouseException(e.getMessage(), e);
        }
        // 出口日志
        if (LOG.isDebugEnabled()) {
            LOG.debug("Exit saveBatch() method");
        }
        return pst;
    }

    public void main(String[] args) throws ClassNotFoundException, BigDataWareHouseException, InterruptedException {
        Conditions cons = new Conditions();
        // Condition con1 = new Condition();
        // con1.setColumn("seq");
        // con1.setValue("1000000000");

        // Condition con2 = new Condition();
        // con2.setColumn("str4");
        // con2.setValue("uesnwokvxq");

        // cons.getCdList().add(con1);
        // cons.getCdList().add(con2);

        try {
            metaUtils.init();
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        ESQueryResult res = queryESTable("test19", new String[] { "seq", "name", "rdm", "str4" }, cons, new ArrayList<Sort>(), new Limit(0, 10), "name1",
                "keyword");
        System.out.println(res.getResult().size());

        ESQueryResult res2 = queryESTable("test19", new String[] { "seq", "name", "rdm", "str4" }, cons, new ArrayList<Sort>(), new Limit(0, 10), "name2",
                "keyword");
        System.out.println(res2.getResult().size());

        ESQueryResult res3 = queryESTable("testuser1", new String[] { "id", "name" }, cons, new ArrayList<Sort>(), new Limit(0, 10), "name2", "keyword");
        System.out.println(res3.getResult().size());

    }

}
