/**
 * Copyright: Copyright (c) 2016 
 * Company:东方网力科技股份有限公司
 * 
 * @author huangjinyan
 * @date 2016年8月15日 下午1:45:02
 * @version V1.0
 */
package com.ning.hhbase.tools;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.script.ScriptEngineManager;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import com.ning.hhbase.bean.Column;
import com.ning.hhbase.bean.GlobalQueryRowkeyPage;
import com.ning.hhbase.bean.Index;
import com.ning.hhbase.bean.KeyValue;
import com.ning.hhbase.bean.KeyValueRange;
import com.ning.hhbase.bean.ResultCode;
import com.ning.hhbase.bean.Table;
import com.ning.hhbase.common.Constants;
import com.ning.hhbase.config.NPBaseConfiguration;
import com.ning.hhbase.connection.ConnectionPoolManager;
import com.ning.hhbase.connection.ESHbaseConnection;
import com.ning.hhbase.engine.NPBaseEngine;
import com.ning.hhbase.exception.BigDataWareHouseException;
import com.ning.hhbase.filter.BaseFilter;

/**
 * @ClassName: Hbase操作接口
 * @Description: TODO
 * @author ningyexin
 * @date 2016年8月15日 下午4:50:05
 *
 **/
public class HbaseUtils {

    /**
     * @Fields LOG : 日志
     **/
    private static Logger LOG = Logger.getLogger(HbaseUtils.class);

    /**
     * @Fields cpm : 连接池
     **/
    private static ConnectionPoolManager cpm = ConnectionPoolManager.getInstance();

    private EsUtils esUtils = null;

    private NPBaseConfiguration config = null;

    public HbaseUtils(NPBaseConfiguration config) {
        this.config = config;
        esUtils = new EsUtils(config);
    }

    public void setConfig(NPBaseConfiguration config) {
        this.config = config;
    }

    /**
     * 
     * @Title: accessInsertToTable
     * @Description: TODO
     * @param table
     * @param columns
     * @param dataList
     * @param rowKey
     * @param keyStructFlag
     * @param ec
     * @return
     * @throws BigDataWareHouseException
     */
    public String accessInsertToTable(Table table, String[] columns, List<Object[]> dataList, String rowKey, boolean keyStructFlag, ESHbaseConnection ec)
            throws BigDataWareHouseException {
        Connection connection = ec.getHbaseConnection();
        // 获取表信息
        List<Column> allCols = table.getColumnList();
        String tableName = table.getName();
        insertCommon(table, columns, dataList, rowKey, keyStructFlag, connection, allCols, tableName);
        return "success";
    }

    /**
     * @Title: insertToTable
     * @Description: 向hbase表插入数据
     * @param tableName
     * @param columns
     * @param dataList
     * @param rowKey
     * @param keyStructFlag
     * @return 结果状态吗
     * @throws BigDataWareHouseException
     * @throws ClassNotFoundException
     * @throws InterruptedException
     * @throws UnsupportedEncodingException
     **/
    public String insertToTable(Table tableInfo, String[] columns, List<Object[]> dataList, String rowKey, boolean keyStructFlag)
            throws BigDataWareHouseException {

        ESHbaseConnection ec = cpm.getConnection();
        Connection connection = ec.getHbaseConnection();

        // 获取表信息
        List<Column> allCols = tableInfo.getColumnList();
        String tableName = tableInfo.getName();
        if (!isExist(tableName, connection)) {
            cpm.close(ec);
            LogUtils.errorMsg(BigDataWareHouseException.TABLE_NOT_EXISTS, tableName);
        } else {
            try {
                insertCommon(tableInfo, columns, dataList, rowKey, keyStructFlag, connection, allCols, tableName);
            } catch (BigDataWareHouseException e) {
                cpm.close(ec);
                throw e;
            }

        }

        cpm.close(ec);

        return "success";
    }

    /**
     * @Title: rangeScanRange
     * @Description: TODO
     * @param index
     * @param keyValueRanges
     * @param endkey
     * @param limit
     * @return
     * @throws BigDataWareHouseException
     **/
    public GlobalQueryRowkeyPage rangeScanRange(Index index, KeyValueRange[] keyValueRanges, String endkey, int limit) throws BigDataWareHouseException {
        GlobalQueryRowkeyPage gqrp = new GlobalQueryRowkeyPage();
        String endRowKey = null;

        ESHbaseConnection ec = cpm.getConnection();
        Connection connection = ec.getHbaseConnection();
        List<String> returnList = new ArrayList<String>();

        String indexName = index.getIndexName();
        HTable table = initTable(indexName, connection);

        if (null == table) {
            cpm.close(ec);
            LOG.error("has no table");
            LogUtils.errorMsg(BigDataWareHouseException.TABLE_NOT_EXISTS, indexName);
        } else {
            Scan scan = new Scan();

            String startRow = "";
            String endRow = "";
            List<Column> colList = index.getIndexColumnList();
            String midCache = null;
            String endMidCache = null;
            Column col = null;
            for (int j = 0; j < colList.size(); j++) {
                col = colList.get(j);
                midCache = "";
                endMidCache = "";
                if (j != colList.size() - 1) {
                    if (col.getIndexLength() > keyValueRanges[j].getStartValue().length()) {
                        for (int j2 = 0; j2 < col.getIndexLength() - keyValueRanges[j].getStartValue().length(); j2++) {
                            midCache += new Character((char) 0);
                        }
                        startRow += keyValueRanges[j].getStartValue() + midCache;

                    } else if (col.getIndexLength() < keyValueRanges[j].getStartValue().length()) {
                        startRow += keyValueRanges[j].getStartValue().substring(0, col.getIndexLength());

                    } else {
                        startRow += keyValueRanges[j].getStartValue();
                    }

                    if (col.getIndexLength() > keyValueRanges[j].getEndValue().length()) {
                        for (int j2 = 0; j2 < col.getIndexLength() - keyValueRanges[j].getEndValue().length(); j2++) {
                            endMidCache += new Character((char) 0);
                        }
                        endRow += keyValueRanges[j].getEndValue() + endMidCache;
                    } else if (col.getIndexLength() < keyValueRanges[j].getEndValue().length()) {
                        endRow += keyValueRanges[j].getEndValue().substring(0, col.getIndexLength());

                    } else {
                        endRow += keyValueRanges[j].getEndValue();
                    }
                    startRow += new Character((char) 127);
                    endRow += new Character((char) 127);
                } else {
                    startRow += keyValueRanges[j].getStartValue();
                    endRow += keyValueRanges[j].getEndValue();
                }

            }
            endRow += new Character((char) 127) + new Character((char) 127);

            if (null != endkey) {
                scan.setStartRow(endkey.getBytes());
            } else {
                scan.setStartRow(startRow.getBytes());
            }
            scan.setStopRow(endRow.getBytes());

            ResultScanner rScanner;
            try {
                rScanner = table.getScanner(scan);
                Result[] results = rScanner.next(limit + 1);
                Result result = null;
                for (int i = 0; i < results.length; i++) {
                    result = results[i];
                    if (null != result.getRow() && (i < (results.length - 1) || results.length != (limit + 1))) {
                        String[] rowKey = Bytes.toString(result.getRow()).split("" + new Character((char) 127));
                        if (rowKey[rowKey.length - 1] != null) {
                            returnList.add(rowKey[rowKey.length - 1]);
                        }
                    }
                    if (results.length == (limit + 1) && null != result) {
                        endRowKey = Bytes.toString(result.getRow());
                    }
                }
            } catch (IOException e) {
                cpm.close(ec);
                LogUtils.errorMsg(e, BigDataWareHouseException.READ_RESULT_ERROR);
            }
        }

        cpm.close(ec);
        gqrp.setEndKey(endRowKey);
        gqrp.setRowkeys(returnList);

        return gqrp;
    }

    /**
     * @Title: scanTable
     * @Description: TODO
     * @param index
     * @param keyValues
     * @param endkey
     * @param limit
     * @return
     * @throws BigDataWareHouseException
     **/
    public GlobalQueryRowkeyPage scanTable(Index index, KeyValue[] keyValues, String endkey, int limit) throws BigDataWareHouseException {
        GlobalQueryRowkeyPage gqrp = new GlobalQueryRowkeyPage();
        String endRowKey = null;
        ESHbaseConnection ec = cpm.getConnection();
        Connection connection = ec.getHbaseConnection();
        List<String> returnList = new ArrayList<String>();
        String indexName = index.getIndexName();
        HTable table = initTable(indexName, connection);
        if (null == table) {
            cpm.close(ec);
            LOG.error("has no table");
            LogUtils.errorMsg(BigDataWareHouseException.TABLE_NOT_EXISTS, indexName);
        } else {
            Scan scan = new Scan();

            String startRow = "";
            String endRow = "";

            List<Column> colList = index.getIndexColumnList();
            String midCache = null;
            Column col = null;
            for (int j = 0; j < colList.size(); j++) {
                col = colList.get(j);
                midCache = "";

                if (j != colList.size() - 1) {
                    if (col.getIndexLength() > keyValues[j].getValue().length()) {
                        for (int j2 = 0; j2 < col.getIndexLength() - keyValues[j].getValue().length(); j2++) {
                            midCache += new Character((char) 0);
                        }
                        startRow += keyValues[j].getValue() + midCache;
                    } else if (col.getIndexLength() < keyValues[j].getValue().length()) {
                        startRow += keyValues[j].getValue().substring(0, col.getIndexLength());

                    } else {
                        startRow += keyValues[j].getValue();
                    }
                    startRow += new Character((char) 127);
                } else {
                    startRow += keyValues[j].getValue();
                }

            }
            endRow += startRow + new Character((char) 127) + new Character((char) 127);

            if (endkey != null) {
                scan.setStartRow(endkey.getBytes());
            } else {
                scan.setStartRow(startRow.getBytes());
            }

            scan.setStopRow(endRow.getBytes());

            ResultScanner rScanner;
            try {
                rScanner = table.getScanner(scan);
                Result[] results = rScanner.next(limit + 1);
                Result result = null;
                for (int i = 0; i < results.length; i++) {
                    result = results[i];
                    if (null != result.getRow() && (i < (results.length - 1) || results.length != (limit + 1))) {
                        String[] rowKey = Bytes.toString(result.getRow()).split("" + new Character((char) 127));
                        if (rowKey[rowKey.length - 1] != null) {
                            returnList.add(rowKey[rowKey.length - 1]);
                        }
                    }
                    if (results.length == (limit + 1) && null != result) {
                        endRowKey = Bytes.toString(result.getRow());
                    }
                }
            } catch (IOException e) {
                cpm.close(ec);
                LogUtils.errorMsg(e, BigDataWareHouseException.READ_RESULT_ERROR);
            }
        }
        cpm.close(ec);
        gqrp.setEndKey(endRowKey);
        gqrp.setRowkeys(returnList);
        return gqrp;
    }

    /**
     * @Title: scanTable
     * @Description: TODO
     * @param index
     * @param keyValues
     * @return
     * @throws BigDataWareHouseException
     **/
    public List<String> scanTable(Index index, KeyValue[] keyValues) throws BigDataWareHouseException {
        ESHbaseConnection ec = cpm.getConnection();
        Connection connection = ec.getHbaseConnection();
        List<String> returnList = new ArrayList<String>();
        String indexName = index.getIndexName();
        HTable table = initTable(indexName, connection);

        if (null == table) {
            cpm.close(ec);
            LOG.error("has no table");
            LogUtils.errorMsg(BigDataWareHouseException.TABLE_NOT_EXISTS, indexName);
        } else {
            Scan scan = new Scan();

            String startRow = "";
            String endRow = "";

            List<Column> colList = index.getIndexColumnList();
            String midCache = null;
            Column col = null;
            for (int j = 0; j < colList.size(); j++) {
                col = colList.get(j);
                midCache = "";

                if (j != colList.size() - 1) {
                    if (col.getIndexLength() > keyValues[j].getValue().length()) {
                        for (int j2 = 0; j2 < col.getIndexLength() - keyValues[j].getValue().length(); j2++) {
                            midCache += new Character((char) 0);
                        }
                        startRow += keyValues[j].getValue() + midCache;
                    } else if (col.getIndexLength() < keyValues[j].getValue().length()) {
                        startRow += keyValues[j].getValue().substring(0, col.getIndexLength());

                    } else {
                        startRow += keyValues[j].getValue();
                    }
                    startRow += new Character((char) 127);
                } else {
                    startRow += keyValues[j].getValue();
                }

            }
            endRow += startRow + new Character((char) 127) + new Character((char) 127);

            scan.setStartRow(startRow.getBytes());
            scan.setStopRow(endRow.getBytes());

            ResultScanner rScanner;
            try {
                rScanner = table.getScanner(scan);
                for (Result result : rScanner) {

                    if (null != result.getRow()) {
                        String[] rowKey = Bytes.toString(result.getRow()).split("" + new Character((char) 127));
                        if (rowKey[rowKey.length - 1] != null) {
                            returnList.add(rowKey[rowKey.length - 1]);
                        }
                    }
                }
            } catch (IOException e) {
                cpm.close(ec);
                LogUtils.errorMsg(e, BigDataWareHouseException.READ_RESULT_ERROR);
            }
        }
        cpm.close(ec);
        return returnList;
    }

    /**
     * @Title: rangeScanRange
     * @Description: TODO
     * @param index
     * @param keyValueRanges
     * @return
     * @throws BigDataWareHouseException
     **/
    public List<String> rangeScanRange(Index index, KeyValueRange[] keyValueRanges) throws BigDataWareHouseException {
        ESHbaseConnection ec = cpm.getConnection();
        Connection connection = ec.getHbaseConnection();
        List<String> returnList = new ArrayList<String>();
        String indexName = index.getIndexName();
        HTable table = initTable(indexName, connection);
        if (null == table) {
            cpm.close(ec);
            LOG.error("has no table");
            LogUtils.errorMsg(BigDataWareHouseException.TABLE_NOT_EXISTS, indexName);
        } else {
            Scan scan = new Scan();

            String startRow = "";
            String endRow = "";
            List<Column> colList = index.getIndexColumnList();
            String midCache = null;
            String endMidCache = null;
            Column col = null;
            for (int j = 0; j < colList.size(); j++) {
                col = colList.get(j);
                midCache = "";
                endMidCache = "";
                if (j != colList.size() - 1) {
                    if (col.getIndexLength() > keyValueRanges[j].getStartValue().length()) {
                        for (int j2 = 0; j2 < col.getIndexLength() - keyValueRanges[j].getStartValue().length(); j2++) {
                            midCache += new Character((char) 0);
                        }
                        startRow += keyValueRanges[j].getStartValue() + midCache;

                    } else if (col.getIndexLength() < keyValueRanges[j].getStartValue().length()) {
                        startRow += keyValueRanges[j].getStartValue().substring(0, col.getIndexLength());

                    } else {
                        startRow += keyValueRanges[j].getStartValue();
                    }

                    if (col.getIndexLength() > keyValueRanges[j].getEndValue().length()) {
                        for (int j2 = 0; j2 < col.getIndexLength() - keyValueRanges[j].getEndValue().length(); j2++) {
                            endMidCache += new Character((char) 0);
                        }
                        endRow += keyValueRanges[j].getEndValue() + endMidCache;
                    } else if (col.getIndexLength() < keyValueRanges[j].getEndValue().length()) {
                        endRow += keyValueRanges[j].getEndValue().substring(0, col.getIndexLength());

                    } else {
                        endRow += keyValueRanges[j].getEndValue();
                    }
                    startRow += new Character((char) 127);
                    endRow += new Character((char) 127);
                } else {
                    startRow += keyValueRanges[j].getStartValue();
                    endRow += keyValueRanges[j].getEndValue();
                }

            }
            endRow += new Character((char) 127) + new Character((char) 127);

            scan.setStartRow(startRow.getBytes());
            scan.setStopRow(endRow.getBytes());

            ResultScanner rScanner;
            try {
                rScanner = table.getScanner(scan);
                for (Result result : rScanner) {

                    if (null != result.getRow()) {
                        String[] rowKey = Bytes.toString(result.getRow()).split("" + new Character((char) 127));
                        if (rowKey[rowKey.length - 1] != null) {
                            returnList.add(rowKey[rowKey.length - 1]);
                        }
                    }
                }
            } catch (IOException e) {
                cpm.close(ec);
                LogUtils.errorMsg(e, BigDataWareHouseException.READ_RESULT_ERROR);
            }
        }
        cpm.close(ec);
        return returnList;
    }

    /**
     * @Title: rangeScan
     * @Description: TODO
     * @param index
     * @param values
     * @return
     * @throws BigDataWareHouseException
     **/
    public List<String> rangeScan(Index index, String[] values) throws BigDataWareHouseException {
        ESHbaseConnection ec = cpm.getConnection();
        Connection connection = ec.getHbaseConnection();
        List<String> returnList = new ArrayList<String>();
        String indexName = index.getIndexName();
        HTable table = initTable(indexName, connection);
        if (null == table) {
            cpm.close(ec);
            LOG.error("has no table");
            LogUtils.errorMsg(BigDataWareHouseException.TABLE_NOT_EXISTS, indexName);
        } else {

            Scan scan = new Scan();

            String startRow = "";
            String endRow = "";

            List<Column> colList = index.getIndexColumnList();
            String midCache = null;
            Column col = null;

            for (int j = 0; j < colList.size(); j++) {
                col = colList.get(j);
                midCache = "";

                if (j != colList.size() - 1) {
                    if (col.getIndexLength() > values[j].length()) {
                        for (int j2 = 0; j2 < col.getIndexLength() - values[j].length(); j2++) {
                            midCache += new Character((char) 0);
                        }
                        startRow += values[j] + midCache;
                    } else {
                        startRow += values[j];
                    }
                    startRow += new Character((char) 127);
                } else {
                    startRow += values[j];
                }

            }
            endRow += startRow + new Character((char) 127) + new Character((char) 127);

            scan.setStartRow(startRow.getBytes());
            scan.setStopRow(endRow.getBytes());

            ResultScanner rScanner;
            try {
                rScanner = table.getScanner(scan);
                for (Result result : rScanner) {

                    if (null != result.getRow()) {
                        String[] rowKey = Bytes.toString(result.getRow()).split("" + new Character((char) 127));
                        if (rowKey[rowKey.length - 1] != null) {
                            returnList.add(rowKey[rowKey.length - 1]);
                        }
                    }
                }
            } catch (IOException e) {
                cpm.close(ec);
                LogUtils.errorMsg(e, BigDataWareHouseException.READ_RESULT_ERROR);
            }
        }

        cpm.close(ec);
        return returnList;

    }

    /**
     * 
     * @Title: selectWithRowkeyRange
     * @Description: TODO
     * @param tableName
     * @param startRow
     * @param endRow
     * @param columns
     * @param object
     * @param object2
     * @return
     * @throws BigDataWareHouseException
     */
    public List<Object[]> selectWithRowkeyRange(String tableName, String startRow, String endRow, String[] columns, String filterName, ResultCode rc)
            throws BigDataWareHouseException {
        ESHbaseConnection ec = cpm.getConnection();
        Connection connection = ec.getHbaseConnection();

        BaseFilter bf = null;

        if (null != filterName) {
            bf = esUtils.getFilterInstance(filterName, rc).getFilterInstance();
        }

        List<Object[]> returnList = new ArrayList<Object[]>();
        Object[] returnLine;

        HTable table = initTable(tableName, connection);

        if (null == table) {
            cpm.close(ec);
            LOG.error("has no table");
            LogUtils.errorMsg(BigDataWareHouseException.TABLE_NOT_EXISTS, tableName);
        } else {
            Scan scan = new Scan();
            if (startRow != null && startRow.length() > 0) {
                scan.setStartRow(startRow.getBytes());
            }
            if (endRow != null && endRow.length() > 0) {
                scan.setStopRow(endRow.getBytes());
            }
            ResultScanner scanner = null;
            try {
                scanner = table.getScanner(scan);
                Iterator<Result> itr = scanner.iterator();
                Result result = null;
                String colVal = null;
                int index = 0;
                while (itr.hasNext()) {
                    if (index > 1000) {
                        break;
                    }

                    result = itr.next();
                    if (result == null) {
                        continue;
                    }
                    returnLine = new Object[columns.length];
                    for (int i = 0; i < columns.length; i++) {
                        if (null != result.getRow()) {
                            if ("key".equals(columns[i])) {
                                colVal = Bytes.toString(result.getRow());
                            } else {
                                colVal = Bytes.toString(result.getValue(result.raw()[0].getFamily(), columns[i].getBytes()));
                            }
                            if (null != colVal && !"".equals(colVal)) {
                                returnLine[i] = colVal;
                            }
                        }

                    }

                    if (null != result.getRow()) {
                        esUtils.filterd(bf, returnList, returnLine, rc);
                    }
                    index++;
                }

            } catch (IOException e) {
                cpm.close(ec);
                LogUtils.errorMsg(e, BigDataWareHouseException.SELECT_ERROR, e.getMessage());
            }

            tableClose(tableName, table);
        }
        cpm.close(ec);
        return returnList;
    }

    /**
     * @Title: scanTable
     * @Description: 查询hbase数据方法
     * @param tableName
     * @param rowkeys
     * @param columns
     * @param filterName
     * @param rc
     * @return 查询结果
     * @throws BigDataWareHouseException
     * @throws ClassNotFoundException
     * @throws InterruptedException
     **/
    public List<Object[]> scanTable(String tableName, String[] columns, String filterName, ResultCode rc) throws BigDataWareHouseException {
        ESHbaseConnection ec = cpm.getConnection();
        Connection connection = ec.getHbaseConnection();

        BaseFilter bf = null;

        if (null != filterName) {
            bf = esUtils.getFilterInstance(filterName, rc).getFilterInstance();
        }

        List<Object[]> returnList = new ArrayList<Object[]>();
        Object[] returnLine;

        HTable table = initTable(tableName, connection);

        if (null == table) {
            cpm.close(ec);
            LOG.error("has no table");
            LogUtils.errorMsg(BigDataWareHouseException.TABLE_NOT_EXISTS, tableName);
        } else {
            Scan scan = new Scan();

            ResultScanner scanner = null;
            Result[] results = null;
            try {
                scanner = table.getScanner(scan);
                results = scanner.next(10);

                String colVal = null;
                for (Result result : results) {
                    returnLine = new Object[columns.length];
                    for (int i = 0; i < columns.length; i++) {
                        if (null != result.getRow()) {
                            if ("key".equals(columns[i])) {
                                colVal = Bytes.toString(result.getRow());
                            } else {
                                colVal = Bytes.toString(result.getValue(result.raw()[0].getFamily(), columns[i].getBytes()));
                            }
                            if (null != colVal && !"".equals(colVal)) {
                                returnLine[i] = colVal;
                            }
                        }

                    }

                    if (null != result.getRow()) {
                        esUtils.filterd(bf, returnList, returnLine, rc);
                    }
                }

                tableClose(tableName, table);
            } catch (IOException e) {
                cpm.close(ec);
                LogUtils.errorMsg(e, BigDataWareHouseException.SELECT_ERROR, e.getMessage());
            }
        }
        cpm.close(ec);
        return returnList;
    }

    /**
     * @Title: selectWithRowkeys
     * @Description: 查询hbase数据方法
     * @param tableName
     * @param rowkeys
     * @param columns
     * @param filterName
     * @param rc
     * @return 查询结果
     * @throws BigDataWareHouseException
     * @throws ClassNotFoundException
     * @throws InterruptedException
     **/
    public List<Object[]> selectWithRowkeys(String tableName, List<String> rowkeys, String[] columns, String filterName, ResultCode rc)
            throws BigDataWareHouseException {
        ESHbaseConnection ec = cpm.getConnection();
        Connection connection = ec.getHbaseConnection();

        BaseFilter bf = null;

        if (null != filterName) {
            bf = esUtils.getFilterInstance(filterName, rc).getFilterInstance();
        }

        List<Object[]> returnList = new ArrayList<Object[]>();
        Object[] returnLine;

        HTable table = initTable(tableName, connection);

        if (null == table) {
            cpm.close(ec);
            LOG.error("has no table");
            LogUtils.errorMsg(BigDataWareHouseException.TABLE_NOT_EXISTS, tableName);
        } else {
            List<Get> getList = new ArrayList<Get>();
            for (int i = 0; i < rowkeys.size(); i++) {
                if (rowkeys.get(i) != null && !"".equals(rowkeys.get(i))) {
                    getList.add(new Get(Bytes.toBytes(rowkeys.get(i))));
                }
            }

            Result[] results = null;
            try {
                results = table.get(getList);

                String colVal = null;
                for (Result result : results) {
                    returnLine = new Object[columns.length];
                    for (int i = 0; i < columns.length; i++) {
                        if (null != result.getRow()) {
                            if ("key".equals(columns[i])) {
                                colVal = Bytes.toString(result.getRow());
                            } else {
                                colVal = Bytes.toString(result.getValue(result.raw()[0].getFamily(), columns[i].getBytes()));
                            }
                            if (null != colVal && !"".equals(colVal)) {
                                returnLine[i] = colVal;
                            }
                        }

                    }

                    if (null != result.getRow()) {
                        esUtils.filterd(bf, returnList, returnLine, rc);
                    }
                }

                tableClose(tableName, table);
            } catch (IOException e) {
                cpm.close(ec);
                LogUtils.errorMsg(e, BigDataWareHouseException.SELECT_ERROR, e.getMessage());
            }
        }
        cpm.close(ec);
        return returnList;
    }

    /**
     * 
     * @Title: selectWithRowkeysResultKV
     * @Description: TODO
     * @param tableName
     * @param rowkeys
     * @param columns
     * @param filterName
     * @param rc
     * @return
     * @throws BigDataWareHouseException
     */
    public List<Map<String, Object>> selectWithRowkeysResultKV(String tableName, List<String> rowkeys, String[] columns, String filterName, ResultCode rc)
            throws BigDataWareHouseException {
        ESHbaseConnection ec = cpm.getConnection();
        Connection connection = ec.getHbaseConnection();

        BaseFilter bf = null;

        if (null != filterName) {
            bf = esUtils.getFilterInstance(filterName, rc).getFilterInstance();
        }

        List<Map<String, Object>> returnList = new ArrayList<Map<String, Object>>();
        Map<String, Object> returnLine;

        HTable table = initTable(tableName, connection);

        if (null == table) {
            cpm.close(ec);
            LOG.error("has no table");
            LogUtils.errorMsg(BigDataWareHouseException.TABLE_NOT_EXISTS, tableName);
        } else {
            List<Get> getList = new ArrayList<Get>();
            for (int i = 0; i < rowkeys.size(); i++) {
                if (rowkeys.get(i) != null && !"".equals(rowkeys.get(i))) {
                    getList.add(new Get(Bytes.toBytes(rowkeys.get(i))));
                }
            }

            Result[] results = null;
            try {
                results = table.get(getList);

                String colVal = null;
                for (Result result : results) {
                    returnLine = new HashMap<String, Object>(columns.length);
                    for (int i = 0; i < columns.length; i++) {
                        if (null != result.getRow()) {
                            if ("key".equals(columns[i])) {
                                colVal = Bytes.toString(result.getRow());
                            } else {
                                colVal = Bytes.toString(result.getValue(result.raw()[0].getFamily(), columns[i].getBytes()));
                            }
                            if (null != colVal && !"".equals(colVal)) {
                                // returnLine[i] = colVal;
                                returnLine.put(columns[i], colVal);
                            }
                        }

                    }

                    if (null != result.getRow()) {
                        esUtils.filterdResultKV(bf, returnList, returnLine, rc);
                    }
                }

                tableClose(tableName, table);
            } catch (IOException e) {
                cpm.close(ec);
                LogUtils.errorMsg(e, BigDataWareHouseException.SELECT_ERROR, e.getMessage());
            }
        }
        cpm.close(ec);
        return returnList;
    }

    /**
     * @Title: initTable
     * @Description: 创建表对象
     * @param tableName
     * @param connection
     * @return 表对象
     **/
    private HTable initTable(String tableName, Connection connection) {
        HTable table;
        try {
            table = (HTable) connection.getTable(TableName.valueOf(tableName));
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
        return table;
    }

    /**
     * 
     * @Title: createTable
     * @Description: 创建数据库表
     * @param tableName
     * @param columnFamilys
     * @throws BigDataWareHouseException
     * @throws InterruptedException
     * @throws ClassNotFoundException
     */
    public String createTable(Table table, boolean esFlag) throws BigDataWareHouseException {
        ESHbaseConnection ec = cpm.getConnection();
        Connection connection = ec.getHbaseConnection();

        // 新建一个数据库管理员
        HBaseAdmin hAdmin = null;
        String tableName = table.getName();
        try {

            hAdmin = (HBaseAdmin) connection.getAdmin();
        } catch (Exception e) {
            cpm.close(ec);
            LOG.error(e.getMessage(), e);
            LogUtils.errorMsg(e, BigDataWareHouseException.CREATE_HBASE_ADMIN_FAIL);
        }
        try {
            if (null == tableName || "".equals(tableName)) {
                LogUtils.errorMsg(BigDataWareHouseException.TABLE_NAME_NULL);
            } else if (hAdmin.tableExists(tableName)) {
                LogUtils.errorMsg(BigDataWareHouseException.TABLE_EXISTS, tableName);
                System.exit(0);
            } else {

                // 新建一个表的描述
                HTableDescriptor tableDesc = new HTableDescriptor(tableName);

                // 在描述里添加列族
                tableDesc.addFamily(new HColumnDescriptor(table.getColumnList().get(1).getFamilyName()));

                // 根据配置好的描述建表
                try {
                    hAdmin.createTable(tableDesc);
                } catch (IOException e) {
                    LogUtils.errorMsg(e, BigDataWareHouseException.CREATE_TABLE_FAIL, tableName);
                }
                return "success";
            }
            hAdmin.close();
        } catch (IOException e) {
            cpm.close(ec);
            LogUtils.errorMsg(e, BigDataWareHouseException.CREATE_TABLE_FAIL, tableName);
        }
        cpm.close(ec);
        return BigDataWareHouseException.CREATE_TABLE_FAIL;
    }

    /**
     * @Title: deleteData
     * @Description: 删除数据库表
     * @param table
     * @param rowKey
     * @return
     * @throws ClassNotFoundException
     * @throws BigDataWareHouseException
     * @throws InterruptedException
     **/
    public String batchDeleteData(Table tableInfo, String[] columns, List<Object[]> dataList, String rowKey, boolean keyStructFlag)
            throws BigDataWareHouseException {
        ESHbaseConnection ec = cpm.getConnection();
        Connection connection = ec.getHbaseConnection();
        HTable hTable = initTable(tableInfo.getName(), connection);
        String res = null;

        List<Integer[]> structIdxs = new ArrayList<Integer[]>();
        List<Column> resStructCols = tableInfo.getStructCols();

        for (Column col : resStructCols) {
            List<Column> structCols = col.getStructCols();
            Integer[] idxs = new Integer[structCols.size()];
            boolean flag = false;
            for (int k = 0; k < structCols.size(); k++) {
                Column structCol = structCols.get(k);
                for (int i = 0; i < columns.length; i++) {
                    if (structCol.getName().equals(columns[i])) {
                        idxs[k] = i;
                        flag = true;
                    }
                }
            }
            if (flag) {
                structIdxs.add(idxs);
            }

        }

        int rowKeyIdx = -1;
        for (int i = 0; i < columns.length; i++) {
            if (rowKey != null && rowKey.equals(columns[i])) {
                rowKeyIdx = i;
            }
        }

        if (rowKeyIdx == -1 && keyStructFlag == false) {
            cpm.close(ec);
            LogUtils.errorMsg(BigDataWareHouseException.ROWKEY_NOT_EXISTS, rowKey, tableInfo.getName());
        }

        List<Delete> dels = new ArrayList<Delete>();
        Delete del = null;
        for (int i = 0; i < dataList.size(); i++) {

            String[] line = (String[]) dataList.get(i);
            if (keyStructFlag && structIdxs.size() > 0) {
                Integer[] idxs = null;

                for (int j = 0; j < resStructCols.size(); j++) {
                    Column col = resStructCols.get(j);
                    if (col.getName().equals(rowKey)) {
                        idxs = structIdxs.get(j);
                        // structIdxs.remove(j);
                        break;
                    }
                }

                String rowkey = "";
                for (int j = 0; j < idxs.length; j++) {
                    if (idxs[j] != null) {
                        rowkey += line[idxs[j]];
                    }
                    if (j != idxs.length - 1) {
                        rowkey += "" + new Character((char) 2);
                    }
                }
                del = new Delete(rowkey.getBytes());
            } else {
                del = new Delete(line[rowKeyIdx].getBytes());
            }

            dels.add(del);
        }

        try {
            hTable.delete(dels);
            res = "success";
        } catch (IOException e) {
            LOG.error(e.getMessage(), e);
            res = "fail";
            LogUtils.errorMsg(e, BigDataWareHouseException.HBASE_DELETE_DATA_FAIL);
        } finally {
            cpm.close(ec);
        }

        return res;
    }

    /**
     * @Title: deleteData
     * @Description: 删除数据库表
     * @param table
     * @param rowKey
     * @return
     * @throws ClassNotFoundException
     * @throws BigDataWareHouseException
     * @throws InterruptedException
     **/
    public String deleteData(Table table, String rowKey) throws BigDataWareHouseException {
        ESHbaseConnection ec = cpm.getConnection();
        Connection connection = ec.getHbaseConnection();
        HTable hTable = initTable(table.getName(), connection);
        String res = null;
        Delete del = new Delete(Bytes.toBytes(rowKey));
        try {
            hTable.delete(del);
            res = "success";
        } catch (IOException e) {
            LOG.error(e.getMessage(), e);
            res = "fail";
            LogUtils.errorMsg(e, BigDataWareHouseException.HBASE_DELETE_DATA_FAIL);
        } finally {
            cpm.close(ec);
        }

        return res;
    }

    /**
     * @Title: isExist
     * @Description: 判断表是否存在
     * @param tableName
     * @param connection
     * @return
     * @throws BigDataWareHouseException
     **/
    private boolean isExist(String tableName, Connection connection) throws BigDataWareHouseException {
        HBaseAdmin ha = null;
        try {
            ha = (HBaseAdmin) connection.getAdmin();
        } catch (Exception e) {
            LogUtils.errorMsg(e, BigDataWareHouseException.CREATE_HBASE_ADMIN_FAIL);
        }

        try {
            return ha.tableExists(tableName);
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }

    }

    /**
     * @Title: tableClose
     * @Description: TODO
     * @param tableName
     * @param table
     * @throws BigDataWareHouseException
     **/
    private void tableClose(String tableName, HTable table) throws BigDataWareHouseException {
        try {
            table.close();
        } catch (IOException e) {
            LogUtils.errorMsg(e, BigDataWareHouseException.TABLE_CLOSE_ERROR, tableName);
        }
    }

    /**
     * @Title: tableFlush
     * @Description: TODO
     * @param tableName
     * @param table
     * @throws BigDataWareHouseException
     **/
    private void tableFlush(String tableName, HTable table) throws BigDataWareHouseException {
        try {
            table.flushCommits();
        } catch (IOException e) {
            LogUtils.errorMsg(e, BigDataWareHouseException.TABLE_FLUSH_ERROR, tableName);
        }
    }

    private void insertCommon(Table tableInfo, String[] columns, List<Object[]> dataList, String rowKey, boolean keyStructFlag, Connection connection,
            List<Column> allCols, String tableName) throws BigDataWareHouseException {
        HTable table = initTable(tableName, connection);
        table.setAutoFlush(false, true);

        try {
            double tmpDubl = Double.valueOf(new ScriptEngineManager().getEngineByName("JavaScript").eval(config.getValue(NPBaseConfiguration.HBASE_BUFFER_SIZE)).toString());
            table.setWriteBufferSize((int) tmpDubl);
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
            LogUtils.errorMsg(e, BigDataWareHouseException.BUFFER_SIZE_NOT_AVLIABLE, "" + config.getValue(NPBaseConfiguration.HBASE_BUFFER_SIZE));
        }

        List<Integer[]> structIdxs = new ArrayList<Integer[]>();
        List<Column> resStructCols = tableInfo.getStructCols();

        for (Column col : resStructCols) {
            List<Column> structCols = col.getStructCols();
            Integer[] idxs = new Integer[structCols.size()];
            boolean flag = false;
            for (int k = 0; k < structCols.size(); k++) {
                Column structCol = structCols.get(k);
                for (int i = 0; i < columns.length; i++) {
                    if (structCol.getName().equals(columns[i])) {
                        idxs[k] = i;
                        flag = true;
                    }
                }
            }
            if (flag) {
                structIdxs.add(idxs);
            }

        }

        int rowKeyIdx = -1;
        for (int i = 0; i < columns.length; i++) {
            if (rowKey != null && rowKey.equals(columns[i])) {
                rowKeyIdx = i;
            }
        }

        if (rowKeyIdx == -1 && keyStructFlag == false) {
            LogUtils.errorMsg(BigDataWareHouseException.ROWKEY_NOT_EXISTS, rowKey, tableName);
        }

        Put put = null;
        for (int i = 0; i < dataList.size(); i++) {
            String[] line = (String[]) dataList.get(i);
            if (keyStructFlag && structIdxs.size() > 0) {
                Integer[] idxs = null;

                for (int j = 0; j < resStructCols.size(); j++) {
                    Column col = resStructCols.get(j);
                    if (col.getName().equals(rowKey)) {
                        idxs = structIdxs.get(j);
                        // structIdxs.remove(j);
                        break;
                    }
                }

                String rowkey = "";
                for (int j = 0; j < idxs.length; j++) {
                    if (idxs[j] != null) {
                        rowkey += line[idxs[j]];
                    }
                    if (j != idxs.length - 1) {
                        rowkey += "" + new Character((char) 2);
                    }
                }
                put = new Put(rowkey.getBytes());

            } else {
                put = new Put(line[rowKeyIdx].getBytes());
            }
            if (keyStructFlag && structIdxs.size() > 1) {
                Integer[] idxs = null;
                for (int j = 0; j < resStructCols.size(); j++) {
                    idxs = structIdxs.get(j);
                    String value = "";
                    for (int k = 0; k < idxs.length; k++) {
                        if (idxs[k] != null) {
                            value += line[idxs[k]];
                        }
                        if (j != idxs.length - 1) {
                            value += "" + new Character((char) 2);
                        }
                    }

                    try {
                        put.add(tableInfo.getColumnList().get(1).getFamilyName().getBytes(), resStructCols.get(j).getName().getBytes(),
                                value.getBytes("utf-8"));
                    } catch (UnsupportedEncodingException e) {
                        LOG.debug("UnsupportedEncodingException for data:" + line[j]);
                        continue;
                    }
                }
            }
            if (line != null && line.length > 0) {
                for (int j = 0; j < line.length; j++) {
                    for (int k = 0; k < allCols.size(); k++) {
                        if (allCols.get(k).getName().equals(columns[j])) {
                            // 一个PUT代表一行数据，再NEW一个PUT表示第二行数据,每行一个唯一的ROWKEY，此处rowkey为put构造方法中传入的值
                            try {
                                put.add(tableInfo.getColumnList().get(1).getFamilyName().getBytes(), columns[j].getBytes(), line[j].getBytes("utf-8"));
                            } catch (UnsupportedEncodingException e) {
                                LOG.debug("UnsupportedEncodingException for data:" + line[j]);
                                continue;
                            }
                        }

                    }

                }
            } else {
                LogUtils.errorMsg(BigDataWareHouseException.NO_DATA);
            }

            try {
                // put.setTTL(5);
                table.put(put);
            } catch (IOException e) {
                LogUtils.errorMsg(e, BigDataWareHouseException.INSERT_ERROR, e.getMessage());
            }
        }

        tableFlush(tableName, table);
        tableClose(tableName, table);
    }

}
