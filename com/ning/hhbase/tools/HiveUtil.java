/**
 * Copyright: Copyright (c) 2016 
 * Company:东方网力科技股份有限公司
 * 
 * @author huangjinyan
 * @date 2016年8月15日 下午4:58:58
 * @version V1.0
 */
package com.ning.hhbase.tools;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hive.hcatalog.streaming.DelimitedInputWriter;
import org.apache.hive.hcatalog.streaming.HiveEndPoint;
import org.apache.hive.hcatalog.streaming.StreamingConnection;
import org.apache.hive.hcatalog.streaming.TransactionBatch;
import org.apache.log4j.Logger;

import com.ning.hhbase.bean.Column;
import com.ning.hhbase.bean.OrcTable;
import com.ning.hhbase.bean.ResultCode;
import com.ning.hhbase.bean.Table;
import com.ning.hhbase.common.Constants;
import com.ning.hhbase.config.NPBaseConfiguration;
import com.ning.hhbase.connection.ConnectionPoolManager;
import com.ning.hhbase.connection.ESHbaseConnection;
import com.ning.hhbase.engine.NPBaseEngine;
import com.ning.hhbase.exception.BigDataWareHouseException;


/**
 * @ClassName: HiveUtil
 * @Description: TODO
 * @author huangjinyan
 * @date 2016年8月16日 上午9:30:15
 *
 **/
public class HiveUtil {

    /**
     * @Fields LOG : 日志
     **/
    private static final Logger LOG = Logger.getLogger(HiveUtil.class);

    private static ConnectionPoolManager cpm = ConnectionPoolManager.getInstance();

    private NPBaseConfiguration config = null;
    
    public HiveUtil(NPBaseConfiguration config) {
        this.config = config;
    }

    public void setConfig(NPBaseConfiguration config) {
        this.config = config;
    }
    
    /**
     * @Title: getConnection
     * @Description: 获得连接
     * @return
     */
    public  Connection getConnection() {

        Connection connection = null;

        try {

            Class.forName("com.mysql.jdbc.Driver");
            String url = config.getValue(NPBaseConfiguration.HIVEMYSQL_URL);
            String user = config.getValue(NPBaseConfiguration.HIVEMYSQL_USER);
            String password = config.getValue(NPBaseConfiguration.HIVEMYSQL_PASSWORD);

            connection = DriverManager.getConnection(url, user, password);
            connection.setAutoCommit(false);
        } catch (Exception e) {
            LOG.error("Get MyCat Connection exception", e);
        }

        return connection;
    }

    /**
     * @Title: ddlTableWithSql
     * @Description: TODO
     * @param sql
     * @return
     * @throws BigDataWareHouseException
     **/
    public  String ddlTableWithSql(String sql) throws BigDataWareHouseException {
        if (sql == null || "".equals(sql)) {
            LogUtils.errorMsg(BigDataWareHouseException.SQL_NULL);
        }

        if (sql.toUpperCase().trim().startsWith("ALTER TABLE")) {
            return BigDataWareHouseException.TABLE_CANT_ALTER;
        }

        try {
            ESHbaseConnection ec = cpm.getConnection();

            Connection con = ec.getHiveConnection();
            Statement stmt = con.createStatement();
            stmt.execute(sql);
            cpm.close(ec);
            return "success";

        } catch (SQLException e) {
            LOG.error(e.getMessage(), e);
            LogUtils.errorMsg(e, e.getMessage());
        } finally {

        }
        return null;
    }

    /**
     * @Title: tableIsExists
     * @Description: 判断表是否存在
     * @param statement
     * @param tableName
     * @return
     * @throws BigDataWareHouseException
     **/
    private  boolean tableIsExists(Statement statement, String tableName) throws BigDataWareHouseException {
        String sql = "show tables '" + tableName + "'";
        ResultSet rs;
        try {
            rs = statement.executeQuery(sql);
            if (rs.next()) {
                return true;
            } else {
                return false;
            }
        } catch (SQLException e) {
            LOG.error(e.getMessage(), e);
            LogUtils.errorMsg(e, BigDataWareHouseException.HIVE_EXISTS_TABLE_FAIL);
        }
        return false;

    }

    private  String[] deleteCol(String[] cols, int index) {
        String[] colsNew = new String[cols.length - 1];
        boolean flag = false;
        for (int i = 0; i < cols.length; i++) {
            if (i == index) {
                flag = true;
            } else if (flag) {
                colsNew[i - 1] = cols[i];
            } else {
                colsNew[i] = cols[i];
            }
        }
        return colsNew;
    }

    public  boolean streamInsertOrc(OrcTable table, String[] columns, List<Object[]> addContent) throws BigDataWareHouseException {
        try {
            String dbName = "default";
            String tblName = table.getName();

            int partIndex = -1;
            for (int i = 0; i < columns.length; i++) {
                if (table.getPartitionColumn().getHiveName().equals(columns[i])) {
                    partIndex = i;
                }
            }
            columns = deleteCol(columns, partIndex);

            Map<String, List<Object[]>> parts = new HashMap<String, List<Object[]>>();
            for (int i = 0; i < addContent.size(); i++) {
                String[] line = (String[]) addContent.get(i);
                if (parts.get(line[partIndex]) == null || parts.get(line[partIndex]).size() == 0) {
                    List<Object[]> lines = new ArrayList<Object[]>();
                    lines.add(line);
                    parts.put(line[partIndex], lines);
                } else {
                    parts.get(line[partIndex]).add(line);
                }

            }

            Set<String> keys = parts.keySet();
            for (String key : keys) {

                List<Object[]> subContent = parts.get(key);

                ArrayList<String> partitionVals = new ArrayList<String>(1);
                // String serdeClass =
                // "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe";
                partitionVals.add(key);
                HiveEndPoint hiveEP = new HiveEndPoint(config.getValue(NPBaseConfiguration.HIVE_THRIFT_URL), dbName, tblName, partitionVals);

                StreamingConnection connection2 = hiveEP.newConnection(true);
                DelimitedInputWriter writer2;

                writer2 = new DelimitedInputWriter(columns, config.getValue(NPBaseConfiguration.DATA_ACCESS_SPLIT), hiveEP);

                TransactionBatch txnBatch = connection2.fetchTransactionBatch(10, writer2);

                ///// Batch 2 - First TXN
                txnBatch.beginNextTransaction();

                for (int i = 0; i < subContent.size(); i++) {
                    String[] aline = (String[]) subContent.get(i);
                    aline = deleteCol(aline, partIndex);

                    StringBuffer fullLine = new StringBuffer();
                    for (int j = 0; j < aline.length; j++) {
                        if(aline[j] == null || "".equals(aline[j])){
                            fullLine.append(" ");
                        }else{
                            fullLine.append(aline[j]);
                        }
                        
                        if (j != aline.length - 1) {
                            fullLine.append(config.getValue(NPBaseConfiguration.DATA_ACCESS_SPLIT));
                        }
                    }

                    txnBatch.write(fullLine.toString().getBytes());

                    // if (i % 10000 == 0) {
                    // txnBatch.commit();
                    // }

                }

                txnBatch.commit();

                txnBatch.close();
                connection2.close();
            }

        } catch (Exception e) {
            LogUtils.errorMsg(e, BigDataWareHouseException.DDL_EXEC_FAIL, "orc sqls");
            return false;
        }
        return true;
    }

    public  boolean deleteOrcTable(Connection conn, OrcTable table, String[] colNames, List<String[]> deleteContent, String[] logicKey)
            throws BigDataWareHouseException {
        try {
            String selectSql = "select " + table.getBucketColumn().getName() + " bucket," + table.getPartitionColumn().getName() + " partitioncol from "
                    + table.getName() + " where ";
            for (int i = 0; i < logicKey.length; i++) {
                selectSql += logicKey[i] + "=?";
                if (i != logicKey.length - 1) {
                    selectSql += " and ";
                }
            }
            List<Integer> rowKeyIndex = new ArrayList<Integer>();
            for (int i = 0; i < colNames.length; i++) {
                for (int j = 0; j < logicKey.length; j++) {
                    if (logicKey[j].equals(colNames[i])) {
                        rowKeyIndex.add(i);
                    }
                }
            }

            String sql = "delete from " + table.getName();
            sql += " where " + table.getBucketColumn().getName() + "=? and " + table.getPartitionColumn().getName() + "=?";

            for (int i = 0; i < logicKey.length; i++) {
                sql += " and " + logicKey[i] + "=?";
            }

            selectSql += " limit 1";

            PreparedStatement selPrest = (PreparedStatement) conn.prepareStatement(selectSql);
            PreparedStatement prest = (PreparedStatement) conn.prepareStatement(sql);
            for (int x = 0; x < deleteContent.size(); x++) {
                String[] aline = (String[]) deleteContent.get(x);

                for (int i = 0; i < logicKey.length; i++) {
                    selPrest.setString(i + 1, aline[rowKeyIndex.get(i)]);
                }
                String bucketVal = null;
                String partitionVal = null;
                ResultSet set = selPrest.executeQuery();
                while (set.next()) {
                    bucketVal = set.getString("bucket");
                    partitionVal = set.getString("partitioncol");
                }

                int i = 1;
                prest.setString(i++, bucketVal);
                prest.setString(i++, partitionVal);
                for (int j = 0; j < logicKey.length; j++) {
                    prest.setString(i++, aline[rowKeyIndex.get(j)]);
                }

                prest.execute();

            }
            selPrest.close();
            prest.close();

        } catch (Exception e) {
            LogUtils.errorMsg(e, BigDataWareHouseException.DDL_EXEC_FAIL, "orc sqls");
            return false;
        }

        return true;
    }

    public  boolean updateOrcTable(Connection conn, OrcTable table, String[] colNames, List<String[]> updateContent, String[] logicKey)
            throws BigDataWareHouseException {
        try {
            String selectSql = "select " + table.getBucketColumn().getName() + " as bucket," + table.getPartitionColumn().getName() + " as partitioncol from "
                    + table.getName() + " where ";
            for (int i = 0; i < logicKey.length; i++) {
                selectSql += logicKey[i] + "=?";
                if (i != logicKey.length - 1) {
                    selectSql += " and ";
                }
            }
            int bucketIndex = -1;
            int partitionIndex = -1;
            List<Integer> rowKeyIndex = new ArrayList<Integer>();
            String sql = "update " + table.getName() + " set ";
            for (int i = 0; i < colNames.length; i++) {

                if (table.getBucketColumn().getName().equals(colNames[i])) {
                    bucketIndex = i;
                    if (i == colNames.length - 1) {
                        sql = sql.substring(0, sql.length()-1);
                    }
                } else if (table.getPartitionColumn().getName().equals(colNames[i])) {
                    partitionIndex = i;
                    if (i == colNames.length - 1) {
                        sql = sql.substring(0, sql.length()-1);
                    }
                } else {
                    sql += colNames[i];
                    sql += "=?";
                    if (i != colNames.length - 1) {
                        sql += ",";
                    }
                }
                for (int j = 0; j < logicKey.length; j++) {
                    if (logicKey[j].equals(colNames[i])) {
                        rowKeyIndex.add(i);
                    }
                }
            }
            sql += " where " + colNames[bucketIndex] + "=? and " + colNames[partitionIndex] + "=? ";
            if (rowKeyIndex != null) {
                for (int i = 0; i < logicKey.length; i++) {
                    sql += "and " + colNames[rowKeyIndex.get(i)] + "=? ";
                }

            }
            
            if(partitionIndex > bucketIndex){
                colNames = deleteCol(colNames, partitionIndex);
                colNames = deleteCol(colNames, bucketIndex);
            }else{
                colNames = deleteCol(colNames, bucketIndex);
                colNames = deleteCol(colNames, partitionIndex);
            }
            
            selectSql = selectSql + " limit 1";

            PreparedStatement selPrest = (PreparedStatement) conn.prepareStatement(selectSql);
            PreparedStatement prest = (PreparedStatement) conn.prepareStatement(sql);
            for (int x = 0; x < updateContent.size(); x++) {
                String[] oriLine = (String[]) updateContent.get(x);
                
                String[] aline = (String[]) updateContent.get(x);
                
                if(partitionIndex > bucketIndex){
                    aline = deleteCol(aline, partitionIndex);
                    aline = deleteCol(aline, bucketIndex);
                }else{
                    aline = deleteCol(aline, bucketIndex);
                    aline = deleteCol(aline, partitionIndex);
                }
                
                
                for (int i = 0; i < logicKey.length; i++) {
                    selPrest.setString(i + 1, oriLine[rowKeyIndex.get(i)]);
                }

                String bucketVal = null;
                String partitionVal = null;
                ResultSet set = selPrest.executeQuery();
                while (set.next()) {
                    bucketVal = set.getString("bucket");
                    partitionVal = set.getString("partitioncol");
                }

                int i = 1;
                for (; i < colNames.length + 1; i++) {
                    prest.setString(i, aline[i - 1]);
                }
                prest.setString(i, bucketVal);
                prest.setString(++i, partitionVal);
                for (int j = 0; j < logicKey.length; j++) {
                    prest.setString(++i, oriLine[rowKeyIndex.get(j)]);
                }

                prest.execute();

            }
            selPrest.close();
            prest.close();
        } catch (Exception e) {
            LogUtils.errorMsg(e, BigDataWareHouseException.DDL_EXEC_FAIL, "orc sqls");
            return false;
        }
        return true;
    }

    public  List<Table> getOrctables(Connection connection) throws BigDataWareHouseException {

        Map<String, String> tbNames = new HashMap<String, String>();
        List<Table> tables = new ArrayList<Table>();
        Table table = null;
        try {
            String tblSql = "select x.tbl_name name,x.cd_id id,x.tbl_id tbid,x.SD_ID sid from SERDES se right join (select t.tbl_id,t.tbl_name,s.serde_id,s.CD_ID,s.SD_ID from TBLS t left join SDS s on t.sd_id=s.sd_id) x on x.serde_id=se.serde_id where se.SLIB='org.apache.hadoop.hive.ql.io.orc.OrcSerde'";
            PreparedStatement pst = (PreparedStatement) connection.prepareStatement(tblSql);
            ResultSet rs = pst.executeQuery();
            while (rs.next()) {
                String name = rs.getString("name");
                String id = rs.getString("id");
                String tbid = rs.getString("tbid");
                String sdid = rs.getString("sid");
                table = new OrcTable();
                table.setName(name);
                tables.add(table);
                tbNames.put(name, id + ";" + tbid + ";" + sdid);
            }
            pst.close();

            String colSql = "select c.COLUMN_NAME name,c.TYPE_NAME type from COLUMNS_V2 c where c.cd_id='";
            String partSql = "select PKEY_NAME name,PKEY_TYPE type from PARTITION_KEYS where tbl_id='";
            String bucketSql = "select BUCKET_COL_NAME name from BUCKETING_COLS where sd_id='";
            for (int i = 0; i < tables.size(); i++) {
                List<Column> columnList = new ArrayList<Column>();
                Table thisTable = tables.get(i);
                String name = thisTable.getName();
                String id = tbNames.get(name);
                PreparedStatement pst1 = (PreparedStatement) connection.prepareStatement(colSql + id.split(";")[0] + "'");
                ResultSet rs1 = pst1.executeQuery();
                Column col = null;
                while (rs1.next()) {
                    col = new Column();
                    String colName = rs1.getString("name");
                    String type = rs1.getString("type");
                    col.setName(colName);
                    col.setHiveType(type);
                    col.setType("string");
                    col.setHiveName(colName);
                    columnList.add(col);
                }

                pst1.close();

                PreparedStatement pst2 = (PreparedStatement) connection.prepareStatement(partSql + id.split(";")[1] + "'");
                ResultSet rs2 = pst2.executeQuery();
                Column partCol = null;
                while (rs2.next()) {
                    partCol = new Column();
                    String colName = rs2.getString("name");
                    String type = rs2.getString("type");
                    partCol.setName(colName);
                    partCol.setHiveType(type);
                    partCol.setType(type);
                    partCol.setHiveName(colName);

                }
                OrcTable orcTable = (OrcTable) thisTable;
                orcTable.setPartitionColumn(partCol);
                columnList.add(partCol);
                pst2.close();

                PreparedStatement pst3 = (PreparedStatement) connection.prepareStatement(bucketSql + id.split(";")[2] + "'");
                ResultSet rs3 = pst3.executeQuery();
                Column bucketCol = null;
                while (rs3.next()) {
                    bucketCol = new Column();
                    String colName = rs3.getString("name");
                    bucketCol.setName(colName);
                    bucketCol.setHiveName(colName);

                }
                orcTable.setBucketColumn(bucketCol);
                pst3.close();

                thisTable.setColumnList(columnList);
            }
            connection.close();
        } catch (SQLException e) {
            LogUtils.errorMsg(e, BigDataWareHouseException.DDL_EXEC_FAIL, "orc sqls");
        }

        return tables;
    }
    
    public List<Table> getTxtTables(Connection connection)  throws BigDataWareHouseException{
        Map<String, String> tbNames = new HashMap<String, String>();
        List<Table> tables = new ArrayList<Table>();
        Table table = null;
        try {
            String tblSql = "select y.name name,y.id id,y.tbid tbid,y.sid sid from (select x.tbl_name name,x.cd_id id,x.tbl_id tbid,x.SD_ID sid from SERDES se right join (select t.tbl_id,t.tbl_name,s.serde_id,s.CD_ID,s.SD_ID from TBLS t left join SDS s on t.sd_id=s.sd_id) x on x.serde_id=se.serde_id where se.SLIB='org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe') y inner join SDS z on y.sid=z.SD_ID where z.INPUT_FORMAT='org.apache.hadoop.mapred.TextInputFormat' and z.OUTPUT_FORMAT='org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'";
            PreparedStatement pst = (PreparedStatement) connection.prepareStatement(tblSql);
            ResultSet rs = pst.executeQuery();
            while (rs.next()) {
                String name = rs.getString("name");
                String id = rs.getString("id");
                String tbid = rs.getString("tbid");
                String sdid = rs.getString("sid");
                table = new Table();
                table.setName(name);
                tables.add(table);
                tbNames.put(name, id + ";" + tbid + ";" + sdid);
            }
            pst.close();

            String colSql = "select c.COLUMN_NAME name,c.TYPE_NAME type from COLUMNS_V2 c where c.cd_id='";
            for (int i = 0; i < tables.size(); i++) {
                List<Column> columnList = new ArrayList<Column>();
                Table thisTable = tables.get(i);
                String name = thisTable.getName();
                String id = tbNames.get(name);
                PreparedStatement pst1 = (PreparedStatement) connection.prepareStatement(colSql + id.split(";")[0] + "'");
                ResultSet rs1 = pst1.executeQuery();
                Column col = null;
                while (rs1.next()) {
                    col = new Column();
                    String colName = rs1.getString("name");
                    String type = rs1.getString("type");
                    col.setName(colName);
                    col.setHiveType(type);
                    col.setType("string");
                    col.setHiveName(colName);
                    columnList.add(col);
                }

                pst1.close();

                thisTable.setColumnList(columnList);
            }
            connection.close();
        } catch (SQLException e) {
            LogUtils.errorMsg(e, BigDataWareHouseException.DDL_EXEC_FAIL, "txt sqls");
        }

        return tables;
    }

    public  ResultSet queryTableWithSql(String sql, ResultCode rc) throws BigDataWareHouseException {
        if (sql == null || "".equals(sql)) {
            rc.setResultCode(ResultCode.VALUE_NO_DATA);
            LogUtils.errorMsg(BigDataWareHouseException.SQL_NULL);
        }
        ResultSet rs = null;
        try {
            ESHbaseConnection ec = cpm.getConnection();

            Connection con = ec.getHiveConnection();
            Statement stmt = con.createStatement();
            rs = stmt.executeQuery(sql);
            cpm.close(ec);

        } catch (SQLException e) {
            rc.setResultCode(ResultCode.INTERNAL_ERROR);
            LOG.error(e.getMessage(), e);
            LogUtils.errorMsg(e, e.getMessage());
        } 
        return rs;
    }

    /**
     * 通用取结果方案,返回list
     * 
     * @param rs
     * @return
     * @throws SQLException
     */
    public  List<Map<String,Object>> extractData(ResultSet rs) throws SQLException {
        ResultSetMetaData md = rs.getMetaData();
        int num = md.getColumnCount();
        List<Map<String,Object>> listOfRows = new ArrayList<Map<String,Object>>();
        while (rs.next()) {
            Map<String,Object> mapOfColValues = new HashMap<String,Object>(num);
            for (int i = 1; i <= num; i++) {
                mapOfColValues.put(md.getColumnName(i), rs.getObject(i));
            }
            listOfRows.add(mapOfColValues);
        }
        return listOfRows;
    }

   
}
