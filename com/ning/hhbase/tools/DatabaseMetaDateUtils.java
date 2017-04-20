/*
 * 文 件 名:  DatabaseMetaDateUtils.java
 * 版    权:  NetPosa Technologies, Ltd. Copyright YYYY-YYYY,  All rights reserved
 * 描    述:  <描述>
 * 修 改 人:  
 * 修改时间:  2016年8月4日
 */
package com.ning.hhbase.tools;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.log4j.Logger;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.ning.hhbase.config.NPBaseConfiguration;

/**
 * @ClassName: DatabaseMetaDateUtils
 * @Description: 数据库元数据
 * @author huangjinyan
 * @date 2016年8月15日 下午1:38:54
 *
 **/
public final class DatabaseMetaDateUtils {
    /**
     * @Fields log : 日志
     **/
    private static Logger LOG = Logger.getLogger(DatabaseMetaDateUtils.class);

    /**
     * @Fields INSTANCE : 单例实例
     **/
    private static DatabaseMetaDateUtils INSTANCE = null;

    /**
     * @Fields dbMetaData : 数据示例
     **/
    private DatabaseMetaData dbMetaData = null;

    /**
     * @Fields con : 连接
     **/
    private Connection con = null;

    /**
     * @Fields tableNames : 表名
     **/
    private JSONArray tableNames = new JSONArray();

    /**
     * @Fields tableInfo : 表字段详情，详情属性： columnName dataTypeName columnSize
     **/
    private JSONObject tableInfo = new JSONObject();

    /**
     * @Fields tableFieldsMap : 表字段映射
     **/
    private JSONObject tableFieldsMap = new JSONObject();

    private NPBaseConfiguration config = null;
    
    public void setConfig(NPBaseConfiguration config) {
        this.config = config;
    }
    
    private DatabaseMetaDateUtils(NPBaseConfiguration config) {
        this.config = config;
    }

    /**
     * @Title: getInstance
     * @Description: 获取实例
     * @return 实例
     **/
    public static DatabaseMetaDateUtils getInstance(NPBaseConfiguration config) {
        if(INSTANCE == null){
            INSTANCE = new DatabaseMetaDateUtils(config);
        }
        return INSTANCE;
    }

    /**
     * @Title: init
     * @Description: 初始化表和表结构信息
     * @throws Exception Exception
     **/
    public void init() throws Exception {
        tableNames = new JSONArray();
        tableInfo = new JSONObject();
        tableFieldsMap = new JSONObject();

        // 再次去load
        getDatabaseMetaData();

        getAllTableList(null);

        // 关闭
        closeCon();
    }

    /**
     * @Title: getAllTableNames
     * @Description: 获取所有的表名
     * @return TableNames
     **/
    public JSONArray getAllTableNames() {
        return tableNames;
    }

    /**
     * @Title: getFieldNamesByTableName
     * @Description: 获取指定表名的所有列名
     * @param tableName tableName
     * @return FieldNames
     **/
    public JSONArray getFieldNamesByTableName(String tableName) {
        if (tableFieldsMap.containsKey(tableName)) {
            return tableFieldsMap.getJSONArray(tableName);
        }
        return new JSONArray();
    }

    /**
     * @Title: getFieldInfoByTableName
     * @Description: 获取指定表名的字段信息
     * @param tableName tableName
     * @return FieldInfo
     **/
    public JSONArray getFieldInfoByTableName(String tableName) {
        if (tableInfo.containsKey(tableName)) {
            return tableInfo.getJSONArray(tableName);
        }
        return new JSONArray();
    }

    /**
     * @Title: getDatabaseMetaData
     * @Description: TODO
     * @throws Exception
     **/
    private void getDatabaseMetaData() throws Exception {

        try {
            if (dbMetaData == null) {
                Class.forName("com.mysql.jdbc.Driver");
                String url = config.getValue(NPBaseConfiguration.MYCAT_URL);
                String user = config.getValue(NPBaseConfiguration.MYCAT_USER);
                String password = config.getValue(NPBaseConfiguration.MYCAT_PASSWORD);
                con = DriverManager.getConnection(url, user, password);
                dbMetaData = con.getMetaData();
            }
        } catch (ClassNotFoundException e) {
            LOG.error(e.getMessage());
            throw e;
        } catch (SQLException e) {
            LOG.error(e.getMessage());
            throw e;
        }
    }

    /**
     * 获得该用户下面的所有表
     * 
     * @throws Exception Exception
     */
    private void getAllTableList(String schemaName) {
        try {
            // table type. Typical types are "TABLE", "VIEW", "SYSTEM TABLE",
            // "GLOBAL TEMPORARY", "LOCAL TEMPORARY", "ALIAS", "SYNONYM".
            String[] types = { "TABLE" };
            ResultSet rs = dbMetaData.getTables(null, schemaName, "%", types);
            while (rs.next()) {
                // 存放表名
                tableNames.add(rs.getString("TABLE_NAME"));

                // 存放表名信息
                tableInfo.put(rs.getString("TABLE_NAME"), getTableColumns(null, rs.getString("TABLE_NAME")));
            }
        } catch (SQLException e) {
            LOG.error(e.getMessage(), e);
        }
    }

    /**
     * 获得表或视图中的所有列信息
     */
    private JSONArray getTableColumns(String schemaName, String tableName) {
        JSONArray result = new JSONArray();
        try {
            ResultSet rs = dbMetaData.getColumns(null, schemaName, tableName, "%");
            JSONObject info;

            // 所有的列名
            JSONArray fieldNames = new JSONArray();
            while (rs.next()) {
                info = new JSONObject();
                info.put("columnName", rs.getString("COLUMN_NAME")); // 列名
                info.put("dataTypeName", rs.getString("TYPE_NAME")); // java.sql.Types类型
                                                                     // 名称
                info.put("columnSize", rs.getString("COLUMN_SIZE")); // 列大小

                // 加载表名和字段名的关系
                fieldNames.add(rs.getString("COLUMN_NAME"));
                result.add(info);
            }

            tableFieldsMap.put(tableName, fieldNames);
        } catch (SQLException e) {
            LOG.error(e.getMessage(), e);
        }

        return result;
    }

    /**
     * @Title: closeCon
     * @Description: 获取连接
     **/
    private void closeCon() {
        try {
            if (con != null) {
                dbMetaData = null;
                con.close();
            }
        } catch (SQLException e) {
            LOG.error(e.getMessage(), e);
        }
    }

    /**
     * <一句话功能简述> <功能详细描述>
     * 
     * @param args
     * @see [类、类#方法、类#成员]
     */
    public static void main(String[] args) {
        // DatabaseMetaDateApplication metaData = new
        // DatabaseMetaDateApplication();
        // metaData.getDataBaseInformations();
        // metaData.getAllTableList(null);
        // metaData.getAllViewList(null);
        // metaData.getAllSchemas();
        // metaData.getTableColumnsInfo(null, "ods_people");
        // metaData.getIndexInfo(null, "zsc_admin");
        // metaData.getAllPrimaryKeys(null, "zsc_admin");
        // metaData.getAllExportedKeys(null, "zsc_admin");
        // metaData.closeCon();
    }
}
