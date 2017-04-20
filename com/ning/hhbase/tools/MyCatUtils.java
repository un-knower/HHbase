/**
 * Copyright: Copyright (c) 2016 
 * Company:东方网力科技股份有限公司
 * 
 * @author Administrator
 * @date 2016年8月19日 下午4:57:02
 * @version V1.0
 */
package com.ning.hhbase.tools;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import com.mysql.jdbc.PreparedStatement;
import com.ning.hhbase.common.Constants;
import com.ning.hhbase.config.NPBaseConfiguration;
import com.ning.hhbase.engine.NPBaseEngine;
import com.ning.hhbase.exception.BigDataWareHouseException;

/**
 * @ClassName: MyCatUtils
 * @Description: MyCat工具类
 * @author Administrator
 * @date 2016年8月19日 下午4:57:02
 *
 */
public class MyCatUtils {

    /**
     * @Fields LOG : 日志
     **/
    private static Logger LOG = Logger.getLogger(MyCatUtils.class);

    private NPBaseConfiguration config = null;
    
    private static MyCatUtils instance =null;
    
    public void setConfig(NPBaseConfiguration config) {
        this.config = config;
    }
    
    
    private MyCatUtils(NPBaseConfiguration config){
        this.config = config;
    }
    
    /**
     * @Title: getInstance
     * @Description: 单例
     * @return
     */
    public static MyCatUtils getInstance(NPBaseConfiguration config) {
        if(instance == null){
            instance = new MyCatUtils(config);
        }
        return instance;
    }

    /**
     * @Title: getConnection
     * @Description: 获得连接
     * @return
     */
    public Connection getConnection() {

        Connection connection = null;

        try {

            Class.forName("com.mysql.jdbc.Driver");
            String url = config.getValue(NPBaseConfiguration.MYCAT_URL);
            String user = config.getValue(NPBaseConfiguration.MYCAT_USER);
            String password = config.getValue(NPBaseConfiguration.MYCAT_PASSWORD);

            connection = DriverManager.getConnection(url, user, password);
            connection.setAutoCommit(false);
        } catch (Exception e) {
            LOG.error("Get MyCat Connection exception", e);
        }

        return connection;
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
    public  long save(Connection connection, String tableName, List<Object[]> dataList, String[] columns, Map<String, Integer> columnLengthMap) throws BigDataWareHouseException, SQLException {

        long count = 0;

        String sql = conversionSQL(tableName, columns);

        // 将字段长度按columns顺序放入数组中
        int[] columnLengthArray = new int[columns.length];

        for (int i = 0; i < columns.length; i++) {

            if (null != columnLengthMap.get(columns[i])) {

                columnLengthArray[i] = columnLengthMap.get(columns[i]);

            } else {

                columnLengthArray[i] = 0;
            }
        }

        try {


            PreparedStatement pst =  (PreparedStatement) connection.prepareStatement(sql);

            if (CollectionUtils.isNotEmpty(dataList)) {

                // 如果接收数据的长度大于mysql表列字段的长度 ，则截取
                for (int i = 0; i < dataList.size(); i++) {
                    
                 // 封装参数
                    Object[] params = new Object[columns.length];
                    
                    // 主键ID设置
                    params[0] = System.currentTimeMillis();

                    Object[] objects = dataList.get(i);

                    for (int j = 1; j < params.length; j++) {

                        if (columnLengthArray[j] > 0) {

                            params[j] = subString((String) objects[j-1], columnLengthArray[j]);

                        } else {

                            params[j ] = objects[j-1];

                        }
                    }
                    
                  //批量插入数据
                    for(int j=0;j<params.length;j++){
                        pst.setObject(j+1, params[j]);
                    }
                    
                    pst.addBatch();
                    count++;
                }
            }
            //执行批量语句
            pst.executeBatch();
            //由于mycat被设置为自动提交，此时无需手动提交
            connection.commit();
            pst.close();
          

        } catch (BigDataWareHouseException e) {
            LogUtils.errorMsg(e, BigDataWareHouseException.DDL_EXEC_FAIL, sql);
            throw e;
        }
        catch (SQLException e)
        {
            LogUtils.errorMsg(e, BigDataWareHouseException.DDL_EXEC_FAIL, sql);
            e.printStackTrace();
            throw e;
        }

        return count;
    }
    
    /**
     * 
     * <批量保存mysql数据>
     * <功能详细描述>
     * @param connection
     * @param tableName
     * @param inserSql
     * @param params
     * @return
     * @throws SQLException
     * @see [类、类#方法、类#成员]
     */
    public  PreparedStatement saveBatch(Connection connection, String tableName, String inserSql, Object params[]) throws SQLException{
        PreparedStatement pst =  (PreparedStatement) connection.prepareStatement(inserSql);
        int i =1;
        for(Object param:params){
            pst.setObject(i, param);
            i++;
        }
        pst.addBatch();
        connection.commit();
        return pst;
    }

    /**
     * @Title: conversionSQL
     * @Description: 组装SQL
     * @param tableName 表名
     * @param columns 列
     * @return SQL
     */
    private  String conversionSQL(String tableName, String[] columns) {

        // 拼写SQL
        StringBuffer into_sql = new StringBuffer("insert into " + tableName + "(");

        StringBuffer value_sql = new StringBuffer(") values( ");

        for (int i = 0; i < columns.length - 1; i++) {

            into_sql.append(columns[i] + ",");
            value_sql.append("?,");
        }

        into_sql.append(columns[columns.length - 1]);
        value_sql.append("?)");
        into_sql.append(value_sql);

        return into_sql.toString();
    }

    /**
     * @Title: subString
     * @Description: 截取字符串的长度
     * @param str 字符串
     * @param subSLength 需要截取的长度
     * @return String
     * @throws BigDataWareHouseException
     */
    private  String subString(String str, int subSLength) throws BigDataWareHouseException {

        String subStr = "";
        if (StringUtils.isNotBlank(str)) {
            if(str.length()>subSLength){
                subStr = str.substring(0, subSLength);
            }else{
                subStr = str;
            }
            
        }
        
//以下为截取字节长度
//        if (StringUtils.isNotBlank(str)) {
//
//            // 截取字节数
//            int tempSubLength = subSLength;
//
//            // 截取的子串
//            subStr = str.substring(0, str.length() < subSLength ? str.length() : subSLength);
//
//            int subStrByetsL;
//
//            try {
//
//                subStrByetsL = subStr.getBytes("UTF-8").length;
//
//                while (subStrByetsL > tempSubLength) {
//
//                    int subSLengthTemp = --subSLength;
//                    subStr = str.substring(0, subSLengthTemp > str.length() ? str.length() : subSLengthTemp);
//                    subStrByetsL = subStr.getBytes("UTF-8").length;
//                }
//            } catch (Exception e) {
//
//                LogUtils.errorMsg(e, BigDataWareHouseException.COLUMNS_IS_EMPTY, str);
//            }
//        }

        return subStr;
    }
    
    
}
