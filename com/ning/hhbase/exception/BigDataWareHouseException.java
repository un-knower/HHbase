/**
 * Copyright: Copyright (c) 2016 
 * Company:东方网力科技股份有限公司
 * 
 * @author huangjinyan
 * @date 2016年8月15日 下午2:24:22
 * @version V1.0
 */
package com.ning.hhbase.exception;

/**
 * @ClassName: ESHbaseException
 * @Description: 异常
 * @author ningyexin
 * @date 2016年8月15日 下午2:24:22
 *
 **/
public class BigDataWareHouseException extends Exception {

    private Throwable errorCause = null;
    
    private static final long serialVersionUID = 1191876397503769831L;

    public static final String TABLE_NOT_EXISTS = "表：{1}不存在";

    public static final String BUFFER_SIZE_NOT_AVLIABLE = "Hbase数据插入缓存大小：{1}不合法!";

    public static final String ROWKEY_NOT_EXISTS = "声明的主键字段：{1}在表：{2}中不存在！";

    public static final String COLUMN_NOT_EXISTS = "字段：{1}在表：{2}中不存在！";

    public static final String VALUE_NO_DATA = "条件中字段：{1}不存在！";

    public static final String VALUE_FORMAT_ERROR = "条件中字段：{1}值{2}格式不正确！";

    public static final String INSERT_ERROR = "插入数据异常：{1}";

    public static final String SELECT_ERROR = "查询数据异常：{1}";

    public static final String CREATE_TABLE_FAIL = "创建表：{1}失败！";

    public static final String TABLE_EXISTS = "表 ：{1}已存在！";

    public static final String CREATE_HBASE_ADMIN_FAIL = "创建Hbase管理员失败！";

    public static final String MAPPING_COLS_NOT_EXISTS = "表：{1}的字段映射不存在！";

    public static final String TABLE_FLUSH_ERROR = "表 ：{1}commit异常！";

    public static final String TABLE_CLOSE_ERROR = "表 ：{1}关闭异常！";

    public static final String NO_DATA = "没有数据！";

    public static final String COLUMNS_NO_DATA = "传入字段列表没有数据！";

    public static final String COLUMNS_IS_EMPTY = "传入列为空！";

    public static final String TABLE_NAME_NULL = "表名为空！";

    public static final String CREATE_INDEX_FAIL = "索引：{1}创建失败！";

    public static final String CREATE_INDEX_COLS_FAIL = "索引：{1}创建字段失败！";

    public static final String INDEX_INSERT_FAIL = "索引：{1}录入数据失败！";

    public static final String CFG_TABLE_NOT_EXISTS = "xml配置中table：{1}不存在";

    public static final String CFG_TOPIC_NOT_EXISTS = "xml配置中topic：{1}不存在";

    public static final String CFG_INIT_FAIL = "配置文件初始化失败！";

    public static final String HBSE_CONNECTION_FAIL = "链接Hbase失败！";

    public static final String CONNECTION_TIMEOUT = "获取连接超时！";

    public static final String HBASE_DELETE_DATA_FAIL = "删除Hbase数据失败！";

    public static final String HIVE_DRIVER_NOTFOUND = "缺少Hive驱动类！";

    public static final String HIVE_CONNECT_FAIL = "Hive连接失败！";

    public static final String CONNECTION_POOL_INTERRUPTED = "连接池中断异常！";

    public static final String CONNECTION_POOL_CONFIG = "连接池配置项不合法！";

    public static final String HIVE_EXISTS_TABLE_FAIL = "Hive查询表存在失败！";

    public static final String TABLE_NOT_CONFIG = "表：{1}没有在xml中配置！";

    public static final String TABLE_COLS_NULL = "传入的表无字段信息！";

    public static final String TABLE_NULL = "传入的表对象为空！";

    public static final String SQL_NULL = "sql语句为空！";

    public static final String DDL_EXEC_FAIL = "sql语句执行失败！";

    public static final String TABLE_CANT_ALTER = "不能修改表结构！";
    
    public static final String ES_CLIENT_ERROR = "获取es客户端失败";
    
    public static final String NO_INDEX = "输入的查询条件没有对应的索引！";

    public static final String FILTER_DONT_EXSIST = "过滤器：{1}不存在！";

    public static final String FILTER_CLASS_DONT_EXSIST = "过滤器类：{1}无法加载！";

    public static final String READ_RESULT_ERROR = "读取数据库结果集异常！";

    public static final String TABLE_TYPE_WRONG = "表：{1}类型错误！";

    public static final String INDEX_TYPE_NOT_MATCH = "传入的表和表的索引类型不同!";

    public static final String NPBASE_NOT_INIT = "请先调用build方法初始化NPBase!";
    
    public static final String CONFIG_IS_NULL = "必填参数：{1}为空!";

    public static final String DATA_FORMAT_ERROR = "日期格式异常！";

    public static final String UPDATE_SETTING_ERROR = "更新es索引设置失败！";
    
    public BigDataWareHouseException() {
        super();
    }

    public BigDataWareHouseException(String msg) {
        super(msg);
    }
    
    public BigDataWareHouseException(Throwable e) {
        super(e);
    }
    
    public BigDataWareHouseException(String msg, Throwable e) {
        super(msg, e);
        setErrorCause(e);
    }

    public static BigDataWareHouseException throwException(Throwable e,String msg, String... params) {
        String finalMsg = buildMsg(msg, params);
        return new BigDataWareHouseException(finalMsg,e);
    }
    
    public static BigDataWareHouseException throwException(String msg, String... params) {
        String finalMsg = buildMsg(msg, params);
        return new BigDataWareHouseException(finalMsg);
    }

    public static String buildMsg(String msg, String... params) {
        String finalMsg = msg;
        if (null != params && params.length > 0) {
            finalMsg = "";
            String[] parts = msg.split("[{\\d}]", -1);

            if (msg.trim().startsWith("{1}")) {
                finalMsg = params[0];
            }

            int j = 0;
            for (int i = 0; i < parts.length; i++) {
                if (null != parts[i] && !"".equals(parts[i].trim())) {
                    if (i == parts.length - 1) {
                        finalMsg += parts[i];
                        j++;
                    } else {
                        finalMsg += parts[i] + params[j];
                        j++;
                    }
                }
            }
        }
        return finalMsg;
    }

    public static String buildMsg(Throwable e, String msg, String... params) {
        String finalMsg = buildMsg(msg,params);
        finalMsg +=";; Root cause:"+e.getMessage();
        return finalMsg;
    }
    
    
    public Throwable getErrorCause()
    {
        return errorCause;
    }

    public void setErrorCause(Throwable errorCause)
    {
        this.errorCause = errorCause;
    }

    


  
}